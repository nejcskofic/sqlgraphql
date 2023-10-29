import enum
from functools import singledispatch
from typing import cast

import graphene
from graphene import JSONString
from graphene.types.unmountedtype import UnmountedType
from sqlalchemy import Column, Enum
from sqlalchemy.sql.elements import ColumnClause
from sqlalchemy.sql.type_api import TypeEngine

from sqlgraphql import registry
from sqlgraphql.converters import TypeRegistry

try:
    import sqlalchemy_utils

    # Need to use different field, since mypy cannot infer
    # ModuleType | None for sqlalchemy_utils and does not
    # allow upfront declaration.
    _HAS_SQLALCHEMY_UTILS = True
except ImportError:
    _HAS_SQLALCHEMY_UTILS = False


def build_field(column: ColumnClause) -> graphene.Field:
    if isinstance(column, Column):
        if column.primary_key:
            # TODO how to handle composite primary keys?
            return graphene.Field(graphene.ID, required=True)

        # TODO May not be True if table is left joined
        is_required = not column.nullable
    else:
        # We cannot track computed column's nullability
        is_required = False

    graphene_type = convert_from_db_type(column.type, column)
    return graphene.Field(graphene_type, required=is_required)


@singledispatch
def convert_from_db_type(db_type: TypeEngine, column: ColumnClause) -> type[UnmountedType]:
    python_type = None
    try:
        python_type = db_type.python_type
    except NotImplementedError:
        pass

    if python_type is None:
        raise TypeError(f"Cannot convert type '{db_type!r}' into graphene type.")

    graphene_type = TypeRegistry.get(python_type)
    if graphene_type is None:
        raise TypeError(f"Cannot convert type '{python_type!r}' into graphene type.")

    return graphene_type


@convert_from_db_type.register
def _(db_type: Enum, column: ColumnClause) -> type[UnmountedType]:
    enum_class: type[Enum] | None = db_type.enum_class  # type: ignore[attr-defined]
    if enum_class is not None:
        return registry.current().get_or_build_enum(
            enum_class.__name__, lambda name: graphene.Enum.from_enum(enum_class)
        )
    else:
        # TODO: allow names override?
        enums: list[str] = db_type.enums  # type: ignore[attr-defined]
        return registry.current().get_or_build_enum(
            column.name,
            lambda name: cast(
                type[graphene.Enum],
                graphene.Enum(name, [(entry, entry) for entry in enums]),
            ),
        )


if _HAS_SQLALCHEMY_UTILS:

    @convert_from_db_type.register
    def _(db_type: sqlalchemy_utils.ChoiceType, column: ColumnClause) -> type[UnmountedType]:
        choices = db_type.choices
        if isinstance(choices, type) and issubclass(choices, enum.Enum):
            return registry.current().get_or_build_enum(
                choices.__name__, lambda name: graphene.Enum.from_enum(choices)
            )
        else:
            # TODO: allow names override?
            return registry.current().get_or_build_enum(
                column.name, lambda name: cast(type[graphene.Enum], graphene.Enum(name, choices))
            )

    @convert_from_db_type.register
    def _(db_type: sqlalchemy_utils.JSONType, column: ColumnClause) -> type[UnmountedType]:
        # Alternative would be to use GenericScalar which is effectively
        # any type
        return JSONString
