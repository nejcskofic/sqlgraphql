import datetime
from typing import Any, Dict, Optional, Tuple, Type

from graphene import ID, Date, Field, Int, Scalar, String
from graphene.types.objecttype import ObjectType, ObjectTypeOptions
from sqlalchemy import Column, sql
from sqlalchemy.sql import elements, type_api


class QueryableTypeOptions(ObjectTypeOptions):
    base_query: sql.Select = None  # type: ignore


class QueryableObjectType(ObjectType):
    _meta: QueryableTypeOptions

    @classmethod
    def __init_subclass_with_meta__(  # type: ignore[override]
        cls,
        interfaces: Tuple[Any, ...] = (),
        possible_types: Tuple[Any, ...] = (),
        default_resolver: Optional[Any] = None,
        base_query: Optional[sql.Select] = None,
        _meta: Optional[QueryableTypeOptions] = None,
        **options: Any,
    ) -> None:
        if _meta is None:
            _meta = QueryableTypeOptions(cls)

        if not isinstance(base_query, sql.Select):
            raise ValueError("Expected 'base_query' of type 'sqlalchemy.sql.Select'")

        # We transform query into one where we are not mapping to object but we get instead
        # flat result. TODO: Test
        # There is actually no need to do that since resolver will select only fields which are
        # needed There is a bit of weirdness here: this will not keep textual columns (we probably
        # don't care about this?)
        base_query = base_query.with_only_columns(base_query.selected_columns.values())
        columns: sql.ColumnCollection = base_query.selected_columns
        column: elements.ColumnClause
        fields: Dict[str, Field] = {}
        for name, column in columns.items():
            if name in fields:
                raise ValueError(f"Duplicate field with name '{name}' detected")

            graphene_type, is_optional = _get_graphene_type(column)
            fields[name] = Field(graphene_type, required=not is_optional)

        _meta.base_query = base_query
        if _meta.fields:
            _meta.fields.update(fields)
        else:
            _meta.fields = fields

        super().__init_subclass_with_meta__(
            interfaces=interfaces,
            possible_types=possible_types,
            default_resolver=default_resolver,
            _meta=_meta,
            **options,
        )


def _get_graphene_type(
    column: elements.ColumnClause,
) -> Tuple[Type[Scalar], bool]:
    column_type: type_api.TypeEngine = column.type
    python_type = column_type.python_type
    if isinstance(column, Column):
        if column.primary_key:
            return ID, False

        # TODO May not be True if table is left joined
        is_optional = column.nullable
    else:
        # We cannot track computed column's nullability
        is_optional = True

    graphene_type: Type[Scalar]
    if python_type == str:
        graphene_type = String
    elif python_type == int:
        graphene_type = Int
    elif python_type == datetime.date:
        graphene_type = Date
    else:
        # TODO: proper implementation which takes into account:
        #       - database type
        #       - python type (potentially)
        #       - nullability?
        raise AssertionError(f"Invalid type: {python_type}")

    return graphene_type, is_optional
