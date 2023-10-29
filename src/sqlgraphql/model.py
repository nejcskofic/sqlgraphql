from typing import Any

from graphene import Field
from graphene.types.objecttype import ObjectType, ObjectTypeOptions
from sqlalchemy import sql
from sqlalchemy.sql import elements

from sqlgraphql.builders import build_field


class QueryableTypeOptions(ObjectTypeOptions):
    base_query: sql.Select = None  # type: ignore[assignment]


class QueryableObjectType(ObjectType):
    _meta: QueryableTypeOptions

    @classmethod
    def __init_subclass_with_meta__(  # type: ignore[override]
        cls,
        interfaces: tuple[Any, ...] = (),
        possible_types: tuple[Any, ...] = (),
        default_resolver: Any | None = None,
        base_query: sql.Select | None = None,
        _meta: QueryableTypeOptions | None = None,
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
        fields: dict[str, Field] = {}
        for name, column in columns.items():
            if name in fields:
                raise ValueError(f"Duplicate field with name '{name}' detected")

            fields[name] = build_field(column)

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
