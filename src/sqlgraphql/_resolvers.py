from collections.abc import Iterable
from typing import Any

from graphql import GraphQLResolveInfo
from sqlalchemy import Row

from sqlgraphql._transformers import QueryBuilder, Record
from sqlgraphql.types import TypedResolveContext


class DbFieldResolver:
    __slots__ = ("_field_name",)

    def __init__(self, field_name: str):
        self._field_name = field_name

    def __call__(self, parent: Row | Record, info: GraphQLResolveInfo) -> Any:
        if type(parent) is Record:
            return parent[self._field_name]
        else:
            return getattr(parent, self._field_name)


class ListResolver:
    __slots__ = ("_transformer",)

    def __init__(self, transformer: QueryBuilder):
        self._transformer = transformer

    def __call__(self, parent: object | None, info: GraphQLResolveInfo, **kwargs: Any) -> Iterable:
        context: TypedResolveContext = info.context
        return self._transformer.build(parent, info, kwargs, context["db_session"]).execute()


class FieldResolver:
    __slots__ = ("_field_name",)

    def __init__(self, field_name: str):
        self._field_name = field_name

    def __call__(self, parent: Any, info: GraphQLResolveInfo) -> Any:
        return getattr(parent, self._field_name)
