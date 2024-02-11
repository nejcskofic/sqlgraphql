from collections.abc import Iterable
from typing import Any

from graphql import GraphQLResolveInfo
from sqlalchemy import Row

from sqlgraphql._transformers import QueryTransformer
from sqlgraphql.types import TypedResolveContext


class DbFieldResolver:
    __slots__ = ("_field_name",)

    def __init__(self, field_name: str):
        self._field_name = field_name

    def __call__(self, parent: Row, info: GraphQLResolveInfo) -> Any:
        return parent._mapping.get(self._field_name)


class ListResolver:
    __slots__ = ("_transformer",)

    def __init__(self, transformer: QueryTransformer):
        self._transformer = transformer

    def __call__(self, parent: object | None, info: GraphQLResolveInfo, **kwargs: Any) -> Iterable:
        query = self._transformer.transform(info, kwargs)

        context: TypedResolveContext = info.context
        return context["db_session"].execute(query)


class FieldResolver:
    __slots__ = ("_field_name",)

    def __init__(self, field_name: str):
        self._field_name = field_name

    def __call__(self, parent: Any, info: GraphQLResolveInfo) -> Any:
        return getattr(parent, self._field_name)
