from collections.abc import Iterable, Sequence
from typing import Any

from graphql import GraphQLResolveInfo
from sqlalchemy import Row

from sqlgraphql._ast import AnalyzedNode, transform_query
from sqlgraphql._builders.util import QueryTransformer
from sqlgraphql.types import TypedResolveContext


class DbFieldResolver:
    __slots__ = ("_field_name",)

    def __init__(self, field_name: str):
        self._field_name = field_name

    def __call__(self, parent: Row, info: GraphQLResolveInfo) -> Any:
        return parent._mapping.get(self._field_name)


class ListResolver:
    __slots__ = ("_node", "_transformers")

    def __init__(self, node: AnalyzedNode, transformers: Sequence[QueryTransformer]):
        self._node = node
        self._transformers = transformers

    def __call__(self, parent: object | None, info: GraphQLResolveInfo, **kwargs: Any) -> Iterable:
        query = transform_query(info, self._node)
        for transformer in self._transformers:
            query = transformer(query, self._node, info, **kwargs)

        context: TypedResolveContext = info.context
        return context["db_session"].execute(query)


class FieldResolver:
    __slots__ = ("_field_name",)

    def __init__(self, field_name: str):
        self._field_name = field_name

    def __call__(self, parent: Any, info: GraphQLResolveInfo) -> Any:
        return getattr(parent, self._field_name)
