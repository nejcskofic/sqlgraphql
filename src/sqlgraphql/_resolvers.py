from collections.abc import Iterable
from typing import Any

from graphql import GraphQLResolveInfo
from sqlalchemy import Row

from sqlgraphql._ast import AnalyzedNode, transform_query
from sqlgraphql.types import TypedResolveContext


class DbFieldResolver:
    __slots__ = ("_field_name",)

    def __init__(self, field_name: str):
        self._field_name = field_name

    def __call__(self, parent: Row, info: GraphQLResolveInfo) -> Any:
        return parent._mapping.get(self._field_name)


class ListResolver:
    __slots__ = ("_node",)

    def __init__(self, node: AnalyzedNode):
        self._node = node

    def __call__(self, parent: object | None, info: GraphQLResolveInfo) -> Iterable:
        query = transform_query(info, self._node)
        context: TypedResolveContext = info.context
        return context["db_session"].execute(query)
