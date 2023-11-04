from collections.abc import Iterable

from graphql import GraphQLResolveInfo

from sqlgraphql._ast import AnalyzedNode, transform_query
from sqlgraphql.types import TypedResolveContext


class ListResolver:
    def __init__(self, node: AnalyzedNode):
        self._node = node

    def __call__(self, parent: object | None, info: GraphQLResolveInfo) -> Iterable:
        query = transform_query(info, self._node)
        context: TypedResolveContext = info.context
        return context["db_session"].execute(query)
