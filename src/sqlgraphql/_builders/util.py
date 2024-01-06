from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol

from graphql import GraphQLArgument, GraphQLResolveInfo
from sqlalchemy import Select

from sqlgraphql._ast import AnalyzedNode


class QueryTransformer(Protocol):
    def __call__(
        self, query: Select, node: AnalyzedNode, info: GraphQLResolveInfo, **kwargs: Any
    ) -> Select:
        ...


@dataclass(frozen=True, slots=True)
class GQLFieldModifiers:
    args: Mapping[str, GraphQLArgument]
    transformer: QueryTransformer
