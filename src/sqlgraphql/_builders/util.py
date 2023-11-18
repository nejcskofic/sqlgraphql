from dataclasses import dataclass
from typing import Any, Protocol

from graphql import GraphQLInputObjectType, GraphQLList, GraphQLResolveInfo
from sqlalchemy import Select

from sqlgraphql._ast import AnalyzedNode


class QueryTransformer(Protocol):
    def __call__(
        self, query: Select, node: AnalyzedNode, info: GraphQLResolveInfo, **kwargs: Any
    ) -> Select:
        ...


@dataclass(frozen=True, slots=True)
class GQLFieldModifiers:
    arg_name: str
    arg_gql_type: GraphQLInputObjectType | GraphQLList
    transformer: QueryTransformer
