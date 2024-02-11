from collections.abc import Mapping
from dataclasses import dataclass

from graphql import GraphQLArgument

from sqlgraphql._transformers import ArgumentRule


@dataclass(frozen=True, slots=True)
class GQLFieldModifiers:
    args: Mapping[str, GraphQLArgument]
    transformer: ArgumentRule
