from graphql import GraphQLEnumType, GraphQLInputField, GraphQLInputObjectType

from sqlgraphql._ast import AnalyzedNode, SortDirection
from sqlgraphql._gql import TypeMap
from sqlgraphql._utils import CacheDict


class SortableArgumentBuilder:
    _TYPE_SUFFIX = "SortInputObject"

    def __init__(self, type_map: TypeMap) -> None:
        self._type_map = type_map
        self._sort_direction_gql_enum = type_map.add(
            GraphQLEnumType(
                "SortDirection",
                {key.lower(): value for key, value in SortDirection.__members__.items()},
            )
        )
        self._cache = CacheDict[AnalyzedNode, GraphQLInputObjectType](
            self._construct_sort_argument_type
        )

    def build_from_node(self, node: AnalyzedNode) -> GraphQLInputObjectType:
        return self._cache[node]

    def _construct_sort_argument_type(self, node: AnalyzedNode) -> GraphQLInputObjectType:
        return self._type_map.add(
            GraphQLInputObjectType(
                self._type_map.get_unique_name(node.node.name, self._TYPE_SUFFIX),
                {
                    field.gql_name: GraphQLInputField(self._sort_direction_gql_enum)
                    for field in node.fields.values()
                },
            )
        )
