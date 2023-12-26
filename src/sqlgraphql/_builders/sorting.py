import enum
from typing import Any

from graphql import (
    GraphQLEnumType,
    GraphQLInputField,
    GraphQLInputObjectType,
    GraphQLList,
    GraphQLResolveInfo,
)
from sqlalchemy import Select

from sqlgraphql._ast import AnalyzedNode
from sqlgraphql._builders.util import GQLFieldModifiers
from sqlgraphql._gql import TypeMap
from sqlgraphql._utils import CacheDict, get_single_key_value


class SortDirection(enum.Enum):
    ASC = enum.auto()
    DESC = enum.auto()


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

    def build_from_node(self, node: AnalyzedNode) -> GQLFieldModifiers:
        input_object = self._cache[node]
        return GQLFieldModifiers("sort", GraphQLList(input_object), _transform_sortable_query)

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


def _transform_sortable_query(
    query: Select,
    node: AnalyzedNode,
    info: GraphQLResolveInfo,
    *,
    sort: list[dict[str, SortDirection]] | None = None,
    **kwargs: Any,
) -> Select:
    if sort is None:
        return query

    for part in sort:
        field_name, direction = get_single_key_value(part)
        field = node.fields[field_name]
        query = query.order_by(
            field.orm_field.asc() if direction == SortDirection.ASC else field.orm_field.desc()
        )
    return query
