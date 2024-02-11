from __future__ import annotations

from collections.abc import Callable

from graphql import (
    GraphQLArgument,
    GraphQLField,
    GraphQLList,
    GraphQLObjectType,
    GraphQLSchema,
)
from graphql.pyutils import snake_to_camel

from sqlgraphql._ast import Analyzer
from sqlgraphql._builders.enum import EnumBuilder
from sqlgraphql._builders.filtering.builder import FilteringArgumentBuilder
from sqlgraphql._builders.pagination import OffsetPagedArgumentBuilder
from sqlgraphql._builders.selecting import ObjectBuilder
from sqlgraphql._builders.sorting import SortableArgumentBuilder
from sqlgraphql._gql import ScalarTypeRegistry, TypeMap
from sqlgraphql._orm import TypeRegistry
from sqlgraphql._resolvers import ListResolver
from sqlgraphql._transformers import QueryTransformer
from sqlgraphql.model import QueryableNode


def _snake_to_camel_case(value: str) -> str:
    return snake_to_camel(value, upper=False)


class SchemaBuilder:
    def __init__(self, field_name_converter: Callable[[str], str] = _snake_to_camel_case):
        self._analyzer = Analyzer(field_name_converter)
        self._query_root_members: dict[str, GraphQLField] = {}
        self._type_map = TypeMap()
        self._orm_type_registry = TypeRegistry()
        self._gql_type_registry = ScalarTypeRegistry(self._type_map)
        self._enum_builder = EnumBuilder(self._type_map)
        self._object_builder = ObjectBuilder(
            self._type_map, self._enum_builder, self._orm_type_registry, self._gql_type_registry
        )
        self._sortable_builder = SortableArgumentBuilder(self._type_map)
        self._filter_builder = FilteringArgumentBuilder(self._type_map)
        self._offset_paged_builder = OffsetPagedArgumentBuilder(self._type_map)

    def add_root_list(
        self,
        name: str,
        node: QueryableNode,
        *,
        sortable: bool = False,
        filterable: bool = False,
        pageable: bool = False,
    ) -> SchemaBuilder:
        if name in self._query_root_members:
            raise ValueError(f"Name '{name}' has already been used")

        analyzed_node = self._analyzer.get(node)

        object_type = self._object_builder.build_object(analyzed_node)

        args: dict[str, GraphQLArgument] = {}
        transformers = []
        if sortable:
            sortable_config = self._sortable_builder.build_from_node(analyzed_node)
            args.update(sortable_config.args)
            transformers.append(sortable_config.transformer)

        if filterable:
            filterable_config = self._filter_builder.build_filter(analyzed_node)
            args.update(filterable_config.args)
            transformers.append(filterable_config.transformer)

        transformer = QueryTransformer.create(analyzed_node, transformers)

        if pageable:
            field = self._offset_paged_builder.build_paged_list_field(
                analyzed_node, args, transformer
            )
        else:
            field = GraphQLField(
                GraphQLList(object_type),
                args=args,
                resolve=ListResolver(transformer),
            )

        self._query_root_members[name] = field
        return self

    def build(self) -> GraphQLSchema:
        query_type = GraphQLObjectType(
            "Query",
            self._query_root_members,
        )
        return GraphQLSchema(query_type)
