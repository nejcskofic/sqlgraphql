from __future__ import annotations

from collections.abc import Callable

from graphql import (
    GraphQLArgument,
    GraphQLEnumType,
    GraphQLField,
    GraphQLList,
    GraphQLNonNull,
    GraphQLObjectType,
    GraphQLScalarType,
    GraphQLSchema,
)
from graphql.pyutils import snake_to_camel
from sqlalchemy import Column

from sqlgraphql._ast import AnalyzedField, AnalyzedNode
from sqlgraphql._builders.enum import EnumBuilder
from sqlgraphql._builders.sorting import SortableArgumentBuilder
from sqlgraphql._gql import ScalarTypeRegistry, TypeMap
from sqlgraphql._orm import TypeRegistry
from sqlgraphql._resolvers import DbFieldResolver, ListResolver
from sqlgraphql.model import QueryableNode


def _snake_to_camel_case(value: str) -> str:
    return snake_to_camel(value, upper=False)


class SchemaBuilder:
    def __init__(self, field_name_converter: Callable[[str], str] = _snake_to_camel_case):
        # config
        self._field_name_converter = field_name_converter

        # other fields
        self._analyzed_nodes: dict[QueryableNode, AnalyzedNode] = {}
        self._query_root_members: dict[str, GraphQLField] = {}
        self._type_map = TypeMap()
        self._orm_type_registry = TypeRegistry()
        self._gql_type_registry = ScalarTypeRegistry(self._type_map)
        self._enum_builder = EnumBuilder(self._type_map)
        self._sortable_builder = SortableArgumentBuilder(self._type_map)

    def add_root_list(
        self, name: str, node: QueryableNode, *, sortable: bool = False
    ) -> SchemaBuilder:
        if name in self._query_root_members:
            raise ValueError(f"Name '{name}' has already been used")

        analyzed_node = self._get_analyzed_node(node)

        object_type = GraphQLObjectType(
            node.name,
            {
                entry.gql_name: GraphQLField(
                    self._convert_to_gql_type(entry),
                    resolve=DbFieldResolver(entry.orm_name),
                )
                for entry in analyzed_node.fields.values()
            },
        )

        args: dict[str, GraphQLArgument] = {}
        transformers = []
        if sortable:
            sortable_config = self._sortable_builder.build_from_node(analyzed_node)
            args[sortable_config.arg_name] = GraphQLArgument(sortable_config.arg_gql_type)
            transformers.append(sortable_config.transformer)

        self._query_root_members[name] = GraphQLField(
            GraphQLList(object_type), args=args, resolve=ListResolver(analyzed_node, transformers)
        )
        return self

    def build(self) -> GraphQLSchema:
        query_type = GraphQLObjectType(
            "Query",
            self._query_root_members,
        )
        return GraphQLSchema(query_type)

    def _get_analyzed_node(self, node: QueryableNode) -> AnalyzedNode:
        analyzed_node = self._analyzed_nodes.get(node)
        if analyzed_node is not None:
            return analyzed_node

        columns = node.query.selected_columns
        field_name_converter = self._field_name_converter
        analyzed_fields = {}
        for column in columns:
            if isinstance(column, Column) and column.nullable is not None:
                required = not column.nullable
            else:
                # we don't have information, assume weaker constraint
                required = False

            gql_field_name = field_name_converter(column.name)
            analyzed_fields[gql_field_name] = AnalyzedField(
                orm_name=column.name,
                gql_name=gql_field_name,
                orm_field=column,
                required=required,
            )

        analyzed_node = AnalyzedNode(node=node, fields=analyzed_fields)
        self._analyzed_nodes[node] = analyzed_node
        return analyzed_node

    def _convert_to_gql_type(
        self, field: AnalyzedField
    ) -> GraphQLScalarType | GraphQLNonNull | GraphQLEnumType:
        enum_type = self._enum_builder.build_from_field(field)
        if enum_type is not None:
            return enum_type
        python_type = self._orm_type_registry.get_python_type(field.orm_type)
        return self._gql_type_registry.get_scalar_type(python_type, field.required)
