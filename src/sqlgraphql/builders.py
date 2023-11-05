from __future__ import annotations

from collections.abc import Callable

from graphql import (
    GraphQLField,
    GraphQLList,
    GraphQLNonNull,
    GraphQLObjectType,
    GraphQLResolveInfo,
    GraphQLScalarType,
    GraphQLSchema,
)
from graphql.pyutils import snake_to_camel
from sqlalchemy import Column, Row

from sqlgraphql._ast import AnalyzedField, AnalyzedNode
from sqlgraphql._gql import ScalarTypeRegistry
from sqlgraphql._orm import TypeRegistry
from sqlgraphql._resolvers import ListResolver
from sqlgraphql.model import QueryableNode
from sqlgraphql.types import SimpleResolver


def _snake_to_camel_case(value: str) -> str:
    return snake_to_camel(value, upper=False)


class SchemaBuilder:
    def __init__(self, field_name_converter: Callable[[str], str] = _snake_to_camel_case):
        # config
        self._field_name_converter = field_name_converter

        # other fields
        self._analyzed_nodes: dict[QueryableNode, AnalyzedNode] = {}
        self._query_root_members: dict[str, GraphQLField] = {}
        self._orm_type_registry = TypeRegistry()
        self._gql_type_registry = ScalarTypeRegistry()

    def add_root_list(self, name: str, node: QueryableNode) -> SchemaBuilder:
        if name in self._query_root_members:
            raise ValueError(f"Name '{name}' has already been used")

        analyzed_node = self._get_analyzed_node(node)

        def build_field_resolver(name: str) -> SimpleResolver:
            def resolver(parent: Row, info: GraphQLResolveInfo) -> object | None:
                return parent._mapping.get(name)

            return resolver

        object_type = GraphQLObjectType(
            node.name,
            {
                entry.gql_name: GraphQLField(
                    self._convert_to_gql_type(entry),
                    resolve=build_field_resolver(entry.orm_name),
                )
                for entry in analyzed_node.fields.values()
            },
        )

        self._query_root_members[name] = GraphQLField(
            GraphQLList(object_type), resolve=ListResolver(analyzed_node)
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

    def _convert_to_gql_type(self, field: AnalyzedField) -> GraphQLScalarType | GraphQLNonNull:
        python_type = self._orm_type_registry.get_python_type(field.orm_type)
        return self._gql_type_registry.get_scalar_type(python_type, field.required)
