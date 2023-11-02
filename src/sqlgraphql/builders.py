from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass

from graphql import (
    GraphQLField,
    GraphQLInt,
    GraphQLList,
    GraphQLNonNull,
    GraphQLObjectType,
    GraphQLResolveInfo,
    GraphQLScalarType,
    GraphQLSchema,
    GraphQLString,
)
from graphql.pyutils import snake_to_camel
from sqlalchemy import Column, ColumnElement, Date, Integer, Row, String
from sqlalchemy.sql.type_api import TypeEngine

from sqlgraphql.model import QueryableNode
from sqlgraphql.types import SimpleResolver, TypedResolveContext


def _snake_to_camel_case(value: str) -> str:
    return snake_to_camel(value, upper=False)


@dataclass(frozen=True, slots=True)
class _AnalyzedField:
    field: ColumnElement
    required: bool

    @property
    def orm_type(self) -> TypeEngine:
        return self.field.type


@dataclass(frozen=True, slots=True)
class _AnalyzedNode:
    node: QueryableNode
    fields: Mapping[str, _AnalyzedField]


class SchemaBuilder:
    def __init__(self, field_name_converter: Callable[[str], str] = _snake_to_camel_case):
        # config
        self._field_name_converter = field_name_converter

        # other fields
        self._analyzed_nodes: dict[QueryableNode, _AnalyzedNode] = {}
        self._query_root_members: dict[str, GraphQLField] = {}

    def add_root_list(self, name: str, node: QueryableNode) -> SchemaBuilder:
        if name in self._query_root_members:
            raise ValueError(f"Name '{name}' has already been used")

        analyzed_node = self._get_analyzed_node(node)

        def build_field_resolver(name: str) -> SimpleResolver:
            def resolver(parent: Row, info: GraphQLResolveInfo) -> object | None:
                return parent._mapping.get(name)

            return resolver

        field_name_converter = self._field_name_converter
        object_type = GraphQLObjectType(
            node.name,
            {
                field_name_converter(name): GraphQLField(
                    self._convert_to_gql_type(entry.orm_type, entry.required),
                    resolve=build_field_resolver(name),
                )
                for name, entry in analyzed_node.fields.items()
            },
        )

        self._query_root_members[name] = GraphQLField(
            GraphQLList(object_type), resolve=_ListResolver(node)
        )
        return self

    def build(self) -> GraphQLSchema:
        query_type = GraphQLObjectType(
            "Query",
            self._query_root_members,
        )
        return GraphQLSchema(query_type)

    def _get_analyzed_node(self, node: QueryableNode) -> _AnalyzedNode:
        analyzed_node = self._analyzed_nodes.get(node)
        if analyzed_node is not None:
            return analyzed_node

        columns = node.query.selected_columns
        analyzed_fields = {}
        for column in columns:
            if isinstance(column, Column) and column.nullable is not None:
                required = not column.nullable
            else:
                # we don't have information, assume weaker constraint
                required = False

            analyzed_fields[column.name] = _AnalyzedField(column, required)

        analyzed_node = _AnalyzedNode(node, analyzed_fields)
        self._analyzed_nodes[node] = analyzed_node
        return analyzed_node

    @classmethod
    def _convert_to_gql_type(
        cls, sql_type: TypeEngine, required: bool
    ) -> GraphQLScalarType | GraphQLNonNull:
        # TODO: introduce abstraction
        sql_type_cls = type(sql_type)
        if sql_type_cls is Integer:
            gql_type = GraphQLInt
        elif sql_type_cls in [String, Date]:
            gql_type = GraphQLString
        else:
            raise NotImplementedError("Not yet implemented")

        if required:
            return GraphQLNonNull(gql_type)
        else:
            return gql_type


class _ListResolver:
    def __init__(self, node: QueryableNode):
        self._node = node

    def __call__(self, parent: object | None, info: GraphQLResolveInfo) -> Iterable:
        context: TypedResolveContext = info.context
        return context["db_session"].execute(self._node.query)
