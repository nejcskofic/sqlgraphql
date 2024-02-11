from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field

import sqlalchemy
from graphql import (
    GraphQLEnumType,
    GraphQLObjectType,
    GraphQLScalarType,
)
from sqlalchemy import Column, ForeignKeyConstraint
from sqlalchemy.sql.type_api import TypeEngine

from sqlgraphql._orm import get_implicit_relation
from sqlgraphql._transformers import FieldRules
from sqlgraphql._utils import CacheDictCM
from sqlgraphql.exceptions import GQLBuilderException, InvalidOperationException
from sqlgraphql.model import Link, QueryableNode


@dataclass(slots=True, kw_only=True)
class FieldData:
    python_type: type | None = None
    gql_type: GraphQLScalarType | GraphQLEnumType | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class AnalyzedField:
    orm_name: str
    orm_field: sqlalchemy.ColumnElement
    required: bool
    gql_name: str
    data: FieldData = field(default_factory=FieldData, compare=False)

    @property
    def orm_type(self) -> TypeEngine:
        return self.orm_field.type


@dataclass(slots=True, kw_only=True)
class LinkData:
    remote_node: AnalyzedNode | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class AnalyzedLink:
    gql_name: str
    node_accessor: Callable[[], AnalyzedNode]
    relationship: ForeignKeyConstraint
    data: LinkData = field(default_factory=LinkData, compare=False)

    @property
    def node(self) -> AnalyzedNode:
        if self.data.remote_node is None:
            self.data.remote_node = self.node_accessor()
        return self.data.remote_node


@dataclass(slots=True, kw_only=True)
class NodeData:
    gql_type: GraphQLObjectType | None = None
    field_rules: Mapping[str, FieldRules] | None = None


@dataclass(frozen=True, slots=True, kw_only=True, eq=False)
class AnalyzedNode:
    node: QueryableNode
    fields: Mapping[str, AnalyzedField]
    links: Mapping[str, AnalyzedLink]
    data: NodeData = field(default_factory=NodeData, compare=False)

    def __hash__(self) -> int:
        return hash(id(self))

    def __eq__(self, other: object) -> bool:
        return self is other


class Analyzer:
    def __init__(self, field_name_converter: Callable[[str], str]):
        self._field_name_converter = field_name_converter

        self._analyzed_nodes = CacheDictCM[QueryableNode, AnalyzedNode](self._create_analyzed_node)

    def get(self, node: QueryableNode) -> AnalyzedNode:
        return self._analyzed_nodes[node]

    @contextmanager
    def _create_analyzed_node(self, node: QueryableNode) -> Iterator[AnalyzedNode]:
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

        analyzed_links = {}
        to_process = []
        for name, value in node.extra.items():
            if isinstance(value, QueryableNode):
                analyzed_links[name] = self._create_link(node, name, value)
                to_process.append(value)
            elif isinstance(value, Link):
                analyzed_links[name] = self._create_link(node, name, value.node)
                to_process.append(value.node)
            else:
                raise InvalidOperationException("Unsupported")

        yield AnalyzedNode(node=node, fields=analyzed_fields, links=analyzed_links)

        for entry in to_process:
            self.get(entry)

    def _create_link(
        self, node: QueryableNode, name: str, remote_node: QueryableNode
    ) -> AnalyzedLink:
        try:
            relation = get_implicit_relation(node.query, remote_node.query)
        except InvalidOperationException as e:
            raise GQLBuilderException(
                f"Failed to determine implicit relation for member '{name}'"
                f" in node '{node.name}':\n"
                f"  {e}\n"
                f"Relationship should be specified explicitly."
            )

        return AnalyzedLink(
            gql_name=name,
            node_accessor=lambda: self._analyzed_nodes[remote_node],
            relationship=relation,
        )
