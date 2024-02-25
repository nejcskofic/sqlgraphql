from __future__ import annotations

import itertools
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Literal

import sqlalchemy
from graphql import (
    GraphQLEnumType,
    GraphQLObjectType,
    GraphQLScalarType,
)
from sqlalchemy import Column, Select, Table
from sqlalchemy.sql.type_api import TypeEngine

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
    orm_ordinal_position: int
    required: bool
    gql_name: str
    data: FieldData = field(default_factory=FieldData, compare=False)

    @property
    def orm_type(self) -> TypeEngine:
        return self.orm_field.type


@dataclass(frozen=True)
class JoinPoint:
    kind: Literal["0", "1", "n"]
    joins: Sequence[tuple[Column, Column]]

    def is_required(self) -> bool:
        return self.kind == "1"


@dataclass(slots=True, kw_only=True)
class LinkData:
    remote_node: AnalyzedNode | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class AnalyzedLink:
    gql_name: str
    node_accessor: Callable[[], AnalyzedNode]
    join: JoinPoint
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
        for idx, column in enumerate(columns):
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
                orm_ordinal_position=idx,
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
            join_point = _get_implicit_relation(node.query, remote_node.query)
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
            join=join_point,
        )


def _get_implicit_relation(source_query: Select, remote_query: Select) -> JoinPoint:
    candidate: JoinPoint | None = None

    for entry in _iterate_implicit_relations(source_query, remote_query):
        if candidate is not None:
            raise InvalidOperationException(
                "Expected single candidate relationship, found more than one."
            )
        candidate = entry

    if candidate is None:
        raise InvalidOperationException("Expected single candidate relationship, found 0.")

    return candidate


def _iterate_implicit_relations(source_query: Select, remote_query: Select) -> Iterator[JoinPoint]:
    source_iter = (
        source_from
        for source_from in source_query.get_final_froms()
        if isinstance(source_from, Table)
    )
    remote_iter = (
        remote_from
        for remote_from in remote_query.get_final_froms()
        if isinstance(remote_from, Table)
    )
    for source_from, remote_from in itertools.product(source_iter, remote_iter):
        for fk in source_from.foreign_key_constraints:
            if fk.referred_table is not remote_from:
                continue

            is_optional = any(column.nullable for column in fk.columns)

            yield JoinPoint(
                kind="0" if is_optional else "1",
                joins=tuple(zip(fk.columns, remote_from.primary_key.columns)),
            )

        for fk in remote_from.foreign_key_constraints:
            if fk.referred_table is not source_from:
                continue

            yield JoinPoint(
                kind="n", joins=tuple(zip(remote_from.primary_key.columns, fk.columns))
            )
