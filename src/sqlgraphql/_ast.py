import enum
from collections.abc import Mapping
from dataclasses import dataclass

import sqlalchemy
from graphql import (
    FieldNode,
    FragmentDefinitionNode,
    FragmentSpreadNode,
    GraphQLResolveInfo,
    Node,
    Visitor,
    VisitorAction,
    visit,
)
from sqlalchemy.sql.type_api import TypeEngine

from sqlgraphql.model import QueryableNode


@dataclass(frozen=True, slots=True, kw_only=True)
class AnalyzedField:
    orm_name: str
    gql_name: str
    orm_field: sqlalchemy.ColumnElement
    required: bool

    @property
    def orm_type(self) -> TypeEngine:
        return self.orm_field.type


@dataclass(frozen=True, slots=True, kw_only=True)
class AnalyzedNode:
    node: QueryableNode
    fields: Mapping[str, AnalyzedField]


class _TransformQueryVisitor(Visitor):
    def __init__(self, info: GraphQLResolveInfo, node: AnalyzedNode):
        super().__init__()
        self._info = info
        self._query = node.node.query
        self._node = node
        self._selected_fields: list[sqlalchemy.ColumnElement] = []

    @property
    def query(self) -> sqlalchemy.Select:
        return self._query

    def enter_fragment_spread(
        self,
        node: FragmentSpreadNode,
        key: str | int,
        parent: Node,
        path: list[str | int],
        ancestors: list[Node],
    ) -> FragmentDefinitionNode:
        return self._info.fragments[node.name.value]

    def enter_field(
        self,
        node: FieldNode,
        key: str | int | None,
        parent: Node,
        path: list[str | int],
        ancestors: list[Node],
    ) -> VisitorAction:
        if not path:
            return self.IDLE

        # we are not on the root
        gql_field_name = node.name.value
        self._selected_fields.append(self._node.fields[gql_field_name].orm_field)
        return self.SKIP

    def leave_field(
        self,
        node: FieldNode,
        key: str | int | None,
        parent: Node,
        path: list[str | int],
        ancestors: list[Node],
    ) -> VisitorAction:
        if not path:
            # we are on the root
            self._query = self._query.with_only_columns(*self._selected_fields)
        return self.IDLE


def transform_query(info: GraphQLResolveInfo, node: AnalyzedNode) -> sqlalchemy.Select:
    assert len(info.field_nodes) == 1
    visitor = _TransformQueryVisitor(info, node)
    visit(info.field_nodes[0], visitor)
    return visitor.query


class SortDirection(enum.Enum):
    ASC = enum.auto()
    DESC = enum.auto()
