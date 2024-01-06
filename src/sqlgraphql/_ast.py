from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from typing import NamedTuple

import sqlalchemy
from graphql import (
    FieldNode,
    FragmentSpreadNode,
    GraphQLEnumType,
    GraphQLObjectType,
    GraphQLResolveInfo,
    GraphQLScalarType,
    InlineFragmentNode,
    SelectionNode,
)
from sqlalchemy import ColumnElement
from sqlalchemy.sql.type_api import TypeEngine

from sqlgraphql.exceptions import InvalidStateException
from sqlgraphql.model import QueryableNode


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
class NodeData:
    gql_type: GraphQLObjectType | None = None


@dataclass(frozen=True, slots=True, kw_only=True, eq=False)
class AnalyzedNode:
    node: QueryableNode
    fields: Mapping[str, AnalyzedField]
    data: NodeData = field(default_factory=NodeData, compare=False)

    def __hash__(self) -> int:
        return hash(id(self))

    def __eq__(self, other: object) -> bool:
        return self is other


class _FieldInfo(NamedTuple):
    name: str
    node: FieldNode


class _FieldWalker:
    __slots__ = ("_info", "_node_stack", "_current_relative_path")

    def __init__(self, node: FieldNode, info: GraphQLResolveInfo):
        self._info = info
        self._node_stack = [node]
        self._current_relative_path: list[str] = []

    def descend(self, member: str, raise_: bool = False) -> bool:
        for child in self.children():
            if child.name == member:
                break
        else:
            if raise_:
                raise ValueError(f"No child with name '{member}'")
            else:
                return False

        self._node_stack.append(child.node)
        self._current_relative_path.append(child.name)
        return True

    def ascend(self, raise_: bool = False) -> bool:
        if not self._current_relative_path:
            if raise_:
                raise InvalidStateException("We are already at the root")
            else:
                return False
        self._node_stack.pop()
        self._node_stack.pop()
        assert self._node_stack
        return True

    def children(self) -> Iterator[_FieldInfo]:
        node = self._node_stack[-1]
        if node.selection_set is None:
            return

        for selection in node.selection_set.selections:
            yield from self._materialize_children(selection)

    def _materialize_children(self, node: SelectionNode) -> Iterator[_FieldInfo]:
        if isinstance(node, FieldNode):
            yield _FieldInfo(node.name.value, node)
        elif isinstance(node, FragmentSpreadNode):
            fragment = self._info.fragments[node.name.value]
            for selection in fragment.selection_set.selections:
                yield from self._materialize_children(selection)
        elif isinstance(node, InlineFragmentNode):
            for selection in node.selection_set.selections:
                yield from self._materialize_children(selection)
        else:
            raise ValueError(f"Unknown SelectionNode type: {type(node)!r}")


def transform_query(
    info: GraphQLResolveInfo, node: AnalyzedNode, sub_path: Sequence[str] = ()
) -> sqlalchemy.Select:
    # TODO: move other transformations (filter, order by) here as well
    assert len(info.field_nodes) == 1
    walker = _FieldWalker(info.field_nodes[0], info)
    requested_fields: list[ColumnElement] | None = None
    for segment in sub_path:
        if not walker.descend(segment):
            requested_fields = []
            break

    if requested_fields is None:
        # rewrite query to select only desired fields
        requested_fields = [
            node.fields[gql_field_name].orm_field for gql_field_name, _ in walker.children()
        ]

    orig_query = node.node.query
    if not requested_fields:
        # Keep original query if there is no select
        # Use case is pagination data only without actual select
        return orig_query
    else:
        return orig_query.with_only_columns(*requested_fields)
