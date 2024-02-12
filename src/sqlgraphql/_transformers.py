from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, NamedTuple, TypeAlias

from graphql import (
    FieldNode,
    FragmentSpreadNode,
    GraphQLResolveInfo,
    InlineFragmentNode,
    SelectionNode,
)
from sqlalchemy import Select
from sqlalchemy.sql.elements import ColumnElement

from sqlgraphql._utils import assert_not_none
from sqlgraphql.exceptions import InvalidOperationException

if TYPE_CHECKING:
    from sqlgraphql._ast import AnalyzedNode


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
                raise InvalidOperationException("We are already at the root")
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


@dataclass(frozen=True, slots=True)
class ColumnSelectRule:
    selectable: ColumnElement


@dataclass(frozen=True, slots=True)
class InlineObjectRule:
    base_query: Select
    fields_accessor: Mapping[str, FieldRules] | Callable[[], Mapping[str, FieldRules]]

    @classmethod
    def create(cls, node: AnalyzedNode) -> InlineObjectRule:
        return cls(
            node.node.query,
            node.data.field_rules
            if node.data.field_rules is not None
            else lambda: assert_not_none(node.data.field_rules),
        )

    @property
    def fields(self) -> Mapping[str, FieldRules]:
        if isinstance(self.fields_accessor, Mapping):
            return self.fields_accessor
        else:
            fields = self.fields_accessor()
            object.__setattr__(self, "fields_accessor", fields)
            return fields


FieldRules: TypeAlias = ColumnSelectRule | InlineObjectRule


class ArgumentRule(ABC):
    @abstractmethod
    def apply(self, query: Select, info: GraphQLResolveInfo, args: dict[str, Any]) -> Select:
        raise NotImplementedError()


class QueryTransformer:
    __slots__ = ("_root_rule", "_arg_rules")

    def __init__(self, root_rule: InlineObjectRule, arg_rules: Sequence[ArgumentRule] = ()):
        self._root_rule = root_rule
        self._arg_rules = arg_rules

    @classmethod
    def create(
        cls, node: AnalyzedNode, arg_transformers: Sequence[ArgumentRule] = ()
    ) -> QueryTransformer:
        return cls(InlineObjectRule.create(node), arg_transformers)

    def transform(
        self, info: GraphQLResolveInfo, args: dict[str, Any], sub_path: Sequence[str] = ()
    ) -> Select:
        query = self._root_rule.base_query
        select_rules = self._root_rule.fields

        assert len(info.field_nodes) == 1
        walker = _FieldWalker(info.field_nodes[0], info)
        requested_fields: list[ColumnElement] = []
        for segment in sub_path:
            if not walker.descend(segment):
                # We may have paged request without actually going into field selection
                break
        else:
            for field in walker.children():
                transformer = select_rules.get(field.name)
                if transformer is None:
                    raise InvalidOperationException(
                        f"Field '{field.name}' does not have transformer"
                    )
                match transformer:
                    case None:
                        raise InvalidOperationException(
                            f"Field '{field.name}' does not have transformer"
                        )
                    case ColumnSelectRule():
                        requested_fields.append(transformer.selectable)
                    case _:
                        raise NotImplementedError(
                            f"Application of transformer '{type(transformer)!r}' is not supported"
                        )

        if requested_fields:
            # Select only requested fields. Otherwise keep selection as is, since we require at
            # least single field (and we may want to do filter on top of it)
            query = query.with_only_columns(*requested_fields)

        for rule in self._arg_rules:
            query = rule.apply(query, info, args)

        return query
