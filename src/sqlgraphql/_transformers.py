from __future__ import annotations

from abc import ABC, abstractmethod
from collections import deque
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, NamedTuple, TypeAlias

from graphql import (
    FieldNode,
    FragmentSpreadNode,
    GraphQLResolveInfo,
    InlineFragmentNode,
    SelectionNode,
)
from sqlalchemy import Column, FromClause, Row, Select, Subquery, func
from sqlalchemy.orm import Session
from sqlalchemy.sql.elements import ColumnElement, literal

from sqlgraphql._utils import assert_not_none
from sqlgraphql.exceptions import InvalidOperationException

if TYPE_CHECKING:
    from sqlgraphql._ast import AnalyzedNode, JoinPoint


class _FieldInfo(NamedTuple):
    name: str
    node: FieldNode


class _FieldWalker:
    __slots__ = ("_info", "_node_stack", "_current_relative_path")

    def __init__(self, node: FieldNode, info: GraphQLResolveInfo):
        self._info = info
        self._node_stack = [node]
        self._current_relative_path: list[str] = []

    @property
    def current_relative_path(self) -> Sequence[str]:
        return self._current_relative_path

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
    ordinal_position: int


@dataclass(frozen=True, slots=True)
class InlineObjectRule:
    base_query: Select
    join: JoinPoint | None
    fields_accessor: Mapping[str, FieldRules] | Callable[[], Mapping[str, FieldRules]]

    @classmethod
    def create(cls, node: AnalyzedNode, join: JoinPoint | None) -> InlineObjectRule:
        return cls(
            node.node.query,
            join,
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

    def reduce_select(self) -> FromClause | None:
        if self.base_query.whereclause is None:
            resolved_froms = self.base_query.get_final_froms()
            if len(resolved_froms) == 1:
                return resolved_froms[0]

        return None


@dataclass(frozen=True, slots=True)
class LinkDataRule:
    selectables: Sequence[Column]


FieldRules: TypeAlias = ColumnSelectRule | InlineObjectRule | LinkDataRule


class ArgumentRule(ABC):
    @abstractmethod
    def apply(
        self, query: Select, root: Any, info: GraphQLResolveInfo, args: dict[str, Any]
    ) -> Select:
        raise NotImplementedError()


class ApplyLinkRule(ArgumentRule):
    __slots__ = ("_join",)

    def __init__(self, join: JoinPoint):
        self._join = join

    def apply(
        self, query: Select, root: Any, info: GraphQLResolveInfo, args: dict[str, Any]
    ) -> Select:
        accessor = self._get_accessor(root)
        left, right = self._join.joins[0]
        condition = right == accessor(f"__{left.name}")
        for left, right in self._join.joins[1:]:
            condition = condition and right == accessor(f"__{left.name}")

        return query.where(condition)

    @classmethod
    def _get_accessor(cls, root: Any) -> Callable[[str], Any]:
        if isinstance(root, Record):
            return lambda key: root[key]
        else:
            return lambda key: getattr(root, key)


class Record(dict[str, Any]):
    @classmethod
    def from_row(cls, row: Row) -> Record:
        record = Record()
        for name, idx in row._key_to_index.items():
            if isinstance(name, str):
                record[name] = row[idx]
        return record


@dataclass(frozen=True, slots=True)
class _Mapper:
    prefix: str
    path: Sequence[str]


class QueryExecutor:
    __slots__ = ("_query", "_session", "_mappers")

    def __init__(self, query: Select, session: Session, mappers: Sequence[_Mapper]):
        self._query = query
        self._session = session
        self._mappers = mappers

    def execute(self) -> Iterator:
        if not self._mappers:
            yield from self._session.execute(self._query).__iter__()
        else:
            for row in self._session.execute(self._query):
                yield self._map_child_entities(row)

    def execute_with_pagination(self, page: int, page_size: int) -> Iterator:
        paged_query = self._query.limit(page_size).offset(page * page_size)
        if not self._mappers:
            yield from self._session.execute(paged_query).__iter__()
        else:
            for row in self._session.execute(self._query):
                yield self._map_child_entities(row)

    def record_count(self) -> int:
        page_info_query = self._query.with_only_columns(
            func.count(), maintain_column_froms=True
        ).order_by(None)
        # alternative
        # page_info_query = select(func.count()).select_from(self._query.order_by(None).subquery())
        return self._session.execute(page_info_query).scalar_one()

    def _map_child_entities(self, row: Row) -> Record:
        record = Record.from_row(row)
        for mapper in self._mappers:
            child_record = Record()
            entity_exists = False
            for name, value in record.items():
                if name == mapper.prefix:
                    entity_exists = bool(value)
                elif name.startswith(mapper.prefix):
                    child_record[name[len(mapper.prefix) :]] = value

            if not entity_exists or len(child_record) == 0:
                continue

            base_record = record
            for path_segment in mapper.path[:-1]:
                seg_record = base_record.get(path_segment)
                if seg_record is None:
                    seg_record = base_record[path_segment] = Record()
                base_record = seg_record

            base_record[mapper.path[-1]] = child_record
        return record


class QueryBuilder:
    __slots__ = ("_root_rule", "_arg_rules")

    def __init__(self, root_rule: InlineObjectRule, arg_rules: Sequence[ArgumentRule] = ()):
        self._root_rule = root_rule
        self._arg_rules = arg_rules

    @classmethod
    def create(
        cls, node: AnalyzedNode, arg_transformers: Sequence[ArgumentRule] = ()
    ) -> QueryBuilder:
        return cls(InlineObjectRule.create(node, None), arg_transformers)

    def build(
        self,
        root: Any,
        info: GraphQLResolveInfo,
        args: dict[str, Any],
        session: Session,
        sub_path: Sequence[str] = (),
    ) -> QueryExecutor:
        query = self._root_rule.base_query

        assert len(info.field_nodes) == 1
        walker = _FieldWalker(info.field_nodes[0], info)
        requested_fields: list[ColumnElement] = []
        mappers: list[_Mapper] = []
        for segment in sub_path:
            if not walker.descend(segment):
                # We may have paged request without actually going into field selection
                break
        else:
            processing_queue: deque[_FieldInfo | Literal[-1]] = deque(walker.children())
            context_queue: deque[tuple[InlineObjectRule, str, Subquery | None]] = deque()
            context_queue.append((self._root_rule, "", None))
            entity_counter = 1

            return_cmd: Literal[-1] = -1

            while processing_queue:
                field = processing_queue.popleft()
                if field == return_cmd:
                    walker.ascend()
                    context_queue.pop()
                    continue

                current_rule, alias_prefix, current_subquery = context_queue[-1]
                transformer = current_rule.fields.get(field.name)
                match transformer:
                    case None:
                        raise InvalidOperationException(
                            f"Field '{field.name}' does not have transformer"
                        )
                    case ColumnSelectRule():
                        selectable = transformer.selectable
                        sql_name = getattr(selectable, "name", None)
                        if current_subquery is not None:
                            if sql_name is None:
                                raise InvalidOperationException(
                                    "Cannot select over non named column via subquery"
                                )
                            selectable = current_subquery.columns[sql_name]
                        if sql_name != field.name or alias_prefix:
                            selectable = selectable.label(f"{alias_prefix}{field.name}")
                        requested_fields.append(selectable)
                    case LinkDataRule():
                        # TODO: determine if columns are already selected and don't reselect
                        for selectable in transformer.selectables:
                            sql_name = selectable.name
                            if current_subquery is not None:
                                selectable = current_subquery.columns[sql_name]
                            requested_fields.append(
                                selectable.label(f"{alias_prefix}__{sql_name}")
                            )
                    case InlineObjectRule():
                        alias_prefix = f"__e{entity_counter}_"
                        entity_counter += 1

                        target_from = transformer.reduce_select()
                        if target_from is not None:
                            for column in transformer.base_query.selected_columns:
                                if isinstance(column, Column) and not column.nullable:
                                    break
                            else:
                                # couldn't find column via which we will determine if
                                # entity is present, switch to subquery mode
                                target_from = None

                        if target_from is not None:
                            subquery = None
                            requested_fields.append(column.is_not(None).label(alias_prefix))
                            query = query.outerjoin(
                                target_from, self._construct_join_clause(transformer.join)
                            )
                        else:
                            subquery = transformer.base_query.add_columns(
                                literal(True).label(alias_prefix)
                            ).alias()
                            requested_fields.append(subquery.columns[alias_prefix])
                            query = query.outerjoin(
                                subquery, self._construct_join_clause(transformer.join, subquery)
                            )

                        # descend into field
                        walker.descend(field.name)
                        processing_queue.appendleft(return_cmd)
                        processing_queue.extendleft(reversed(list(walker.children())))
                        context_queue.append((transformer, alias_prefix, subquery))

                        # store mapper for this object
                        mappers.append(
                            _Mapper(alias_prefix, walker.current_relative_path[len(sub_path) :])
                        )
                    case _:
                        raise NotImplementedError(
                            f"Application of transformer '{type(transformer)!r}' is not supported"
                        )

        if requested_fields:
            # Select only requested fields. Otherwise keep selection as is, since we require at
            # least single field (and we may want to do filter on top of it)
            query = query.with_only_columns(*requested_fields)

        for rule in self._arg_rules:
            query = rule.apply(query, root, info, args)

        return QueryExecutor(query, session, mappers)

    @classmethod
    def _construct_join_clause(
        cls, join: JoinPoint | None, target_subquery: Subquery | None = None
    ) -> ColumnElement:
        if join is None:
            raise ValueError("Cannot construct join clause without join point")

        joins = join.joins
        target: ColumnElement
        source, target = joins[0]
        if target_subquery is not None:
            target = target_subquery.columns[target.name]
        condition = source == target
        for source, target in joins[1:]:
            if target_subquery is not None:
                target = target_subquery.columns[target.name]
            condition = condition and (source == target)
        return condition
