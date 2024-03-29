from __future__ import annotations

from collections.abc import Mapping
from typing import Any, NamedTuple, Union, cast

from graphql import (
    GraphQLArgument,
    GraphQLEnumType,
    GraphQLInputField,
    GraphQLInputObjectType,
    GraphQLList,
    GraphQLNonNull,
    GraphQLResolveInfo,
    GraphQLScalarType,
)
from graphql.pyutils import snake_to_camel
from sqlalchemy import ColumnExpressionArgument, Select, and_, not_, or_

from sqlgraphql._ast import AnalyzedNode
from sqlgraphql._builders.filtering.base import FilterOp, TypeFilterRegistry
from sqlgraphql._builders.filtering.filters import BUILTIN_FILTERS
from sqlgraphql._builders.util import GQLFieldModifiers
from sqlgraphql._gql import TypeMap
from sqlgraphql._transformers import ArgumentRule
from sqlgraphql._utils import CacheDict, get_single_key_value
from sqlgraphql.exceptions import GQLBuilderException


class _TypeKey(NamedTuple):
    python_type: type
    gql_type: GraphQLScalarType | GraphQLEnumType


class _ScalarFilterType(NamedTuple):
    gql_object: GraphQLInputObjectType
    apply_map: Mapping[str, type[FilterOp]]


class _FilterInputType(NamedTuple):
    gql_object: GraphQLInputObjectType
    apply_map: Mapping[tuple[str, str], FilterOp]


class _CompositeFilterNames:
    AND = "_and"
    OR = "_or"
    NOT = "_not"


class FilteringArgumentBuilder:
    _TYPE_SUFFIX = "FilterInputObject"

    def __init__(self, type_map: TypeMap):
        self._type_map = type_map
        self._type_filter_registry = TypeFilterRegistry(BUILTIN_FILTERS)
        self._scalar_gql_type_cache = CacheDict[_TypeKey, _ScalarFilterType](
            self._build_scalar_filter_gql_type
        )
        self._gql_input_object_type_cache = CacheDict[AnalyzedNode, _FilterInputType](
            self._build_input_object_filter
        )

    def build_filter(self, node: AnalyzedNode) -> GQLFieldModifiers:
        filter_object = self._gql_input_object_type_cache[node]
        return GQLFieldModifiers(
            dict(filter=GraphQLArgument(GraphQLList(filter_object.gql_object))),
            transformer=FilterQueryTransformer(filter_object.apply_map),
        )

    def _build_scalar_filter_gql_type(self, key: _TypeKey) -> _ScalarFilterType:
        filters_specs = self._type_filter_registry.get_specs_for_type(key.python_type)
        fields = {}
        appliers = {}
        for entry in filters_specs:
            if entry.meta.op in fields:
                raise GQLBuilderException(
                    f"Duplicate filter for type {key.python_type}: {entry.meta.op}"
                )
            fields[entry.meta.op] = entry.build_input_field(key.gql_type)
            appliers[entry.meta.op] = entry

        gql_filter_type = self._type_map.add(
            GraphQLInputObjectType(
                self._type_map.get_unique_name(
                    snake_to_camel(key.python_type.__name__), self._TYPE_SUFFIX
                ),
                fields,
            )
        )
        return _ScalarFilterType(gql_filter_type, appliers)

    def _build_input_object_filter(self, node: AnalyzedNode) -> _FilterInputType:
        fields = {}
        apply_map = {}
        for field in node.fields.values():
            assert field.data.gql_type is not None
            assert field.data.python_type is not None

            scalar_type = self._scalar_gql_type_cache[
                _TypeKey(field.data.python_type, field.data.gql_type)
            ]
            fields[field.gql_name] = GraphQLInputField(scalar_type.gql_object)
            for filter_name, op in scalar_type.apply_map.items():
                apply_map[(field.gql_name, filter_name)] = op(field.orm_field)

        gql_type = self._type_map.add(
            GraphQLInputObjectType(
                self._type_map.get_unique_name(node.node.name, self._TYPE_SUFFIX), lambda: fields
            )
        )

        # modify fields to include composites
        self._apply_filter_composites(gql_type, fields)

        return _FilterInputType(gql_type, apply_map)

    @classmethod
    def _apply_filter_composites(
        cls, gql_input_object: GraphQLInputObjectType, fields: dict[str, GraphQLInputField]
    ) -> None:
        fields[_CompositeFilterNames.AND] = GraphQLInputField(
            GraphQLList(GraphQLNonNull(gql_input_object))
        )
        fields[_CompositeFilterNames.OR] = GraphQLInputField(
            GraphQLList(GraphQLNonNull(gql_input_object))
        )
        fields[_CompositeFilterNames.NOT] = GraphQLInputField(gql_input_object)


_ScalarFilterData = dict[str, Any]
_EntityFilterData = dict[
    str, Union[_ScalarFilterData, "_EntityFilterData", list["_EntityFilterData"]]
]


class FilterQueryTransformer(ArgumentRule):
    def __init__(self, apply_map: Mapping[tuple[str, str], FilterOp]):
        self._apply_map = apply_map

    def apply(
        self, query: Select, root: Any, info: GraphQLResolveInfo, args: dict[str, Any]
    ) -> Select:
        filter: list[_EntityFilterData] = args.pop("filter", [])
        if not filter:
            return query

        return query.where(*(self._apply_filter(entry) for entry in filter))

    def _apply_filter(self, filter_data: _EntityFilterData) -> ColumnExpressionArgument[bool]:
        field_name, filter_arg = get_single_key_value(filter_data)
        if field_name == _CompositeFilterNames.NOT:
            return not_(self._apply_filter(cast(_EntityFilterData, filter_arg)))
        elif field_name == _CompositeFilterNames.AND:
            filter_arg = cast(list[_EntityFilterData], filter_arg)
            if not len(filter_arg):
                raise ValueError("There should be at least one element for AND block")
            return and_(*(self._apply_filter(entry) for entry in filter_arg))
        elif field_name == _CompositeFilterNames.OR:
            filter_arg = cast(list[_EntityFilterData], filter_arg)
            if not len(filter_arg):
                raise ValueError("There should be at least one element for OR block")
            return or_(*(self._apply_filter(entry) for entry in filter_arg))
        else:
            filter_name, filter_value = get_single_key_value(cast(_ScalarFilterData, filter_arg))
            apply_func = self._apply_map[(field_name, filter_name)]
            return apply_func(filter_value)
