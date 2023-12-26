from collections.abc import Mapping
from typing import Any, NamedTuple

from graphql import (
    GraphQLEnumType,
    GraphQLInputField,
    GraphQLInputObjectType,
    GraphQLList,
    GraphQLResolveInfo,
    GraphQLScalarType,
)
from graphql.pyutils import snake_to_camel
from sqlalchemy import ColumnExpressionArgument, Select

from sqlgraphql._ast import AnalyzedNode
from sqlgraphql._builders.filtering.base import FilterOp, TypeFilterRegistry
from sqlgraphql._builders.filtering.filters import BUILTIN_FILTERS
from sqlgraphql._builders.util import GQLFieldModifiers
from sqlgraphql._gql import TypeMap
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
            arg_name="filter",
            arg_gql_type=GraphQLList(filter_object.gql_object),
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
                self._type_map.get_unique_name(node.node.name, self._TYPE_SUFFIX), fields
            )
        )
        return _FilterInputType(gql_type, apply_map)


_ScalarFilterData = dict[str, Any]
_EntityFilterData = dict[str, _ScalarFilterData]


class FilterQueryTransformer:
    def __init__(self, apply_map: Mapping[tuple[str, str], FilterOp]):
        self._apply_map = apply_map

    def __call__(
        self,
        query: Select,
        node: AnalyzedNode,
        info: GraphQLResolveInfo,
        *,
        filter: list[_EntityFilterData] | None = None,
        **kwargs: Any,
    ) -> Select:
        if not filter:
            return query

        return query.where(*(self._apply_filter(entry) for entry in filter))

    def _apply_filter(self, filter_data: _EntityFilterData) -> ColumnExpressionArgument[bool]:
        field_name, filter_pair = get_single_key_value(filter_data)
        filter_name, filter_value = get_single_key_value(filter_pair)
        apply_func = self._apply_map[(field_name, filter_name)]
        return apply_func(filter_value)
