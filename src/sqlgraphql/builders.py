from __future__ import annotations

import enum
from collections.abc import Callable, Mapping
from typing import Any, NamedTuple

from graphql import (
    GraphQLEnumType,
    GraphQLField,
    GraphQLList,
    GraphQLNonNull,
    GraphQLObjectType,
    GraphQLScalarType,
    GraphQLSchema,
)
from graphql.pyutils import snake_to_camel
from sqlalchemy import Column, Enum
from sqlalchemy.sql.type_api import TypeEngine

from sqlgraphql._ast import AnalyzedField, AnalyzedNode
from sqlgraphql._gql import ScalarTypeRegistry
from sqlgraphql._orm import TypeRegistry
from sqlgraphql._resolvers import DbFieldResolver, ListResolver
from sqlgraphql.model import QueryableNode


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
        self._enum_builder = _EnumBuilder()

    def add_root_list(self, name: str, node: QueryableNode) -> SchemaBuilder:
        if name in self._query_root_members:
            raise ValueError(f"Name '{name}' has already been used")

        analyzed_node = self._get_analyzed_node(node)

        object_type = GraphQLObjectType(
            node.name,
            {
                entry.gql_name: GraphQLField(
                    self._convert_to_gql_type(entry),
                    resolve=DbFieldResolver(entry.orm_name),
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

    def _convert_to_gql_type(
        self, field: AnalyzedField
    ) -> GraphQLScalarType | GraphQLNonNull | GraphQLEnumType:
        enum_type = self._enum_builder.build_from_field(field)
        if enum_type is not None:
            return enum_type
        python_type = self._orm_type_registry.get_python_type(field.orm_type)
        return self._gql_type_registry.get_scalar_type(python_type, field.required)


class _EnumType(NamedTuple):
    value: type[enum.Enum]


class _ExplicitMappings(NamedTuple):
    value: Mapping[str, Any]


_AnalyzedEnum = _EnumType | _ExplicitMappings


class _EnumBuilder:
    # TODO: Rework builder. Add ability to register custom handlers, support subclassing types
    def __init__(self) -> None:
        self._type_handlers = self._build_default_handlers()
        self._cache: dict[type[enum.Enum], GraphQLEnumType] = {}

    def build_from_field(
        self, field: AnalyzedField
    ) -> GraphQLEnumType | GraphQLNonNull[GraphQLEnumType] | None:
        handler = self._type_handlers.get(type(field.orm_type))
        if handler is None:
            return None

        analyzed_enum = handler(field.orm_type)
        match analyzed_enum:
            case _EnumType(enum_cls):
                gql_type = self._cache.get(enum_cls)
                if gql_type is None:
                    # Library will either use enum values or enum keys. It will not work with actual
                    # enum object.
                    # https://github.com/graphql-python/graphql-core/issues/73
                    gql_type = GraphQLEnumType(
                        enum_cls.__name__,
                        {key: value for key, value in enum_cls.__members__.items()},
                    )
                    self._cache[enum_cls] = gql_type
            case _ExplicitMappings(value):
                # We don't cache implicit enums
                gql_type = GraphQLEnumType(snake_to_camel(field.orm_name), value)
            case _:
                assert False, "Should not happen"

        if field.required:
            return GraphQLNonNull(gql_type)
        else:
            return gql_type

    @classmethod
    def _build_default_handlers(cls) -> Mapping[type[TypeEngine], Callable[[Any], _AnalyzedEnum]]:
        def enum_handler(type_: Enum) -> _AnalyzedEnum:
            if type_.enum_class is not None:
                return _EnumType(type_.enum_class)
            else:
                return _ExplicitMappings({value: value for value in type_.enums})

        mapping: dict[type, Callable[[Any], _AnalyzedEnum]] = {Enum: enum_handler}

        try:
            import sqlalchemy_utils.types.choice as su

            def choice_handler(type_: su.ChoiceType) -> _AnalyzedEnum:
                if isinstance(type_.type_impl, su.EnumTypeImpl):
                    return _EnumType(type_.type_impl.enum_class)
                else:
                    # Choice works as attached data to lookup key, so we need to do identity map
                    return _ExplicitMappings({key: key for key in type_.type_impl.choices_dict})

            mapping[su.ChoiceType] = choice_handler
        except ImportError:
            pass

        return mapping
