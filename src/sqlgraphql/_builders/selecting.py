import enum
from collections.abc import Iterator, Mapping
from contextlib import contextmanager

from graphql import (
    GraphQLEnumType,
    GraphQLField,
    GraphQLNonNull,
    GraphQLObjectType,
    GraphQLScalarType,
    ThunkMapping,
)

from sqlgraphql._ast import AnalyzedField, AnalyzedNode
from sqlgraphql._builders.enum import EnumBuilder
from sqlgraphql._gql import ScalarTypeRegistry, TypeMap
from sqlgraphql._orm import TypeRegistry
from sqlgraphql._resolvers import DbFieldResolver
from sqlgraphql._transformers import ColumnSelectRule, FieldRules, InlineObjectRule
from sqlgraphql._utils import assert_not_none


class ObjectBuilder:
    def __init__(
        self,
        type_map: TypeMap,
        enum_builder: EnumBuilder,
        orm_type_registry: TypeRegistry,
        gql_type_registry: ScalarTypeRegistry,
    ):
        self._type_map = type_map
        self._enum_builder = enum_builder
        self._orm_type_registry = orm_type_registry
        self._gql_type_registry = gql_type_registry

    def build_object(self, node: AnalyzedNode) -> GraphQLObjectType:
        data = node.data
        if data.gql_type is None:
            with self._build_gql_object(node) as result:
                data.gql_type, data.field_rules = result
        return data.gql_type

    @contextmanager
    def _build_gql_object(
        self, node: AnalyzedNode
    ) -> Iterator[tuple[GraphQLObjectType, dict[str, FieldRules]]]:
        fields = {}
        rules: dict[str, FieldRules] = {}

        for entry in node.fields.values():
            gql_type = self._convert_to_gql_type(entry)
            fields[entry.gql_name] = GraphQLField(
                GraphQLNonNull(gql_type) if entry.required else gql_type,
                resolve=DbFieldResolver(entry.gql_name),
            )
            rules[entry.gql_name] = ColumnSelectRule(
                entry.orm_field.label(entry.gql_name)
                if entry.gql_name != entry.orm_name
                else entry.orm_field
            )

        if node.links:
            # we need to process all children
            linked_nodes = [link.node for link in node.links.values()]
            links = [link for link in node.links.values()]

            # we need to lazy load fields so that recursive definitions work
            def factory() -> Mapping[str, GraphQLField]:
                for link in links:
                    # TODO: is relationship required or not?
                    fields[link.gql_name] = GraphQLField(assert_not_none(link.node.data.gql_type))

                return fields

            field_arg: ThunkMapping[GraphQLField] = factory

            for link in node.links.values():
                rules[link.gql_name] = InlineObjectRule.create(link.node)
        else:
            field_arg = fields
            linked_nodes = []

        yield (
            self._type_map.add(
                GraphQLObjectType(
                    self._type_map.get_unique_name(node.node.name),
                    field_arg,
                )
            ),
            rules,
        )

        for node in linked_nodes:
            self.build_object(node)

    def _convert_to_gql_type(self, field: AnalyzedField) -> GraphQLScalarType | GraphQLEnumType:
        data = field.data
        if data.gql_type is not None:
            return data.gql_type

        # If field is enum, build enum
        enum_type = self._enum_builder.build_from_field(field)
        if enum_type is not None:
            data.python_type = enum.Enum
            data.gql_type = enum_type
            return enum_type

        # Treat as scalar
        data.python_type = python_type = self._orm_type_registry.get_python_type(field.orm_type)
        data.gql_type = self._gql_type_registry.get_scalar_type(python_type)
        return data.gql_type
