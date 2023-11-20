from graphql import (
    GraphQLEnumType,
    GraphQLField,
    GraphQLNonNull,
    GraphQLObjectType,
    GraphQLScalarType,
)

from sqlgraphql._ast import AnalyzedField, AnalyzedNode
from sqlgraphql._builders.enum import EnumBuilder
from sqlgraphql._gql import ScalarTypeRegistry, TypeMap
from sqlgraphql._orm import TypeRegistry
from sqlgraphql._resolvers import DbFieldResolver
from sqlgraphql._utils import CacheDict


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
        self._cache = CacheDict[AnalyzedNode, GraphQLObjectType](self._build_gql_object)

    def build_object(self, node: AnalyzedNode) -> GraphQLObjectType:
        return self._cache[node]

    def _build_gql_object(self, node: AnalyzedNode) -> GraphQLObjectType:
        fields = {}

        for entry in node.fields.values():
            gql_type = entry.data.gql_type
            if gql_type is None:
                entry.data.gql_type = gql_type = self._convert_to_gql_type(entry)
            fields[entry.gql_name] = GraphQLField(
                gql_type,
                resolve=DbFieldResolver(entry.orm_name),
            )

        return self._type_map.add(
            GraphQLObjectType(
                self._type_map.get_unique_name(node.node.name),
                fields,
            )
        )

    def _convert_to_gql_type(
        self, field: AnalyzedField
    ) -> GraphQLScalarType | GraphQLNonNull | GraphQLEnumType:
        enum_type = self._enum_builder.build_from_field(field)
        if enum_type is not None:
            return enum_type
        python_type = self._orm_type_registry.get_python_type(field.orm_type)
        return self._gql_type_registry.get_scalar_type(python_type, field.required)
