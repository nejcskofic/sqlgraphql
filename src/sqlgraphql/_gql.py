import datetime
from collections.abc import Mapping
from decimal import Decimal
from typing import TypeVar, cast
from uuid import UUID

import graphql

from sqlgraphql.gql import scalars
from sqlgraphql.types import AnyJsonValue

_DEFAULT_TYPE_MAP: Mapping[type, graphql.GraphQLScalarType] = {
    bool: graphql.GraphQLBoolean,
    float: graphql.GraphQLFloat,
    int: graphql.GraphQLInt,
    str: graphql.GraphQLString,
    datetime.date: scalars.GraphQLDate,
    datetime.datetime: scalars.GraphQLDateTime,
    datetime.time: scalars.GraphQLTime,
    UUID: graphql.GraphQLID,
    bytes: scalars.GraphQLBase64,
    Decimal: scalars.GraphQLDecimal,
    cast(type, AnyJsonValue): scalars.GraphQLJson,
}


TGraphQLNamedType = TypeVar("TGraphQLNamedType", bound=graphql.GraphQLNamedType)


class TypeMap:
    __slots__ = ("_map",)

    def __init__(self) -> None:
        self._map: dict[str, graphql.GraphQLNamedType] = {}

    def add(self, gql_type: TGraphQLNamedType) -> TGraphQLNamedType:
        name = gql_type.name
        if name in self._map:
            raise ValueError(f"Name '{name}' has already been registered.")
        self._map[name] = gql_type
        return gql_type

    def get_unique_name(self, name: str, suffix: str = "") -> str:
        unique_name = name + suffix
        if unique_name not in self._map:
            return unique_name

        idx = 1
        while True:
            unique_name = f"{name}{idx}{suffix}"
            if unique_name not in self._map:
                return unique_name
            idx += 1


class ScalarTypeRegistry:
    def __init__(
        self,
        type_map: TypeMap,
        type_map_overrides: Mapping[type, graphql.GraphQLScalarType] | None = None,
    ):
        mapping = dict(_DEFAULT_TYPE_MAP)
        if type_map_overrides:
            mapping.update(type_map_overrides)
        for entry in set(mapping.values()):
            type_map.add(entry)
        self._type_map = type_map
        self._mapping = mapping

    def get_scalar_type(self, python_type: type) -> graphql.GraphQLScalarType:
        gql_type = self._mapping.get(python_type)
        if gql_type is None:
            raise ValueError(f"Type '{python_type!r}' does not have GQL scalar equivalent")

        return gql_type
