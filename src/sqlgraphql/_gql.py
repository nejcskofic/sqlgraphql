import datetime
from collections.abc import Mapping
from decimal import Decimal
from typing import cast
from uuid import UUID

import graphql

from sqlgraphql import gql_scalars
from sqlgraphql.types import AnyJsonValue

_DEFAULT_TYPE_MAP: Mapping[type, graphql.GraphQLScalarType] = {
    bool: graphql.GraphQLBoolean,
    float: graphql.GraphQLFloat,
    int: graphql.GraphQLInt,
    str: graphql.GraphQLString,
    datetime.date: gql_scalars.GraphQLDate,
    datetime.datetime: gql_scalars.GraphQLDateTime,
    datetime.time: gql_scalars.GraphQLTime,
    UUID: graphql.GraphQLID,
    bytes: gql_scalars.GraphQLBase64,
    Decimal: gql_scalars.GraphQLDecimal,
    cast(type, AnyJsonValue): gql_scalars.GraphQLJson,
}


class ScalarTypeRegistry:
    def __init__(self, type_map_overrides: Mapping[type, graphql.GraphQLScalarType] | None = None):
        mapping = dict(_DEFAULT_TYPE_MAP)
        if type_map_overrides:
            mapping.update(type_map_overrides)
        self._mapping = mapping

    def get_scalar_type(
        self, python_type: type, required: bool
    ) -> graphql.GraphQLScalarType | graphql.GraphQLNonNull:
        gql_type = self._mapping.get(python_type)
        if gql_type is None:
            raise ValueError(f"Type '{python_type!r}' does not have GQL scalar equivalent")

        if required:
            return graphql.GraphQLNonNull(gql_type)
        else:
            return gql_type
