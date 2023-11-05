import datetime
from typing import Mapping, Any

import graphql
from graphql import GraphQLError, print_ast
from graphql.pyutils import inspect


def _serialize_date(output_value: Any) -> str:
    if isinstance(output_value, datetime.date):
        return output_value.isoformat()
    elif isinstance(output_value, datetime.datetime):
        return output_value.date().isoformat()
    elif isinstance(output_value, str):
        return datetime.date.fromisoformat(output_value).isoformat()
    else:
        raise GraphQLError(
            "Date cannot represent non date value: " + inspect(output_value)
        )


def _coerce_date(input_value: Any) -> datetime.date:
    if not isinstance(input_value, str):
        raise GraphQLError(
            "Date cannot represent non date value: " + inspect(input_value)
        )
    return datetime.date.fromisoformat(input_value)


def _parse_date_literal(value_node: graphql.ValueNode, _variables: Any = None) -> datetime.date:
    if not isinstance(value_node, graphql.StringValueNode):
        raise GraphQLError(
            "Date cannot represent non date value: " + print_ast(value_node),
            value_node,
        )
    return datetime.date.fromisoformat(value_node.value)


GraphQLDate = graphql.GraphQLScalarType(
    name="Date",
    description=(
        "Date scalar type represents date in ISO format"
        "(YYYY-MM-DD)."
    ),
    serialize=_serialize_date,
    parse_value=_coerce_date,
    parse_literal=_parse_date_literal,
)


_DEFAULT_TYPE_MAP: Mapping[type, graphql.GraphQLScalarType] = {
    bool: graphql.GraphQLBoolean,
    float: graphql.GraphQLFloat,
    int: graphql.GraphQLInt,
    str: graphql.GraphQLString,
    datetime.date: GraphQLDate
}


class ScalarTypeRegistry:
    def __init__(self, type_map_overrides: Mapping[type, graphql.GraphQLScalarType] | None = None):
        mapping = dict(_DEFAULT_TYPE_MAP)
        if type_map_overrides:
            mapping.update(type_map_overrides)
        self._mapping = mapping

    def get_scalar_type(self, python_type: type, required: bool) -> graphql.GraphQLScalarType | graphql.GraphQLNonNull:
        gql_type = self._mapping.get(python_type)
        if gql_type is None:
            raise ValueError(f"Type '{python_type!r}' does not have GQL scalar equivalent")

        if required:
            return graphql.GraphQLNonNull(gql_type)
        else:
            return gql_type
