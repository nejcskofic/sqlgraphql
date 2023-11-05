import base64
import datetime
from decimal import Decimal
from typing import Any

from graphql import (
    FloatValueNode,
    GraphQLError,
    GraphQLScalarType,
    IntValueNode,
    StringValueNode,
    ValueNode,
    print_ast,
)
from graphql.pyutils import inspect


def _serialize_date(output_value: Any) -> str:
    if isinstance(output_value, datetime.date):
        return output_value.isoformat()
    elif isinstance(output_value, datetime.datetime):
        return output_value.date().isoformat()
    else:
        raise GraphQLError("Date cannot represent non date value: " + inspect(output_value))


def _coerce_date(input_value: Any) -> datetime.date:
    if isinstance(input_value, datetime.date):
        return input_value
    elif isinstance(input_value, datetime.datetime):
        return input_value.date()
    elif isinstance(input_value, str):
        try:
            return datetime.date.fromisoformat(input_value)
        except ValueError:
            pass

    raise GraphQLError("Date cannot represent non date value: " + inspect(input_value))


def _parse_date_literal(value_node: ValueNode, _variables: Any = None) -> datetime.date:
    if isinstance(value_node, StringValueNode):
        try:
            return datetime.date.fromisoformat(value_node.value)
        except ValueError:
            pass

    raise GraphQLError(
        "Date cannot represent non date value: " + print_ast(value_node),
        value_node,
    )


GraphQLDate = GraphQLScalarType(
    name="Date",
    description="Date scalar type represents date in ISO format (YYYY-MM-DD).",
    serialize=_serialize_date,
    parse_value=_coerce_date,
    parse_literal=_parse_date_literal,
)


def _serialize_datetime(output_value: Any) -> str:
    if isinstance(output_value, datetime.datetime):
        return output_value.isoformat()
    else:
        raise GraphQLError(
            "DateTime cannot represent non datetime value: " + inspect(output_value)
        )


def _coerce_datetime(input_value: Any) -> datetime.datetime:
    if isinstance(input_value, datetime.datetime):
        return input_value
    elif isinstance(input_value, str):
        try:
            return datetime.datetime.fromisoformat(input_value)
        except ValueError:
            pass

    raise GraphQLError("DateTime cannot represent non datetime value: " + inspect(input_value))


def _parse_datetime_literal(value_node: ValueNode, _variables: Any = None) -> datetime.datetime:
    if isinstance(value_node, StringValueNode):
        try:
            return datetime.datetime.fromisoformat(value_node.value)
        except ValueError:
            pass

    raise GraphQLError(
        "DateTime cannot represent non datetime value: " + print_ast(value_node),
        value_node,
    )


GraphQLDateTime = GraphQLScalarType(
    name="DateTime",
    description=(
        "DateTime scalar type represents datetime in ISO 8601 format. Timezone information"
        " is present if date is zone aware."
    ),
    serialize=_serialize_datetime,
    parse_value=_coerce_datetime,
    parse_literal=_parse_datetime_literal,
)


def _serialize_time(output_value: Any) -> str:
    if isinstance(output_value, datetime.time):
        return output_value.isoformat()
    elif isinstance(output_value, datetime.datetime):
        return output_value.time().isoformat()
    else:
        raise GraphQLError("Time cannot represent non time value: " + inspect(output_value))


def _coerce_time(input_value: Any) -> datetime.time:
    if isinstance(input_value, datetime.time):
        return input_value
    elif isinstance(input_value, datetime.datetime):
        return input_value.time()
    elif isinstance(input_value, str):
        try:
            return datetime.time.fromisoformat(input_value)
        except ValueError:
            pass

    raise GraphQLError("Time cannot represent non time value: " + inspect(input_value))


def _parse_time_literal(value_node: ValueNode, _variables: Any = None) -> datetime.time:
    if isinstance(value_node, StringValueNode):
        try:
            return datetime.time.fromisoformat(value_node.value)
        except ValueError:
            pass

    raise GraphQLError(
        "Time cannot represent non time value: " + print_ast(value_node),
        value_node,
    )


GraphQLTime = GraphQLScalarType(
    name="Time",
    description=("Time scalar type represents datetime in ISO 8601 format."),
    serialize=_serialize_time,
    parse_value=_coerce_time,
    parse_literal=_parse_time_literal,
)


def _serialize_bytes(output_value: Any) -> str:
    if isinstance(output_value, bytes | bytearray):
        return base64.b64encode(output_value).decode("ascii")
    else:
        raise GraphQLError("Base64 cannot represent non bytes value: " + inspect(output_value))


def _coerce_bytes(input_value: Any) -> bytes:
    if isinstance(input_value, bytes | bytearray):
        return input_value
    elif isinstance(input_value, str):
        try:
            return base64.b64decode(input_value)
        except ValueError:
            pass

    raise GraphQLError("Base64 cannot represent non bytes value: " + inspect(input_value))


def _parse_bytes_literal(value_node: ValueNode, _variables: Any = None) -> bytes:
    if isinstance(value_node, StringValueNode):
        try:
            return base64.b64decode(value_node.value)
        except ValueError:
            pass

    raise GraphQLError(
        "Base64 cannot represent non bytes value: " + print_ast(value_node),
        value_node,
    )


GraphQLBase64 = GraphQLScalarType(
    name="Base64",
    description="Bytes object serialized as base64.",
    serialize=_serialize_bytes,
    parse_value=_coerce_bytes,
    parse_literal=_parse_bytes_literal,
)


def _serialize_decimal(output_value: Any) -> str:
    if isinstance(output_value, Decimal):
        return str(output_value)
    elif isinstance(output_value, int | float):
        return str(Decimal(output_value))
    else:
        raise GraphQLError("Decimal cannot represent non decimal value: " + inspect(output_value))


def _coerce_decimal(input_value: Any) -> Decimal:
    if isinstance(input_value, Decimal):
        return input_value
    elif isinstance(input_value, int | float):
        return Decimal(input_value)
    elif isinstance(input_value, str):
        try:
            return Decimal(input_value)
        except ValueError:
            pass

    raise GraphQLError("Decimal cannot represent non decimal value: " + inspect(input_value))


def _parse_decimal_literal(value_node: ValueNode, _variables: Any = None) -> Decimal:
    if isinstance(value_node, StringValueNode | IntValueNode | FloatValueNode):
        try:
            return Decimal(value_node.value)
        except ValueError:
            pass

    raise GraphQLError(
        "Decimal cannot represent non decimal value: " + print_ast(value_node),
        value_node,
    )


GraphQLDecimal = GraphQLScalarType(
    name="Decimal",
    description=("Scalar representing number for exact arithmetic"),
    serialize=_serialize_decimal,
    parse_value=_coerce_decimal,
    parse_literal=_parse_decimal_literal,
)
