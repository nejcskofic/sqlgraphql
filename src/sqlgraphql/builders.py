from collections.abc import Iterable
from typing import TypedDict

from graphql import (
    GraphQLField,
    GraphQLInt,
    GraphQLList,
    GraphQLNonNull,
    GraphQLObjectType,
    GraphQLResolveInfo,
    GraphQLScalarType,
    GraphQLString,
)
from graphql.pyutils import snake_to_camel
from sqlalchemy import Date, Integer, Row, String
from sqlalchemy.orm import Session
from sqlalchemy.sql.type_api import TypeEngine

from sqlgraphql.model import QueryableNode
from sqlgraphql.types import SimpleResolver


class TypedResolveContext(TypedDict):
    db_session: Session


def build_list(node: QueryableNode) -> GraphQLField:
    def build_field_resolver(name: str) -> SimpleResolver:
        def resolver(parent: Row, info: GraphQLResolveInfo) -> object | None:
            return parent._mapping.get(name)

        return resolver

    object_type = GraphQLObjectType(
        node.name,
        {
            snake_to_camel(entry.name, upper=False): GraphQLField(
                _convert_to_gql_type(entry.type, entry.required),
                resolve=build_field_resolver(entry.name),
            )
            for entry in node.get_select_elements()
        },
    )

    def resolver(parent: object, info: GraphQLResolveInfo) -> Iterable:
        context: TypedResolveContext = info.context
        return context["db_session"].execute(node.query)

    return GraphQLField(GraphQLList(object_type), resolve=resolver)


def _convert_to_gql_type(
    sql_type: TypeEngine, required: bool
) -> GraphQLScalarType | GraphQLNonNull:
    # TODO: introduce abstraction
    sql_type_cls = type(sql_type)
    if sql_type_cls is Integer:
        gql_type = GraphQLInt
    elif sql_type_cls in [String, Date]:
        gql_type = GraphQLString
    else:
        raise NotImplementedError("Not yet implemented")

    if required:
        return GraphQLNonNull(gql_type)
    else:
        return gql_type
