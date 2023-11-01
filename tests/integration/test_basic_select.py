import pytest
from graphql import GraphQLObjectType, GraphQLSchema
from sqlalchemy import select

from sqlgraphql.builders import build_list
from sqlgraphql.model import QueryableNode
from tests.integration.conftest import UserDB


class TestBasicSelectWithExplicitORMQuery:
    @pytest.fixture()
    def schema(self):
        user_node = QueryableNode(
            "User", query=select(UserDB.id, UserDB.name, UserDB.registration_date)
        )
        query_type = GraphQLObjectType(
            "Query",
            {"users": build_list(user_node)},
        )
        return GraphQLSchema(query_type)

    def test_select_all_names(self, schema, executor):
        result = executor(
            schema,
            """
            query {
                users {
                    name
                }
            }
            """,
        )
        assert not result.errors
        assert result.data == {
            "users": [
                dict(name="user1"),
                dict(name="user2"),
            ]
        }

    def test_casing_transformation(self, schema, executor):
        result = executor(
            schema,
            """
            query {
                users {
                    registrationDate
                }
            }
            """,
        )
        assert not result.errors
        assert result.data == {
            "users": [
                dict(registrationDate="2000-01-01"),
                dict(registrationDate="2000-01-02"),
            ]
        }


class TestSimpleSelectWithORMEntity:
    @pytest.fixture()
    def schema(self):
        user_node = QueryableNode("User", query=select(UserDB))
        query_type = GraphQLObjectType(
            "Query",
            {"users": build_list(user_node)},
        )
        return GraphQLSchema(query_type)

    def test_select_all_names_with_implicit_query(self, schema, executor):
        result = executor(
            schema,
            """
            query {
                users {
                    name
                }
            }
            """,
        )
        assert not result.errors
        assert result.data == {
            "users": [
                dict(name="user1"),
                dict(name="user2"),
            ]
        }


class TestSimpleSelectWithCoreQuery:
    @pytest.fixture()
    def schema_explicit(self):
        table = UserDB.__table__
        user_node = QueryableNode("User", query=select(table.c.id, table.c.name))
        query_type = GraphQLObjectType(
            "Query",
            {"users": build_list(user_node)},
        )
        return GraphQLSchema(query_type)

    @pytest.fixture()
    def schema_implicit(self):
        table = UserDB.__table__
        user_node = QueryableNode("User", query=select(table))
        query_type = GraphQLObjectType(
            "Query",
            {"users": build_list(user_node)},
        )
        return GraphQLSchema(query_type)

    def test_select_all_names_with_explicit_query(self, schema_explicit, executor):
        result = executor(
            schema_explicit,
            """
            query {
                users {
                    name
                }
            }
            """,
        )
        assert not result.errors
        assert result.data == {
            "users": [
                dict(name="user1"),
                dict(name="user2"),
            ]
        }

    def test_select_all_names_with_implicit_query(self, schema_implicit, executor):
        result = executor(
            schema_implicit,
            """
            query {
                users {
                    name
                }
            }
            """,
        )
        assert not result.errors
        assert result.data == {
            "users": [
                dict(name="user1"),
                dict(name="user2"),
            ]
        }
