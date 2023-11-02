import pytest
from graphql import print_schema
from sqlalchemy import select

from sqlgraphql.builders import SchemaBuilder
from sqlgraphql.model import QueryableNode
from tests.integration.conftest import UserDB


class TestBasicSelectWithExplicitORMQuery:
    @pytest.fixture()
    def schema(self):
        user_node = QueryableNode(
            "User", query=select(UserDB.id, UserDB.name, UserDB.registration_date)
        )
        return SchemaBuilder().add_root_list("users", user_node).build()

    def test_gql_schema_is_as_expected(self, schema):
        assert print_schema(schema) == (
            "type Query {\n"
            "  users: [User]\n"
            "}\n"
            "\n"
            "type User {\n"
            "  id: Int!\n"
            "  name: String!\n"
            "  registrationDate: String!\n"
            "}"
        )

    def test_select_all_names(self, schema, executor, query_watcher):
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
        assert query_watcher.executed_queries == ["SELECT users.name FROM users"]

    def test_casing_transformation(self, schema, executor, query_watcher):
        result = executor(
            schema,
            """
            query {
                users {
                    id
                    registrationDate
                }
            }
            """,
        )
        assert not result.errors
        assert result.data == {
            "users": [
                dict(id=1, registrationDate="2000-01-01"),
                dict(id=2, registrationDate="2000-01-02"),
            ]
        }
        assert query_watcher.executed_queries == [
            "SELECT users.id, users.registration_date FROM users"
        ]


class TestSimpleSelectWithORMEntity:
    @pytest.fixture()
    def schema(self):
        user_node = QueryableNode("User", query=select(UserDB))
        return SchemaBuilder().add_root_list("users", user_node).build()

    def test_gql_schema_is_as_expected(self, schema):
        assert print_schema(schema) == (
            "type Query {\n"
            "  users: [User]\n"
            "}\n"
            "\n"
            "type User {\n"
            "  id: Int!\n"
            "  name: String!\n"
            "  registrationDate: String!\n"
            "}"
        )

    def test_select_all_names_with_implicit_query(self, schema, executor, query_watcher):
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
        assert query_watcher.executed_queries == ["SELECT users.name FROM users"]


class TestSimpleSelectWithCoreQuery:
    @pytest.fixture()
    def schema_explicit(self):
        table = UserDB.__table__
        user_node = QueryableNode("User", query=select(table.c.id, table.c.name))
        return SchemaBuilder().add_root_list("users", user_node).build()

    @pytest.fixture()
    def schema_implicit(self):
        table = UserDB.__table__
        user_node = QueryableNode("User", query=select(table))
        return SchemaBuilder().add_root_list("users", user_node).build()

    def test_select_all_names_with_explicit_query(self, schema_explicit, executor, query_watcher):
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
        assert query_watcher.executed_queries == ["SELECT users.name FROM users"]

    def test_select_all_names_with_implicit_query(self, schema_implicit, executor, query_watcher):
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
        assert query_watcher.executed_queries == ["SELECT users.name FROM users"]
