import pytest
from graphql import print_schema
from sqlalchemy import select

from sqlgraphql.builders import SchemaBuilder
from sqlgraphql.model import QueryableNode
from tests.integration.conftest import UserDB


class TestSortableLists:
    @pytest.fixture()
    def schema(self):
        user_node = QueryableNode(
            "User", query=select(UserDB.id, UserDB.name, UserDB.registration_date)
        )
        return SchemaBuilder().add_root_list("users", user_node, sortable=True).build()

    def test_gql_schema_is_as_expected(self, schema):
        assert print_schema(schema) == (
            "type Query {\n"
            "  users(sort: [UserSortInputObject]): [User]\n"
            "}\n"
            "\n"
            "type User {\n"
            "  id: Int!\n"
            "  name: String!\n"
            "  registrationDate: Date!\n"
            "}\n"
            "\n"
            '"""Date scalar type represents date in ISO format (YYYY-MM-DD)."""\n'
            "scalar Date\n"
            "\n"
            "input UserSortInputObject {\n"
            "  id: SortDirection\n"
            "  name: SortDirection\n"
            "  registrationDate: SortDirection\n"
            "}\n"
            "\n"
            "enum SortDirection {\n"
            "  asc\n"
            "  desc\n"
            "}"
        )

    def test_unsorted(self, schema, executor, query_watcher):
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

    @pytest.mark.parametrize(
        ["sort_arg", "expected_data", "order_by_sql_part"],
        [
            ("{name: desc}", [dict(name="user2"), dict(name="user1")], "ORDER BY users.name DESC"),
            ("{name: asc}", [dict(name="user1"), dict(name="user2")], "ORDER BY users.name ASC"),
            (
                "{registrationDate: desc}",
                [dict(name="user2"), dict(name="user1")],
                "ORDER BY users.registration_date DESC",
            ),
            (
                "{registrationDate: asc}",
                [dict(name="user1"), dict(name="user2")],
                "ORDER BY users.registration_date ASC",
            ),
        ],
    )
    def test_sorted(
        self, sort_arg, expected_data, order_by_sql_part, schema, executor, query_watcher
    ):
        result = executor(
            schema,
            """
            query {
                users (sort: %s) {
                    name
                }
            }
            """
            % sort_arg,
        )
        assert not result.errors
        assert result.data == {"users": expected_data}
        assert query_watcher.executed_queries == [
            f"SELECT users.name FROM users {order_by_sql_part}"
        ]

    def test_sorting_by_multiple_fields(self, schema, executor, query_watcher):
        result = executor(
            schema,
            """
            query {
                users (sort: [{name: asc}, {id: desc}]) {
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
        assert query_watcher.executed_queries == [
            "SELECT users.name FROM users ORDER BY users.name ASC, users.id DESC"
        ]
