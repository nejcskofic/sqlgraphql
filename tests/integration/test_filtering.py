import pytest
from graphql import print_schema
from sqlalchemy import select

from sqlgraphql.model import QueryableNode
from sqlgraphql.schema import SchemaBuilder
from tests.integration.conftest import UserDB


class TestFilterableLists:
    @pytest.fixture()
    def schema(self):
        user_node = QueryableNode(
            "User", query=select(UserDB.id, UserDB.name, UserDB.registration_date)
        )
        return SchemaBuilder().add_root_list("users", user_node, filterable=True).build()

    def test_gql_schema_is_as_expected(self, schema):
        assert print_schema(schema) == (
            "type Query {\n"
            "  users(filter: [UserFilterInputObject]): [User]\n"
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
            "input UserFilterInputObject {\n"
            "  id: IntFilterInputObject\n"
            "  name: StrFilterInputObject\n"
            "  registrationDate: DateFilterInputObject\n"
            "  _and: [UserFilterInputObject!]\n"
            "  _or: [UserFilterInputObject!]\n"
            "  _not: UserFilterInputObject\n"
            "}\n"
            "\n"
            "input IntFilterInputObject {\n"
            "  eq: Int\n"
            "  neq: Int\n"
            "  gt: Int\n"
            "  gte: Int\n"
            "  lt: Int\n"
            "  lte: Int\n"
            "  in: [Int]\n"
            "  notIn: [Int]\n"
            "}\n"
            "\n"
            "input StrFilterInputObject {\n"
            "  eq: String\n"
            "  neq: String\n"
            "  gt: String\n"
            "  gte: String\n"
            "  lt: String\n"
            "  lte: String\n"
            "  startsWith: String\n"
            "  endsWith: String\n"
            "  contains: String\n"
            "  in: [String]\n"
            "  notIn: [String]\n"
            "}\n"
            "\n"
            "input DateFilterInputObject {\n"
            "  eq: Date\n"
            "  neq: Date\n"
            "  gt: Date\n"
            "  gte: Date\n"
            "  lt: Date\n"
            "  lte: Date\n"
            "  in: [Date]\n"
            "  notIn: [Date]\n"
            "}"
        )

    def test_unfiltered(self, schema, executor, query_watcher):
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
        ["gql_filter", "expected", "query_part", "query_args"],
        [
            ('{name: {eq: "user1"}}', [dict(name="user1")], "WHERE users.name = ?", ("user1",)),
            ('{name: {neq: "user2"}}', [dict(name="user1")], "WHERE users.name != ?", ("user2",)),
            (
                '{registrationDate: {gt: "2000-01-01"}}',
                [dict(name="user2")],
                "WHERE users.registration_date > ?",
                ("2000-01-01",),
            ),
            (
                '{registrationDate: {gte: "2000-01-02"}}',
                [dict(name="user2")],
                "WHERE users.registration_date >= ?",
                ("2000-01-02",),
            ),
            (
                '{registrationDate: {lt: "2000-01-02"}}',
                [dict(name="user1")],
                "WHERE users.registration_date < ?",
                ("2000-01-02",),
            ),
            (
                '{registrationDate: {lte: "2000-01-01"}}',
                [dict(name="user1")],
                "WHERE users.registration_date <= ?",
                ("2000-01-01",),
            ),
            (
                '{name: {startsWith: "user"}}',
                [dict(name="user1"), dict(name="user2")],
                "WHERE (users.name LIKE ? || '%')",
                ("user",),
            ),
            (
                '{name: {endsWith: "er1"}}',
                [dict(name="user1")],
                "WHERE (users.name LIKE '%' || ?)",
                ("er1",),
            ),
            (
                '{name: {contains: "er"}}',
                [dict(name="user1"), dict(name="user2")],
                "WHERE (users.name LIKE '%' || ? || '%')",
                ("er",),
            ),
            (
                '{name: {in: ["user1", "user3"]}}',
                [dict(name="user1")],
                "WHERE users.name IN (?, ?)",
                ("user1", "user3"),
            ),
            (
                '{name: {notIn: ["user1", "user3"]}}',
                [dict(name="user2")],
                "WHERE (users.name NOT IN (?, ?))",
                ("user1", "user3"),
            ),
        ],
    )
    def test_filtered(
        self, gql_filter, expected, query_part, query_args, schema, executor, query_watcher
    ):
        result = executor(
            schema,
            """
            query {
                users (filter: [%s]) {
                    name
                }
            }
            """
            % gql_filter,
        )
        assert not result.errors
        assert result.data == {"users": expected}
        assert query_watcher.executed_queries_with_args == [
            (f"SELECT users.name FROM users {query_part}", query_args)
        ]

    def test_multiple_filters(self, schema, executor, query_watcher):
        result = executor(
            schema,
            """
            query {
                users (
                    filter: [
                        {name: {in: ["user1", "user2"]}},
                        {registrationDate: {gt: "2000-01-01"}}
                    ]
                ) {
                    name
                }
            }
            """,
        )
        assert not result.errors
        assert result.data == {"users": [dict(name="user2")]}
        assert query_watcher.executed_queries_with_args == [
            (
                (
                    "SELECT users.name FROM users"
                    " WHERE users.name IN (?, ?) AND users.registration_date > ?"
                ),
                ("user1", "user2", "2000-01-01"),
            )
        ]

    def test_and_filter(self, schema, executor, query_watcher):
        result = executor(
            schema,
            """
            query {
                users (
                    filter: [{
                        _and: [
                            {name: {in: ["user1", "user2"]}},
                            {registrationDate: {gt: "2000-01-01"}}
                        ]
                    }]
                ) {
                    name
                }
            }
            """,
        )
        assert not result.errors
        assert result.data == {"users": [dict(name="user2")]}
        assert query_watcher.executed_queries_with_args == [
            (
                (
                    "SELECT users.name FROM users"
                    " WHERE users.name IN (?, ?) AND users.registration_date > ?"
                ),
                ("user1", "user2", "2000-01-01"),
            )
        ]

    def test_or_filter(self, schema, executor, query_watcher):
        result = executor(
            schema,
            """
            query {
                users (
                    filter: [{
                        _or: [
                            {name: {in: ["user1", "user2"]}},
                            {registrationDate: {gt: "2000-01-01"}}
                        ]
                    }]
                ) {
                    name
                }
            }
            """,
        )
        assert not result.errors
        assert result.data == {"users": [dict(name="user1"), dict(name="user2")]}
        assert query_watcher.executed_queries_with_args == [
            (
                (
                    "SELECT users.name FROM users"
                    " WHERE users.name IN (?, ?) OR users.registration_date > ?"
                ),
                ("user1", "user2", "2000-01-01"),
            )
        ]

    def test_not_filter(self, schema, executor, query_watcher):
        result = executor(
            schema,
            """
            query {
                users (
                    filter: [{
                        _not: {name: {in: ["user1", "user2"]}}
                    }]
                ) {
                    name
                }
            }
            """,
        )
        assert not result.errors
        assert result.data == {"users": []}
        assert query_watcher.executed_queries_with_args == [
            (
                ("SELECT users.name FROM users" " WHERE (users.name NOT IN (?, ?))"),
                ("user1", "user2"),
            )
        ]

    def test_complex_composition(self, schema, executor, query_watcher):
        result = executor(
            schema,
            """
            query {
                users (
                    filter: [{
                        _or: [
                            {name: {in: ["user1", "user3"]}},
                            {_and: [
                                {registrationDate: {gte: "2000-01-01"}},
                                {registrationDate: {lte: "2000-01-03"}},
                                {_not: {registrationDate: {eq: "2000-01-02"}}}
                            ]}
                        ]
                    }]
                ) {
                    name
                }
            }
            """,
        )
        assert not result.errors
        assert result.data == {"users": [dict(name="user1")]}
        assert query_watcher.executed_queries_with_args == [
            (
                (
                    "SELECT users.name FROM users"
                    " WHERE users.name IN (?, ?) OR"
                    " users.registration_date >= ? AND users.registration_date <= ?"
                    " AND users.registration_date != ?"
                ),
                ("user1", "user3", "2000-01-01", "2000-01-03", "2000-01-02"),
            )
        ]
