import datetime

import pytest
from graphql import print_schema
from sqlalchemy import select

from sqlgraphql.model import QueryableNode
from sqlgraphql.schema import SchemaBuilder
from tests.integration.conftest import PostDB, UserDB


class TestN1Selection:
    @pytest.fixture()
    def schema(self):
        user_node = QueryableNode("User", query=select(UserDB))
        post_node = QueryableNode(
            "Post", query=select(PostDB).order_by(PostDB.header), extra={"user": user_node}
        )
        return SchemaBuilder().add_root_list("posts", post_node).build()

    def test_gql_schema_is_as_expected(self, schema):
        assert print_schema(schema) == (
            "type Query {\n"
            "  posts: [Post]\n"
            "}\n"
            "\n"
            "type Post {\n"
            "  id: ID!\n"
            "  userId: Int!\n"
            "  header: String!\n"
            "  body: String!\n"
            "  user: User!\n"
            "}\n"
            "\n"
            "type User {\n"
            "  id: Int!\n"
            "  name: String!\n"
            "  registrationDate: Date!\n"
            "}\n"
            "\n"
            '"""Date scalar type represents date in ISO format (YYYY-MM-DD)."""\n'
            "scalar Date"
        )

    def test_select_relation(self, schema, executor, query_watcher):
        result = executor(
            schema,
            """
            query {
                posts {
                    header
                    user {
                        id
                        name
                        registrationDate
                    }
                }
            }
            """,
        )
        assert not result.errors
        assert result.data == {
            "posts": [
                dict(
                    header=f"Post {i + 1:03}",
                    user=dict(id=1, name="user1", registrationDate="2000-01-01"),
                )
                for i in range(75)
            ]
            + [
                dict(
                    header=f"Post {i + 1:03}",
                    user=dict(id=2, name="user2", registrationDate="2000-01-02"),
                )
                for i in range(75, 100)
            ]
        }
        assert query_watcher.executed_queries == [
            "SELECT posts.header, users.id IS NOT NULL AS __e1_, users.id AS __e1_id, "
            'users.name AS __e1_name, users.registration_date AS "__e1_registrationDate" '
            "FROM posts LEFT OUTER JOIN users ON posts.user_id = users.id ORDER BY "
            "posts.header"
        ]


class TestN1SelectionWithNonTrivialRemote:
    @pytest.fixture()
    def schema(self):
        user_node = QueryableNode(
            "User",
            query=(select(UserDB).where(UserDB.registration_date > datetime.date(2000, 1, 1))),
        )
        post_node = QueryableNode(
            "Post", query=select(PostDB).order_by(PostDB.header), extra={"user": user_node}
        )
        return SchemaBuilder().add_root_list("posts", post_node).build()

    def test_gql_schema_is_as_expected(self, schema):
        assert print_schema(schema) == (
            "type Query {\n"
            "  posts: [Post]\n"
            "}\n"
            "\n"
            "type Post {\n"
            "  id: ID!\n"
            "  userId: Int!\n"
            "  header: String!\n"
            "  body: String!\n"
            "  user: User\n"
            "}\n"
            "\n"
            "type User {\n"
            "  id: Int!\n"
            "  name: String!\n"
            "  registrationDate: Date!\n"
            "}\n"
            "\n"
            '"""Date scalar type represents date in ISO format (YYYY-MM-DD)."""\n'
            "scalar Date"
        )

    def test_select_relation(self, schema, executor, query_watcher):
        result = executor(
            schema,
            """
            query {
                posts {
                    header
                    user {
                        id
                        name
                        registrationDate
                    }
                }
            }
            """,
        )
        assert not result.errors
        assert result.data == {
            "posts": [
                dict(
                    header=f"Post {i + 1:03}",
                    user=None,
                )
                for i in range(75)
            ]
            + [
                dict(
                    header=f"Post {i + 1:03}",
                    user=dict(id=2, name="user2", registrationDate="2000-01-02"),
                )
                for i in range(75, 100)
            ]
        }
        assert query_watcher.executed_queries_with_args == [
            (
                (
                    "SELECT posts.header, anon_1.__e1_, anon_1.id AS __e1_id, anon_1.name AS "
                    '__e1_name, anon_1.registration_date AS "__e1_registrationDate" FROM posts '
                    "LEFT OUTER JOIN (SELECT users.id AS id, users.name AS name, "
                    "users.registration_date AS registration_date, ? AS __e1_ FROM users WHERE "
                    "users.registration_date > ?) AS anon_1 ON posts.user_id = anon_1.id ORDER "
                    "BY posts.header"
                ),
                (1, "2000-01-01"),
            )
        ]
