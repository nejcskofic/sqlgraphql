import pytest
from graphql import print_schema
from sqlalchemy import select

from sqlgraphql.model import QueryableNode
from sqlgraphql.schema import SchemaBuilder
from tests.integration.conftest import PostDB, UserDB


class TestRecursiveDefinitions:
    @pytest.fixture()
    def schema(self):
        user_node = QueryableNode("User", query=select(UserDB))
        post_node = QueryableNode(
            "Post",
            query=select(PostDB)
            .where(PostDB.header.in_(["Post 001", "Post 002", "Post 003", "Post 076"]))
            .order_by(PostDB.header),
            extra={"user": user_node},
        )
        user_node.define_field("posts", post_node)
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
            "  posts: [Post!]\n"
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
                        name
                        posts {
                            header
                        }
                    }
                }
            }
            """,
        )
        assert not result.errors
        assert result.data == {
            "posts": [
                {
                    "header": "Post 001",
                    "user": {
                        "name": "user1",
                        "posts": [
                            {"header": "Post 001"},
                            {"header": "Post 002"},
                            {"header": "Post 003"},
                        ],
                    },
                },
                {
                    "header": "Post 002",
                    "user": {
                        "name": "user1",
                        "posts": [
                            {"header": "Post 001"},
                            {"header": "Post 002"},
                            {"header": "Post 003"},
                        ],
                    },
                },
                {
                    "header": "Post 003",
                    "user": {
                        "name": "user1",
                        "posts": [
                            {"header": "Post 001"},
                            {"header": "Post 002"},
                            {"header": "Post 003"},
                        ],
                    },
                },
                {
                    "header": "Post 076",
                    "user": {"name": "user2", "posts": [{"header": "Post 076"}]},
                },
            ]
        }
        assert query_watcher.executed_queries_with_args == [
            (
                "SELECT posts.header, users.id IS NOT NULL AS __e1_, users.name AS "
                "__e1_name, users.id AS __e1___id FROM posts LEFT OUTER JOIN users ON "
                "posts.user_id = users.id WHERE posts.header IN (?, ?, ?, ?) ORDER BY "
                "posts.header",
                ("Post 001", "Post 002", "Post 003", "Post 076"),
            ),
            (
                "SELECT posts.header FROM posts WHERE posts.header IN (?, ?, ?, ?) AND "
                "posts.user_id = ? ORDER BY posts.header",
                ("Post 001", "Post 002", "Post 003", "Post 076", 1),
            ),
            (
                "SELECT posts.header FROM posts WHERE posts.header IN (?, ?, ?, ?) AND "
                "posts.user_id = ? ORDER BY posts.header",
                ("Post 001", "Post 002", "Post 003", "Post 076", 1),
            ),
            (
                "SELECT posts.header FROM posts WHERE posts.header IN (?, ?, ?, ?) AND "
                "posts.user_id = ? ORDER BY posts.header",
                ("Post 001", "Post 002", "Post 003", "Post 076", 1),
            ),
            (
                "SELECT posts.header FROM posts WHERE posts.header IN (?, ?, ?, ?) AND "
                "posts.user_id = ? ORDER BY posts.header",
                ("Post 001", "Post 002", "Post 003", "Post 076", 2),
            ),
        ]
