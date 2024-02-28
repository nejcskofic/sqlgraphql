import pytest
from graphql import print_schema
from sqlalchemy import select

from sqlgraphql.model import QueryableNode
from sqlgraphql.schema import SchemaBuilder
from tests.integration.conftest import PostDB, UserDB


class Test1NSelection:
    @pytest.fixture()
    def schema(self):
        post_node = QueryableNode("Post", query=select(PostDB).order_by(PostDB.header))
        user_node = QueryableNode("User", query=select(UserDB), extra={"posts": post_node})
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
            "  registrationDate: Date!\n"
            "  posts: [Post!]\n"
            "}\n"
            "\n"
            '"""Date scalar type represents date in ISO format (YYYY-MM-DD)."""\n'
            "scalar Date\n"
            "\n"
            "type Post {\n"
            "  id: ID!\n"
            "  userId: Int!\n"
            "  header: String!\n"
            "  body: String!\n"
            "}"
        )

    def test_select_relation(self, schema, executor, query_watcher):
        result = executor(
            schema,
            """
            query {
                users {
                    name
                    posts {
                        header
                    }
                }
            }
            """,
        )
        assert not result.errors
        assert result.data == {
            "users": [
                dict(name="user1", posts=[dict(header=f"Post {i+1:03}") for i in range(75)]),
                dict(name="user2", posts=[dict(header=f"Post {i+1:03}") for i in range(75, 100)]),
            ]
        }
        assert query_watcher.executed_queries_with_args == [
            ("SELECT users.name, users.id AS __id FROM users", ()),
            ("SELECT posts.header FROM posts WHERE posts.user_id = ? ORDER BY posts.header", (1,)),
            ("SELECT posts.header FROM posts WHERE posts.user_id = ? ORDER BY posts.header", (2,)),
        ]
