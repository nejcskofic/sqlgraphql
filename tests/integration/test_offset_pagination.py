import pytest
from graphql import print_schema
from sqlalchemy import select

from sqlgraphql.model import QueryableNode
from sqlgraphql.schema import SchemaBuilder
from tests.integration.conftest import PostDB


class TestOffsetPagination:
    @pytest.fixture()
    def schema(self):
        post_node = QueryableNode("Post", query=select(PostDB).order_by(PostDB.header))
        return SchemaBuilder().add_root_list("posts", post_node, pageable=True).build()

    def test_gql_schema_is_as_expected(self, schema):
        assert print_schema(schema) == (
            "type Query {\n"
            "  posts(page: Int, pageSize: Int): PostPagedObject!\n"
            "}\n"
            "\n"
            "type PostPagedObject {\n"
            "  nodes: [Post!]!\n"
            "  pageInfo: OffsetPageInfo!\n"
            "}\n"
            "\n"
            "type Post {\n"
            "  id: ID!\n"
            "  userId: Int!\n"
            "  header: String!\n"
            "  body: String!\n"
            "}\n"
            "\n"
            "type OffsetPageInfo {\n"
            "  page: Int!\n"
            "  pageSize: Int!\n"
            "  totalCount: Int!\n"
            "  totalPages: Int!\n"
            "}"
        )

    def test_default_paged(self, schema, executor, query_watcher):
        result = executor(
            schema,
            """
            query {
                posts {
                    nodes {
                        header
                    }
                    pageInfo {
                        page
                        pageSize
                        totalCount
                        totalPages
                    }
                }
            }
            """,
        )
        assert not result.errors
        assert result.data == {
            "posts": {
                "nodes": [{"header": f"Post {i:03}"} for i in range(1, 51)],
                "pageInfo": {"page": 0, "pageSize": 50, "totalCount": 100, "totalPages": 2},
            }
        }
        assert query_watcher.executed_queries_with_args == [
            ("SELECT posts.header FROM posts ORDER BY posts.header LIMIT ? OFFSET ?", (50, 0)),
            ("SELECT count(*) AS count_1 FROM posts", ()),
        ]

    def test_get_nodes_only(self, schema, executor, query_watcher):
        result = executor(
            schema,
            """
            query {
                posts {
                    nodes {
                        header
                    }
                }
            }
            """,
        )
        assert not result.errors
        assert result.data == {
            "posts": {"nodes": [{"header": f"Post {i:03}"} for i in range(1, 51)]}
        }
        assert query_watcher.executed_queries_with_args == [
            ("SELECT posts.header FROM posts ORDER BY posts.header LIMIT ? OFFSET ?", (50, 0))
        ]

    def test_page_info_only(self, schema, executor, query_watcher):
        result = executor(
            schema,
            """
            query {
                posts {
                    pageInfo {
                        page
                        pageSize
                        totalCount
                        totalPages
                    }
                }
            }
            """,
        )
        assert not result.errors
        assert result.data == {
            "posts": {"pageInfo": {"page": 0, "pageSize": 50, "totalCount": 100, "totalPages": 2}}
        }
        assert query_watcher.executed_queries_with_args == [
            ("SELECT count(*) AS count_1 FROM posts", ())
        ]

    def test_custom_page_index_and_size(self, schema, executor, query_watcher):
        result = executor(
            schema,
            """
            query {
                posts(page: 5, pageSize: 6) {
                    nodes {
                        header
                    }
                    pageInfo {
                        page
                        pageSize
                        totalCount
                        totalPages
                    }
                }
            }
            """,
        )
        assert not result.errors
        assert result.data == {
            "posts": {
                "nodes": [{"header": f"Post {i:03}"} for i in range(31, 37)],
                "pageInfo": {"page": 5, "pageSize": 6, "totalCount": 100, "totalPages": 17},
            }
        }
        assert query_watcher.executed_queries_with_args == [
            ("SELECT posts.header FROM posts ORDER BY posts.header LIMIT ? OFFSET ?", (6, 30)),
            ("SELECT count(*) AS count_1 FROM posts", ()),
        ]
