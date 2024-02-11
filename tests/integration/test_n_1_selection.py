import pytest
from graphql import print_schema
from sqlalchemy import select

from sqlgraphql.model import QueryableNode
from sqlgraphql.schema import SchemaBuilder
from tests.integration.conftest import PostDB, UserDB


@pytest.mark.skip()
class TestN1Selection:
    @pytest.fixture()
    def schema(self):
        user_node = QueryableNode("User", query=select(UserDB))
        post_node = QueryableNode(
            "Post", query=select(PostDB).order_by(PostDB.header), extra={"user": user_node}
        )
        return SchemaBuilder().add_root_list("posts", post_node).build()

    def test_gql_schema_is_as_expected(self, schema):
        assert print_schema(schema) == ""
