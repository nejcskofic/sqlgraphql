import pytest
from graphql import GraphQLObjectType, GraphQLSchema
from sqlalchemy import select

from sqlgraphql.builders import build_list
from sqlgraphql.model import QueryableNode
from tests.integration.conftest import UserDB


class TestBasicSelect:
    @pytest.fixture()
    def schema(self):
        implicit_user_node = QueryableNode("User", query=select(UserDB))
        explicit_user_node = QueryableNode(
            "ExplicitUser", query=select(UserDB.id, UserDB.name, UserDB.registration_date)
        )
        query_type = GraphQLObjectType(
            "Query",
            {
                "implicitUsers": build_list(implicit_user_node),
                "explicitUsers": build_list(explicit_user_node),
            },
        )
        return GraphQLSchema(query_type)

    def test_select_all_names(self, schema, executor):
        result = executor(
            schema,
            """
            query {
                explicitUsers {
                    name
                }
            }
            """,
        )
        assert not result.errors
        assert result.data == {
            "explicitUsers": [
                dict(name="user1"),
                dict(name="user2"),
            ]
        }

    def test_casing_transformation(self, schema, executor):
        result = executor(
            schema,
            """
            query {
                explicitUsers {
                    registrationDate
                }
            }
            """,
        )
        assert not result.errors
        assert result.data == {
            "explicitUsers": [
                dict(registrationDate="2000-01-01"),
                dict(registrationDate="2000-01-02"),
            ]
        }
