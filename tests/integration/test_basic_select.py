import graphene
import pytest
from sqlalchemy import select

from sqlgraphql.factories import ExecutionContext, QueryableList
from sqlgraphql.model import QueryableObjectType
from tests.integration.conftest import UserDB


class User(QueryableObjectType):
    class Meta:
        base_query = select(UserDB)


class ExplicitUser(QueryableObjectType):
    class Meta:
        base_query = select(UserDB.id, UserDB.name, UserDB.registration_date)


class Query(graphene.ObjectType):
    users = QueryableList(User)
    explicit_users = QueryableList(ExplicitUser)


SCHEMA = graphene.Schema(query=Query)


class TestBasicSelect:
    @pytest.mark.parametrize("node", ["users", "explicitUsers"])
    def test_select_all(self, node, session_factory):
        with session_factory.begin() as session:
            result = SCHEMA.execute(
                """
                query {
                    %s {
                        id
                        name
                        registrationDate
                    }
                }
                """
                % node,
                context_value=ExecutionContext(session=session),
            )
        assert not result.errors
        assert result.data == {
            node: [
                dict(id="1", name="user1", registrationDate="2000-01-01"),
                dict(id="2", name="user2", registrationDate="2000-01-02"),
            ]
        }
