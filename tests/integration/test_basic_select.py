from dataclasses import dataclass
from typing import Any

import graphene
import graphql
from sqlalchemy.orm import Session

from tests.integration.conftest import UserDB


@dataclass(frozen=True)
class ExecutionContext:
    session: Session


class User(graphene.ObjectType):
    id = graphene.Field(graphene.ID, required=True)
    name = graphene.Field(graphene.String, required=True)
    registration_date = graphene.Field(graphene.Date, required=True)


class Query(graphene.ObjectType):
    users = graphene.Field(graphene.List(User))

    @staticmethod
    def resolve_users(root: Any, info: graphql.GraphQLResolveInfo):
        context: ExecutionContext = info.context
        return list(context.session.query(UserDB))


SCHEMA = graphene.Schema(query=Query)


class TestBasicSelect:
    def test_select_all(self, session_factory):
        with session_factory.begin() as session:
            result = SCHEMA.execute(
                """
                query {
                    users {
                        id
                        name
                        registrationDate
                    }
                }
                """,
                context_value=ExecutionContext(session=session),
            )
        assert not result.errors
        assert result.data == dict(
            users=[
                dict(id="1", name="user1", registrationDate="2000-01-01"),
                dict(id="2", name="user2", registrationDate="2000-01-02"),
            ]
        )
