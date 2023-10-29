from dataclasses import dataclass
from typing import Any

import graphene
import graphql
from sqlalchemy.orm import Session

from sqlgraphql.model import QueryableObjectType


@dataclass(frozen=True)
class ExecutionContext:
    session: Session


def QueryableList(object_type: type[QueryableObjectType]) -> graphene.Field:
    def resolver(root: Any, info: graphql.GraphQLResolveInfo) -> list[object]:
        context: ExecutionContext = info.context
        return list(context.session.execute(object_type._meta.base_query))

    return graphene.Field(graphene.List(object_type), resolver=resolver)
