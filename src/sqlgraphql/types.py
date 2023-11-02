from collections.abc import Callable
from typing import Any, TypedDict

from graphql import GraphQLResolveInfo
from sqlalchemy.orm import Session

SimpleResolver = Callable[[Any, GraphQLResolveInfo], object | None]


class TypedResolveContext(TypedDict):
    db_session: Session
