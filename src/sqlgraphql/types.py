from collections.abc import Callable
from typing import TYPE_CHECKING, Any, TypedDict

from graphql import GraphQLResolveInfo
from sqlalchemy.orm import Session

SimpleResolver = Callable[[Any, GraphQLResolveInfo], object | None]


class TypedResolveContext(TypedDict):
    db_session: Session


if TYPE_CHECKING:
    AnyJsonValue = dict | list | int | float | bool | None
else:

    class AnyJsonValue:
        def __call__(self, *args, **kwargs):
            raise TypeError("Cannot be instantiated, used only as marker")
