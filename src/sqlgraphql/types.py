from typing import Callable, Any

from graphql import GraphQLResolveInfo

SimpleResolver = Callable[[Any, GraphQLResolveInfo], object | None]
