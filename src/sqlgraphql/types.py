from collections.abc import Callable
from typing import Any

from graphql import GraphQLResolveInfo

SimpleResolver = Callable[[Any, GraphQLResolveInfo], object | None]
