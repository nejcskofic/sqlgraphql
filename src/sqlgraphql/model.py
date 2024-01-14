from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

from sqlalchemy import Select

from sqlgraphql._utils import FrozenMap


@dataclass(frozen=True)
class Link:
    node: QueryableNode


@dataclass(frozen=True)
class QueryableNode:
    name: str
    query: Select
    extra: Mapping[str, Extra] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "extra", FrozenMap(self.extra))


LinkType = QueryableNode | Link
Extra = LinkType
