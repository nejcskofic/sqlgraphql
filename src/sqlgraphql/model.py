from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

from sqlalchemy import Select


@dataclass(frozen=True, eq=False)
class Link:
    node: QueryableNode


@dataclass(frozen=True, eq=False)
class QueryableNode:
    name: str
    query: Select
    extra: Mapping[str, Extra] = field(default_factory=dict)

    def define_field(self, name: str, data: Extra) -> None:
        extra = dict(self.extra)
        extra[name] = data
        object.__setattr__(self, "extra", extra)


LinkType = QueryableNode | Link
Extra = LinkType
