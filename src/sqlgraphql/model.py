from dataclasses import dataclass

from sqlalchemy import Select


@dataclass(frozen=True)
class QueryableNode:
    name: str
    query: Select
