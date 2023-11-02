from dataclasses import dataclass

from sqlalchemy import Select


@dataclass(frozen=True)
class QueryableNode:
    name: str
    query: Select

    def __post_init__(self) -> None:
        # Remove construction of ORM entity if query was specified as using one
        rewritten_query = self.query.with_only_columns(*self.query.selected_columns)
        object.__setattr__(self, "query", rewritten_query)
