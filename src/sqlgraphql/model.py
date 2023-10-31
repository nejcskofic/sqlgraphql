from dataclasses import dataclass
from typing import NamedTuple, Sequence

from sqlalchemy import Select, Column
from sqlalchemy.sql.elements import KeyedColumnElement, ColumnElement
from sqlalchemy.sql.type_api import TypeEngine


class SelectElement(NamedTuple):
    # TODO: also expose primary key information for usage of ID as GQL type
    name: str
    type: TypeEngine  # At which point do we do transformation?
    required: bool


@dataclass(frozen=True)
class QueryableNode:
    name: str
    query: Select

    def get_select_elements(self) -> Sequence[SelectElement]:
        # nullability can be established only for Column
        # for others just assume they are nullable
        # TODO: cache?
        return [
            self._load_from_column_element(el)
            for el in self.query.selected_columns
        ]

    @classmethod
    def _load_from_column_element(cls, element: ColumnElement) -> SelectElement:
        if isinstance(element, Column) and element.nullable is not None:
            required = not element.nullable
        else:
            # we don't have information, assume weaker constraint
            required = False

        return SelectElement(
            name=element.name,
            type=element.type,
            required=required
        )
