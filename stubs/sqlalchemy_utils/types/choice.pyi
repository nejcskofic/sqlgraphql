import enum
from collections.abc import Sequence
from typing import Any

from sqlalchemy import Dialect, types
from sqlalchemy.sql.type_api import TypeEngine

class ChoiceType(types.TypeDecorator):
    impl: TypeEngine = ...
    choices: type[enum.Enum] | tuple[tuple[str, Any], ...]
    type_impl: ChoiceTypeImpl | EnumTypeImpl
    def __init__(
        self, choices: type[enum.Enum] | Sequence[tuple[str, Any]], impl: TypeEngine | None = ...
    ) -> None: ...
    @property
    def python_type(self) -> type: ...
    def process_bind_param(self, value: Any, dialect: Dialect) -> Any: ...
    def process_result_value(self, value: Any, dialect: Dialect) -> Any: ...

class ChoiceTypeImpl:
    choices_dict: dict[str, Any]

class EnumTypeImpl:
    enum_class: type[enum.Enum]
