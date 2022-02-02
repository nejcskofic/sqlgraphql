import enum
from typing import Any, ClassVar, Sequence, Tuple, Type

from sqlalchemy import types
from sqlalchemy.engine import Dialect
from sqlalchemy.sql.type_api import TypeEngine

class ChoiceType(types.TypeDecorator):
    impl: TypeEngine = ...
    cache_ok: ClassVar[bool] = ...
    choices: Type[enum.Enum] | Tuple[Tuple[str, Any], ...]
    type_impl: object
    def __init__(
        self, choices: Type[enum.Enum] | Sequence[Tuple[str, Any]], impl: TypeEngine | None = ...
    ) -> None: ...
    @property
    def python_type(self) -> Type: ...
    def process_bind_param(self, value: Any, dialect: Dialect) -> Any: ...
    def process_result_value(self, value: Any, dialect: Dialect) -> Any: ...
