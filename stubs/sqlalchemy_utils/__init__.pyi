from typing import Any

from sqlalchemy import types
from sqlalchemy.engine import Dialect
from sqlalchemy.sql.type_api import TypeEngine
from sqlalchemy_utils.types.choice import ChoiceType

__all__ = ["ChoiceType", "JSONType"]

class JSONType(types.TypeDecorator):
    impl: TypeEngine = ...
    def __init__(self, *args: Any, **kwargs: Any) -> None: ...
    def process_bind_param(self, value: Any, dialect: Dialect) -> Any: ...
    def process_result_value(self, value: Any, dialect: Dialect) -> Any: ...
