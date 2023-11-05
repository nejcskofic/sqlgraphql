from collections.abc import Mapping
from typing import cast

from sqlalchemy.sql.type_api import TypeEngine

from sqlgraphql.types import AnyJsonValue

_DEFAULT_TYPE_MAP: dict[type[TypeEngine], type] = {}

try:
    import sqlalchemy_utils

    _DEFAULT_TYPE_MAP[sqlalchemy_utils.JSONType] = cast(type, AnyJsonValue)
except ImportError:
    pass


class TypeRegistry:
    def __init__(self, explicit_mappings: Mapping[type[TypeEngine], type] | None = None):
        mapping = dict(_DEFAULT_TYPE_MAP)
        if explicit_mappings is not None:
            mapping.update(explicit_mappings)
        self._mapping = mapping

    def get_python_type(self, type_: TypeEngine) -> type:
        python_type = self._mapping.get(type(type_))
        if python_type is not None:
            return python_type

        # try to get it directly from ORM type
        try:
            return type_.python_type
        except NotImplementedError:
            pass

        raise ValueError(f"ORM type {type_!r} does not have a known mapping to python type.")
