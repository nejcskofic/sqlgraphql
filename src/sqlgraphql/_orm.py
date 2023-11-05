from collections.abc import Mapping

from sqlalchemy.sql.type_api import TypeEngine


class TypeRegistry:
    def __init__(self, explicit_mappings: Mapping[TypeEngine, type] | None = None):
        self._mapping = explicit_mappings or {}

    def get_python_type(self, type_: TypeEngine) -> type:
        python_type = self._mapping.get(type_)
        if python_type is not None:
            return python_type

        # try to get it directly from ORM type
        try:
            return type_.python_type
        except NotImplementedError:
            pass

        raise ValueError(f"ORM type {type_!r} does not have a known mapping to python type.")
