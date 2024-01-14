from collections.abc import Mapping
from typing import cast

from sqlalchemy import Alias, ForeignKeyConstraint, Select, Table
from sqlalchemy.sql.type_api import TypeEngine

from sqlgraphql.exceptions import InvalidOperationException
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


def get_implicit_relation(source_query: Select, remote_query: Select) -> ForeignKeyConstraint:
    remote_query_froms = remote_query.get_final_froms()
    if len(remote_query_froms) != 1:
        raise InvalidOperationException("Remote query does not contain exactly one from statement")

    remote_table = remote_query_froms[0]
    if not isinstance(remote_table, Table):
        raise InvalidOperationException(
            "Remote query does not select from the (non aliased) table."
        )

    candidates = []
    for from_statement in source_query.get_final_froms():
        if isinstance(from_statement, Table):
            source_table = from_statement
        elif isinstance(from_statement, Alias) and isinstance(from_statement.element, Table):
            source_table = from_statement.element
        else:
            continue

        for fk in source_table.foreign_key_constraints:
            if fk.referred_table is remote_table:
                candidates.append(fk)

    if len(candidates) != 1:
        raise InvalidOperationException(
            f"Expected single candidate relationship, found {len(candidates)}."
        )

    return candidates[0]
