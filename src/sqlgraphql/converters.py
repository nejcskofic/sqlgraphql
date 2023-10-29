import weakref
from collections.abc import MutableMapping
from datetime import date, datetime, time
from decimal import Decimal
from typing import Optional
from uuid import UUID

import graphene
from graphene import Scalar


class _TypeMappingRegistry:
    __slots__ = ("_cache", "_data")

    def __init__(self) -> None:
        self._data: dict[type, type[Scalar]] = {}
        self._cache: MutableMapping[type, Optional[type[Scalar]]] = weakref.WeakKeyDictionary()

    def register(self, python_type: type, graphene_type: type[Scalar]) -> None:
        self._data[python_type] = graphene_type
        self._cache.clear()

    def register_all(self, mappings: dict[type, type[Scalar]]) -> None:
        self._data.update(mappings)
        self._cache.clear()

    def get(self, python_type: type) -> Optional[type[Scalar]]:
        try:
            return self._cache[python_type]
        except KeyError:
            try:
                gql_type: Optional[type[Scalar]] = self._data[python_type]
            except KeyError:
                gql_type = self._find_type(python_type)
            self._cache[python_type] = gql_type
            return gql_type

    def _find_type(self, python_type: type) -> Optional[type[Scalar]]:
        # we already tried current type, we just need to visit bases
        for base_type in python_type.mro()[1:]:
            if base_type in self._data:
                return self._data[base_type]

        return None


TypeRegistry = _TypeMappingRegistry()
TypeRegistry.register_all(
    {
        bool: graphene.Boolean,
        date: graphene.Date,
        datetime: graphene.DateTime,
        float: graphene.Float,
        int: graphene.Int,
        bytes: graphene.Base64,  # type: ignore[attr-defined]
        Decimal: graphene.Decimal,
        str: graphene.String,
        time: graphene.Time,
        UUID: graphene.String,
        dict: graphene.JSONString,
    }
)
