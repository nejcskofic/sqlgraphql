from contextlib import contextmanager
from contextvars import ContextVar
from typing import Callable, Dict, Iterator, Type

import graphene


class _Registry:
    __slots__ = ("_enums",)

    def __init__(self) -> None:
        self._enums: Dict[str, Type[graphene.Enum]] = {}

    def get_or_build_enum(
        self, name: str, factory: Callable[[str], Type[graphene.Enum]]
    ) -> Type[graphene.Enum]:
        enum_type = self._enums.get(name)
        if enum_type is None:
            enum_type = factory(name)
            assert issubclass(enum_type, graphene.Enum), "Expected subclass of graphene.Enum"
            self._enums[name] = enum_type
        return enum_type


_REGISTRY = ContextVar("_REGISTRY", default=_Registry())


@contextmanager
def scoped_registry() -> Iterator:
    registry = _Registry()
    token = _REGISTRY.set(registry)
    try:
        yield
    finally:
        _REGISTRY.reset(token)


def current() -> _Registry:
    return _REGISTRY.get()
