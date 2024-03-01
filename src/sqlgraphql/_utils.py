from collections.abc import Callable
from contextlib import AbstractContextManager
from typing import TypeVar

K = TypeVar("K")
V = TypeVar("V")


class CacheDict(dict[K, V]):
    def __init__(self, default_factory: Callable[[K], V]):
        super().__init__()
        self._default_factory = default_factory

    def __missing__(self, key: K) -> V:
        ret = self[key] = self._default_factory(key)
        return ret


class CacheDictCM(dict[K, V]):
    def __init__(self, default_factory: Callable[[K], AbstractContextManager[V]]):
        super().__init__()
        self._default_factory = default_factory

    def __missing__(self, key: K) -> V:
        with self._default_factory(key) as value:
            self[key] = value
        return value


T = TypeVar("T")


def assert_not_none(value: T | None) -> T:
    assert value is not None
    return value


def get_single_key_value(data: dict[str, T]) -> tuple[str, T]:
    if len(data) != 1:
        # TODO: Better error message (integrate oneOf directive?)
        raise ValueError("Expected single entry object")
    return next(iter(data.items()))
