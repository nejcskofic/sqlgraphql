from collections.abc import Callable, Iterator, Mapping
from contextlib import AbstractContextManager
from typing import Generic, TypeVar

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


class FrozenMap(Generic[K, V], Mapping[K, V]):
    __slots__ = ("_dict", "_hash")

    def __init__(self, data: Mapping[K, V]) -> None:
        self._dict: Mapping[K, V] = dict(data)
        self._hash: int | None = None

    def __getitem__(self, key: K) -> V:
        return self._dict[key]

    def __contains__(self, key: object) -> bool:
        return key in self._dict

    def __iter__(self) -> Iterator[K]:
        return iter(self._dict)

    def __len__(self) -> int:
        return len(self._dict)

    def __repr__(self) -> str:
        return f"FrozenMap({repr(self._dict)})"

    def __hash__(self) -> int:
        if self._hash is None:
            self._hash = hash(frozenset(self._dict.items()))
        return self._hash


T = TypeVar("T")


def assert_not_none(value: T | None) -> T:
    assert value is not None
    return value


def get_single_key_value(data: dict[str, T]) -> tuple[str, T]:
    if len(data) != 1:
        # TODO: Better error message (integrate oneOf directive?)
        raise ValueError("Expected single entry object")
    return next(iter(data.items()))
