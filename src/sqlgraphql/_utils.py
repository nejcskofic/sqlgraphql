from collections.abc import Callable
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
