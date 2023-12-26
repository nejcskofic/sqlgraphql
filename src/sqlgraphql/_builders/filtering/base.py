from abc import ABC, abstractmethod
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, ClassVar

from graphql import GraphQLEnumType, GraphQLInputField, GraphQLList, GraphQLScalarType
from sqlalchemy import ColumnElement, ColumnExpressionArgument


@dataclass(frozen=True, kw_only=True, slots=True)
class FilterOpMeta:
    op: str
    for_types: Collection[type] | None


class FilterOp(ABC):
    __slots__ = ("element",)

    meta: ClassVar[FilterOpMeta]
    element: ColumnElement

    def __init__(self, element: ColumnElement):
        object.__setattr__(self, "element", element)

    def __init_subclass__(
        cls, op: str | None = None, for_types: Collection[type] | None = None, **kwargs: Any
    ) -> None:
        super().__init_subclass__(**kwargs)
        if ABC in cls.__bases__:
            return

        if op is None:
            raise TypeError("Op must be defined")
        cls.meta = FilterOpMeta(op=op, for_types=for_types)

    def __setattr__(self, key: str, value: Any) -> None:
        raise AttributeError("Cannot modify instance")

    def __delattr__(self, item: str) -> None:
        raise AttributeError("Cannot modify instance")

    @classmethod
    def build_input_field(cls, gql_type: GraphQLScalarType | GraphQLEnumType) -> GraphQLInputField:
        return GraphQLInputField(gql_type)

    @abstractmethod
    def __call__(self, value: Any) -> ColumnExpressionArgument[bool]:
        raise NotImplementedError()


class ListFilterOp(FilterOp, ABC):
    @classmethod
    def build_input_field(cls, gql_type: GraphQLScalarType | GraphQLEnumType) -> GraphQLInputField:
        return GraphQLInputField(GraphQLList(gql_type))

    @abstractmethod
    def __call__(self, value: list) -> ColumnExpressionArgument[bool]:
        raise NotImplementedError()


class TypeFilterRegistry:
    def __init__(self, filters: Collection[type[FilterOp]]) -> None:
        self._registry: Mapping[str, type[FilterOp]] = {entry.meta.op: entry for entry in filters}

    def get_specs_for_type(self, python_type: type) -> Sequence[type[FilterOp]]:
        return [
            entry
            for entry in self._registry.values()
            if entry.meta.for_types is None or python_type in entry.meta.for_types
        ]
