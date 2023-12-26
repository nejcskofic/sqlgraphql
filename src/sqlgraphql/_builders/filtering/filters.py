from typing import Any

from sqlalchemy import ColumnExpressionArgument

from sqlgraphql._builders.filtering.base import FilterOp, ListFilterOp


class EqFilter(FilterOp, op="eq"):
    def __call__(self, value: Any) -> ColumnExpressionArgument[bool]:
        if value is None:
            return self.element.is_(None)
        else:
            return self.element.__eq__(value)


class NeqFilter(FilterOp, op="neq"):
    def __call__(self, value: Any) -> ColumnExpressionArgument[bool]:
        if value is None:
            return self.element.isnot(None)
        else:
            return self.element.__ne__(value)


class GtFilter(FilterOp, op="gt"):
    def __call__(self, value: Any) -> ColumnExpressionArgument[bool]:
        return self.element.__gt__(value)


class GteFilter(FilterOp, op="gte"):
    def __call__(self, value: Any) -> ColumnExpressionArgument[bool]:
        return self.element.__ge__(value)


class LtFilter(FilterOp, op="lt"):
    def __call__(self, value: Any) -> ColumnExpressionArgument[bool]:
        return self.element.__lt__(value)


class LteFilter(FilterOp, op="lte"):
    def __call__(self, value: Any) -> ColumnExpressionArgument[bool]:
        return self.element.__le__(value)


class StartsWithFilter(FilterOp, op="startsWith", for_types=(str,)):
    def __call__(self, value: Any) -> ColumnExpressionArgument[bool]:
        return self.element.startswith(value)


class EndsWithFilter(FilterOp, op="endsWith", for_types=(str,)):
    def __call__(self, value: Any) -> ColumnExpressionArgument[bool]:
        return self.element.endswith(value)


class ContainsFilter(FilterOp, op="contains", for_types=(str,)):
    def __call__(self, value: Any) -> ColumnExpressionArgument[bool]:
        return self.element.contains(value)


class InFilter(ListFilterOp, op="in"):
    def __call__(self, value: list) -> ColumnExpressionArgument[bool]:
        return self.element.in_(value)


class NotInFilter(ListFilterOp, op="notIn"):
    def __call__(self, value: list) -> ColumnExpressionArgument[bool]:
        return self.element.not_in(value)


BUILTIN_FILTERS = (
    EqFilter,
    NeqFilter,
    GtFilter,
    GteFilter,
    LtFilter,
    LteFilter,
    StartsWithFilter,
    EndsWithFilter,
    ContainsFilter,
    InFilter,
    NotInFilter,
)
