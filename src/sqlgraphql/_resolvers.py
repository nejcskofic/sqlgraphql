from collections.abc import Iterable
from typing import Any

from graphql import GraphQLResolveInfo
from sqlalchemy import Row, Select

from sqlgraphql._ast import AnalyzedNode, SortDirection, transform_query
from sqlgraphql.types import TypedResolveContext


class DbFieldResolver:
    __slots__ = ("_field_name",)

    def __init__(self, field_name: str):
        self._field_name = field_name

    def __call__(self, parent: Row, info: GraphQLResolveInfo) -> Any:
        return parent._mapping.get(self._field_name)


class _ListResolverBase:
    __slots__ = ("_node",)

    def __init__(self, node: AnalyzedNode):
        self._node = node

    def _transform(self, info: GraphQLResolveInfo, **kwargs: Any) -> Select:
        return transform_query(info, self._node)


class ListResolver(_ListResolverBase):
    def __call__(self, parent: object | None, info: GraphQLResolveInfo) -> Iterable:
        query = self._transform(info)
        context: TypedResolveContext = info.context
        return context["db_session"].execute(query)


class SortableListResolver(_ListResolverBase):
    def _transform(
        self,
        info: GraphQLResolveInfo,
        sort: list[dict[str, SortDirection]] | None = None,
        **kwargs: Any,
    ) -> Select:
        query = super()._transform(info, **kwargs)
        if sort is not None:
            for part in sort:
                if len(part) != 1:
                    # TODO: Better error message (integrate oneOf directive?)
                    raise ValueError("Expected single entry object")
                field_name, direction = next(iter(part.items()))
                field = self._node.fields[field_name]
                query = query.order_by(
                    field.orm_field.asc()
                    if direction == SortDirection.ASC
                    else field.orm_field.desc()
                )
        return query

    def __call__(
        self,
        parent: object | None,
        info: GraphQLResolveInfo,
        sort: list[dict[str, SortDirection]] | None = None,
    ) -> Iterable:
        query = self._transform(info, sort=sort)
        context: TypedResolveContext = info.context
        return context["db_session"].execute(query)
