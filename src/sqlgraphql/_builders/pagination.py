import math
from collections.abc import Callable, Iterable, Sequence
from dataclasses import InitVar, dataclass, field
from functools import cached_property
from typing import Any

from graphql import (
    GraphQLArgument,
    GraphQLField,
    GraphQLInt,
    GraphQLList,
    GraphQLNonNull,
    GraphQLObjectType,
    GraphQLResolveInfo,
)
from sqlalchemy import Select, func
from sqlalchemy.orm import Session

from sqlgraphql._ast import AnalyzedNode
from sqlgraphql._builders.util import QueryTransformer
from sqlgraphql._gql import TypeMap
from sqlgraphql._resolvers import FieldResolver
from sqlgraphql._transformers import transform_query
from sqlgraphql._utils import CacheDict
from sqlgraphql.types import TypedResolveContext

DEFAULT_PAGE_SIZE = 50


class OffsetPagedArgumentBuilder:
    _TYPE_SUFFIX = "PagedObject"

    def __init__(self, type_map: TypeMap) -> None:
        self._type_map = type_map
        self._cache = CacheDict[AnalyzedNode, GraphQLObjectType](
            self._construct_offset_paged_accessor_object
        )
        self._offset_page_info_object = type_map.add(
            GraphQLObjectType(
                "OffsetPageInfo",
                {
                    "page": GraphQLField(GraphQLNonNull(GraphQLInt)),
                    "pageSize": GraphQLField(
                        GraphQLNonNull(GraphQLInt), resolve=FieldResolver("page_size")
                    ),
                    "totalCount": GraphQLField(
                        GraphQLNonNull(GraphQLInt), resolve=FieldResolver("total_count")
                    ),
                    "totalPages": GraphQLField(
                        GraphQLNonNull(GraphQLInt), resolve=FieldResolver("total_pages")
                    ),
                },
            )
        )

    def build_paged_list_field(
        self,
        node: AnalyzedNode,
        args: dict[str, GraphQLArgument],
        transformers: Sequence[QueryTransformer],
    ) -> GraphQLField:
        paged_accessor_object = self._cache[node]

        return GraphQLField(
            GraphQLNonNull(paged_accessor_object),
            {**args, "page": GraphQLArgument(GraphQLInt), "pageSize": GraphQLArgument(GraphQLInt)},
            resolve=PagedListResolver(node, transformers, DEFAULT_PAGE_SIZE),
        )

    def _construct_offset_paged_accessor_object(self, node: AnalyzedNode) -> GraphQLObjectType:
        gql_type = node.data.gql_type
        assert gql_type is not None

        return self._type_map.add(
            GraphQLObjectType(
                self._type_map.get_unique_name(node.node.name, self._TYPE_SUFFIX),
                {
                    "nodes": GraphQLField(GraphQLNonNull(GraphQLList(GraphQLNonNull(gql_type)))),
                    "pageInfo": GraphQLField(
                        GraphQLNonNull(self._offset_page_info_object),
                        resolve=FieldResolver("page_info"),
                    ),
                },
            )
        )


@dataclass(frozen=True)
class OffsetPageInfo:
    _get_total_count: Callable[[], int] = field(init=False)

    get_total_count: InitVar[Callable[[], int]]
    page: int
    page_size: int

    def __post_init__(self, get_total_count: Callable[[], int]) -> None:
        if self.page < 0:
            raise ValueError("Current page index should be greater than or equal to 0")
        if self.page_size <= 0:
            raise ValueError("Page size should be at least 1")
        object.__setattr__(self, "_get_total_count", get_total_count)

    @cached_property
    def total_count(self) -> int:
        return self._get_total_count()

    @cached_property
    def total_pages(self) -> int:
        return math.ceil(self.total_count / self.page_size)


class OffsetPagedResult:
    def __init__(self, query: Select, session: Session, page: int, page_size: int):
        self._query = query
        self._session = session
        self._page = page
        self._page_size = page_size

    @cached_property
    def nodes(self) -> Iterable:
        paged_query = self._query.limit(self._page_size).offset(self._page * self._page_size)
        return self._session.execute(paged_query)

    @cached_property
    def page_info(self) -> OffsetPageInfo:
        return OffsetPageInfo(self._get_record_count, self._page, self._page_size)

    def _get_record_count(self) -> int:
        page_info_query = self._query.with_only_columns(
            func.count(), maintain_column_froms=True
        ).order_by(None)
        # alternative
        # page_info_query = select(func.count()).select_from(self._query.order_by(None).subquery())
        return self._session.execute(page_info_query).scalar_one()


class PagedListResolver:
    __slots__ = ("_node", "_transformers", "_default_page_size")

    def __init__(
        self, node: AnalyzedNode, transformers: Sequence[QueryTransformer], default_page_size: int
    ):
        self._node = node
        self._transformers = transformers
        self._default_page_size = default_page_size

    def __call__(
        self, parent: object | None, info: GraphQLResolveInfo, **kwargs: Any
    ) -> OffsetPagedResult:
        query = transform_query(info, self._node, ["nodes"])
        for transformer in self._transformers:
            query = transformer(query, self._node, info, **kwargs)

        context: TypedResolveContext = info.context
        page = kwargs.get("page", 0)
        page_size = kwargs.get("pageSize", self._default_page_size)
        return OffsetPagedResult(query, context["db_session"], page, page_size)
