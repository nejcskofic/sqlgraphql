import math
from collections.abc import Callable, Iterable
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

from sqlgraphql._ast import AnalyzedNode
from sqlgraphql._gql import TypeMap
from sqlgraphql._resolvers import FieldResolver
from sqlgraphql._transformers import QueryBuilder, QueryExecutor
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
        transformer: QueryBuilder,
    ) -> GraphQLField:
        paged_accessor_object = self._cache[node]

        return GraphQLField(
            GraphQLNonNull(paged_accessor_object),
            {**args, "page": GraphQLArgument(GraphQLInt), "pageSize": GraphQLArgument(GraphQLInt)},
            resolve=PagedListResolver(transformer, DEFAULT_PAGE_SIZE),
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
    def __init__(self, query: QueryExecutor, page: int, page_size: int):
        self._query = query
        self._page = page
        self._page_size = page_size

    @cached_property
    def nodes(self) -> Iterable:
        return self._query.execute_with_pagination(self._page, self._page_size)

    @cached_property
    def page_info(self) -> OffsetPageInfo:
        return OffsetPageInfo(self._query.record_count, self._page, self._page_size)


class PagedListResolver:
    __slots__ = ("_transformer", "_default_page_size")

    def __init__(self, transformer: QueryBuilder, default_page_size: int):
        self._transformer = transformer
        self._default_page_size = default_page_size

    def __call__(
        self, parent: object | None, info: GraphQLResolveInfo, **kwargs: Any
    ) -> OffsetPagedResult:
        context: TypedResolveContext = info.context
        query = self._transformer.build(parent, info, kwargs, context["db_session"], ["nodes"])

        page = kwargs.get("page", 0)
        page_size = kwargs.get("pageSize", self._default_page_size)
        return OffsetPagedResult(query, page, page_size)
