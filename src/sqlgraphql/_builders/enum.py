import enum
from collections.abc import Callable, Mapping
from typing import Any, NamedTuple

from graphql import GraphQLEnumType
from graphql.pyutils import snake_to_camel
from sqlalchemy import Enum
from sqlalchemy.sql.type_api import TypeEngine

from sqlgraphql._ast import AnalyzedField
from sqlgraphql._gql import TypeMap
from sqlgraphql._utils import CacheDict


class _EnumType(NamedTuple):
    value: type[enum.Enum]


class _ExplicitMappings(NamedTuple):
    value: Mapping[str, Any]


_AnalyzedEnum = _EnumType | _ExplicitMappings


class EnumBuilder:
    _TYPE_SUFFIX = "Enum"

    # TODO: Rework builder. Add ability to register custom handlers, support subclassing types
    def __init__(self, type_map: TypeMap) -> None:
        self._type_map = type_map
        self._type_handlers = self._build_default_handlers()
        self._cache = CacheDict[type[enum.Enum], GraphQLEnumType](self._construct_gql_enum)

    def build_from_field(self, field: AnalyzedField) -> GraphQLEnumType | None:
        handler = self._type_handlers.get(type(field.orm_type))
        if handler is None:
            return None

        analyzed_enum = handler(field.orm_type)
        match analyzed_enum:
            case _EnumType(enum_cls):
                gql_type = self._cache[enum_cls]
            case _ExplicitMappings(values):
                # We don't cache implicit enums
                gql_type = self._construct_gql_enum_using_dict(
                    snake_to_camel(field.orm_name), values
                )
            case _:
                assert False, "Should not happen"

        return gql_type

    def _construct_gql_enum(self, enum_cls: type[enum.Enum]) -> GraphQLEnumType:
        # Library will either use enum values or enum keys. It will not work with actual
        # enum object.
        # https://github.com/graphql-python/graphql-core/issues/73
        return self._type_map.add(
            GraphQLEnumType(
                self._type_map.get_unique_name(enum_cls.__name__, self._TYPE_SUFFIX),
                {key: value for key, value in enum_cls.__members__.items()},
            )
        )

    def _construct_gql_enum_using_dict(
        self, name: str, values: Mapping[str, Any]
    ) -> GraphQLEnumType:
        # We don't cache implicit enums
        return self._type_map.add(
            GraphQLEnumType(self._type_map.get_unique_name(name, self._TYPE_SUFFIX), values)
        )

    @classmethod
    def _build_default_handlers(cls) -> Mapping[type[TypeEngine], Callable[[Any], _AnalyzedEnum]]:
        def enum_handler(type_: Enum) -> _AnalyzedEnum:
            if type_.enum_class is not None:
                return _EnumType(type_.enum_class)
            else:
                return _ExplicitMappings({value: value for value in type_.enums})

        mapping: dict[type, Callable[[Any], _AnalyzedEnum]] = {Enum: enum_handler}

        try:
            import sqlalchemy_utils.types.choice as su

            def choice_handler(type_: su.ChoiceType) -> _AnalyzedEnum:
                if isinstance(type_.type_impl, su.EnumTypeImpl):
                    return _EnumType(type_.type_impl.enum_class)
                else:
                    # Choice works as attached data to lookup key, so we need to do identity map
                    return _ExplicitMappings({key: key for key in type_.type_impl.choices_dict})

            mapping[su.ChoiceType] = choice_handler
        except ImportError:
            pass

        return mapping
