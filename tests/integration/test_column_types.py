import enum
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any
from uuid import UUID

import pytest
from graphql import print_type
from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    Enum,
    Float,
    Integer,
    LargeBinary,
    MetaData,
    Numeric,
    SmallInteger,
    String,
    Table,
    Text,
    Time,
    Unicode,
    UnicodeText,
    Uuid,
    insert,
    select,
)
from sqlalchemy.orm import Session
from sqlalchemy.sql.type_api import TypeEngine
from sqlalchemy_utils import UUIDType

from sqlgraphql.builders import SchemaBuilder
from sqlgraphql.model import QueryableNode


class DummyEnum(enum.Enum):
    ONE = 1
    TWO = 2
    THREE = 3


@dataclass
class TestCase:
    __test__ = False

    column_type: type[TypeEngine] | TypeEngine
    value: Any
    gql_type: str
    gql_value: Any


TEST_CASES = [
    TestCase(BigInteger, 1, "Int", 1),
    TestCase(Boolean, True, "Boolean", True),
    TestCase(Date, date(2020, 1, 2), "Date", "2020-01-02"),
    TestCase(DateTime, datetime(2020, 1, 2, 3, 4, 5), "DateTime", "2020-01-02T03:04:05"),
    # TestCase(Enum(DummyEnum), DummyEnum.TWO, "TWO"),
    TestCase(Enum("A", "B"), "A", "String", "A"),
    TestCase(Float, 1.1, "Float", 1.1),
    TestCase(Integer, 1, "Int", 1),
    TestCase(LargeBinary, b"1234", "Base64", "MTIzNA=="),
    TestCase(Numeric, Decimal(1.2), "Decimal", "1.2000000000"),
    TestCase(SmallInteger, 1, "Int", 1),
    TestCase(String, "abc", "String", "abc"),
    TestCase(Text, "abcd", "String", "abcd"),
    TestCase(Time, time(1, 2, 3, 4), "Time", "01:02:03.000004"),
    TestCase(Unicode, "ęžâ", "String", "ęžâ"),
    TestCase(UnicodeText, "žâü", "String", "žâü"),
    TestCase(Uuid, UUID(int=1), "ID", "00000000-0000-0000-0000-000000000001"),
    # sqlalchemy utils types
    TestCase(UUIDType, UUID(int=1), "ID", "00000000-0000-0000-0000-000000000001"),
    # TestCase(ChoiceType([("A", 1), ("B", 2)]), "A", "A"),
    # TestCase(ChoiceType(DummyEnum, impl=Integer()), DummyEnum.THREE, "THREE"),
    # TestCase(JSONType, {"m1": "a", "m2": 1}, '{"m1": "a", "m2": 1}'),
]


class TestSupportedColumnTypes:
    @pytest.fixture()
    def table_factory(self, database_engine):
        tables: list[Table] = []
        metadata = MetaData()

        def factory(db_type: TypeEngine, value: Any) -> Table:
            table = Table("dummy", metadata, Column("member", db_type))
            table.create(bind=database_engine)
            tables.append(table)

            with Session(bind=database_engine, future=True) as session:
                session.execute(insert(table).values(member=value))
                session.commit()

            return table

        yield factory

        for table in tables:
            table.drop(bind=database_engine)

    @pytest.mark.parametrize("test_case", TEST_CASES, ids=lambda x: repr(x.column_type))
    def test_column_types(self, test_case: TestCase, table_factory, executor):
        table = table_factory(test_case.column_type, test_case.value)

        node = QueryableNode("Dummy", query=select(table.c.member))

        schema = SchemaBuilder().add_root_list("dummy", node).build()

        result = executor(
            schema,
            """
            query {
                dummy {
                    member
                }
            }
            """,
        )

        assert not result.errors
        assert result.data == dict(dummy=[dict(member=test_case.gql_value)])
        assert (
            print_type(schema.type_map["Dummy"])
            == f"type Dummy {{\n  member: {test_case.gql_type}\n}}"
        )
