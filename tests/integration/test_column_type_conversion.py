from datetime import date, datetime, time
from decimal import Decimal
from typing import Any, Optional
from uuid import UUID

import graphene
import pytest
from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
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
    insert,
    select,
)
from sqlalchemy.orm import Session
from sqlalchemy.sql.type_api import TypeEngine
from sqlalchemy_utils import UUIDType

from sqlgraphql.factories import ExecutionContext, QueryableList
from sqlgraphql.model import QueryableObjectType


class TestColumnTypeConversion:
    @pytest.fixture()
    def table_factory(self, database_engine):
        table: Optional[Table] = None
        metadata = MetaData()

        def factory(db_type: TypeEngine, value: Any) -> Table:
            nonlocal table

            table = Table("dummy", metadata, Column("member", db_type))
            table.create(bind=database_engine)

            with Session(bind=database_engine, future=True) as session:
                session.execute(insert(table).values(member=value))
                session.commit()

            return table

        yield factory

        if table is not None:
            table.drop(bind=database_engine)

    @pytest.mark.parametrize(
        ["column_type", "value", "gql_value"],
        [
            (BigInteger, 1, 1),
            (Boolean, True, True),
            (Date, date(2020, 1, 2), "2020-01-02"),
            (DateTime, datetime(2020, 1, 2, 3, 4, 5), "2020-01-02T03:04:05"),
            (Float, 1.1, 1.1),
            (Integer, 1, 1),
            (LargeBinary, b"1234", "MTIzNA=="),
            (Numeric, Decimal(1.2), "1.2000000000"),
            (SmallInteger, 1, 1),
            (String, "abc", "abc"),
            (Text, "abcd", "abcd"),
            (Time, time(1, 2, 3, 4), "01:02:03.000004"),
            (Unicode, "ęžâ", "ęžâ"),
            (UnicodeText, "žâü", "žâü"),
            # sqlalchemy utils types
            (UUIDType, UUID(int=1), "00000000-0000-0000-0000-000000000001"),
        ],
    )
    def test_type_conversion(
        self, column_type: TypeEngine, value: Any, gql_value: Any, table_factory, session_factory
    ):
        table = table_factory(column_type, value)

        class Dummy(QueryableObjectType):
            class Meta:
                base_query = select(table.c.member)

        class Query(graphene.ObjectType):
            dummy = QueryableList(Dummy)

        schema = graphene.Schema(query=Query)

        with session_factory.begin() as session:
            result = schema.execute(
                """
                query {
                    dummy {
                        member
                    }
                }
                """,
                context_value=ExecutionContext(session=session),
            )

        assert not result.errors
        assert result.data == dict(dummy=[dict(member=gql_value)])
