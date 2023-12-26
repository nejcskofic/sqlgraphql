import datetime
import re
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

import pytest
from graphql import ExecutionResult, GraphQLSchema, graphql_sync
from sqlalchemy import Connection, String, create_engine, event
from sqlalchemy.engine.interfaces import DBAPICursor, ExecutionContext
from sqlalchemy.future import Engine
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker

from sqlgraphql.types import TypedResolveContext

if TYPE_CHECKING:

    class SessionFactory(sessionmaker):
        def begin(self) -> Session:
            ...

        def __call__(self) -> Session:
            ...

else:
    SessionFactory = sessionmaker


class Base(DeclarativeBase):
    pass


class UserDB(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(200))
    registration_date: Mapped[datetime.date]


@pytest.fixture(scope="session")
def database_engine(tmp_path_factory) -> Engine:
    db_path = tmp_path_factory.mktemp("db").joinpath("test.db")
    connection_string = f"sqlite:///{db_path}"
    return create_engine(connection_string)


@pytest.fixture(scope="session", autouse=True)
def session_factory(database_engine) -> SessionFactory:
    Base.metadata.create_all(bind=database_engine)
    return SessionFactory(bind=database_engine, future=True)


@pytest.fixture(scope="session", autouse=True)
def insert_data(session_factory):
    with session_factory.begin() as session:
        session.add_all(
            [
                UserDB(name="user1", registration_date=datetime.date(2000, 1, 1)),
                UserDB(name="user2", registration_date=datetime.date(2000, 1, 2)),
            ]
        )


@pytest.fixture()
def executor(session_factory):
    def executor(
        schema: GraphQLSchema,
        query: str,
        variables: dict[str, Any] | None = None,
    ) -> ExecutionResult:
        with session_factory.begin() as session:
            return graphql_sync(
                schema,
                query,
                variable_values=variables,
                context_value=TypedResolveContext(db_session=session),
            )

    return executor


@pytest.fixture()
def query_watcher(database_engine):
    watcher = _QueryWatcher()
    event.listen(database_engine, "before_cursor_execute", watcher.on_before_cursor_execute)
    yield watcher
    event.remove(database_engine, "before_cursor_execute", watcher.on_before_cursor_execute)


class _QueryWatcher:
    def __init__(self):
        self._executed: list[tuple[str, Any]] = []

    @property
    def executed_queries(self) -> Sequence[str]:
        return [query for query, _ in self._executed]

    @property
    def executed_queries_with_args(self) -> Sequence[tuple[str, Any]]:
        return self._executed

    def on_before_cursor_execute(
        self,
        conn: Connection,
        cursor: DBAPICursor,
        statement: str,
        parameters: Any,
        context: ExecutionContext | None,
        executemany: bool,
    ):
        statement = re.sub(r"\s+", " ", statement)
        self._executed.append((statement, parameters))
