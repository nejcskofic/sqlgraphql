from __future__ import annotations

import datetime
import re
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid4

import pytest
from graphql import ExecutionResult, GraphQLSchema, graphql_sync
from sqlalchemy import Connection, ForeignKey, String, create_engine, event
from sqlalchemy.engine.interfaces import DBAPICursor, ExecutionContext
from sqlalchemy.future import Engine
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    Session,
    mapped_column,
    relationship,
    sessionmaker,
)

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

    post: Mapped[list[PostDB]] = relationship(back_populates="user")


class PostDB(Base):
    __tablename__ = "posts"

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    user_id: Mapped[int] = mapped_column(ForeignKey(UserDB.id))
    header: Mapped[str] = mapped_column(String(200))
    body: Mapped[str]

    user: Mapped[UserDB] = relationship(back_populates="post")


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
    with session_factory() as session:
        user1 = UserDB(name="user1", registration_date=datetime.date(2000, 1, 1))
        user2 = UserDB(name="user2", registration_date=datetime.date(2000, 1, 2))

        session.add_all([user1, user2])
        session.commit()

        bodies = [
            "Some interesting post",
            "Why everything is the best",
            "This new shiny thing solves all the problems",
        ]

        for i in range(75):
            session.add(
                PostDB(user_id=user1.id, header=f"Post {i + 1:03}", body=bodies[i % len(bodies)])
            )

        for i in range(75, 100):
            session.add(
                PostDB(user_id=user2.id, header=f"Post {i + 1:03}", body=bodies[i % len(bodies)])
            )
        session.commit()


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
