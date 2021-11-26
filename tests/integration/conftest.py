import datetime
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import Column, Date, Integer, String, create_engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker

Base = declarative_base()

if TYPE_CHECKING:

    class SessionFactory(sessionmaker):
        def begin(self) -> Session:
            ...

        def __call__(self) -> Session:
            ...


else:
    SessionFactory = sessionmaker


class UserDB(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    name = Column(String(200), nullable=False)
    registration_date = Column(Date, nullable=False)


@pytest.fixture(scope="session", autouse=True)
def session_factory(tmp_path_factory) -> SessionFactory:
    db_path = tmp_path_factory.mktemp("db").joinpath("test.db")
    connection_string = f"sqlite:///{db_path}"
    engine = create_engine(connection_string)
    Base.metadata.create_all(bind=engine)
    return SessionFactory(bind=engine)


@pytest.fixture(scope="session", autouse=True)
def insert_data(session_factory):
    with session_factory.begin() as session:
        session.add_all(
            [
                UserDB(
                    name="user1", registration_date=datetime.date(2000, 1, 1)
                ),
                UserDB(
                    name="user2", registration_date=datetime.date(2000, 1, 2)
                ),
            ]
        )
