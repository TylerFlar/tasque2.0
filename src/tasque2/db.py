from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from sqlalchemy import Engine, create_engine, event
from sqlalchemy.orm import Session, sessionmaker

from tasque2.config import get_settings, reset_settings
from tasque2.models import Base

_engine: Engine | None = None
_session_factory: sessionmaker[Session] | None = None


def database_url_for_path(path: Path) -> str:
    return f"sqlite:///{path.expanduser().resolve().as_posix()}"


def _configure_sqlite(dbapi_connection, _connection_record) -> None:
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.execute("PRAGMA busy_timeout=5000")
    try:
        cursor.execute("PRAGMA journal_mode=WAL")
    finally:
        cursor.close()


def get_engine() -> Engine:
    global _engine
    if _engine is None:
        db_path = get_settings().database_path
        db_path.parent.mkdir(parents=True, exist_ok=True)
        _engine = create_engine(
            database_url_for_path(db_path),
            connect_args={"check_same_thread": False},
            future=True,
        )
        event.listen(_engine, "connect", _configure_sqlite)
    return _engine


def get_session_factory() -> sessionmaker[Session]:
    global _session_factory
    if _session_factory is None:
        _session_factory = sessionmaker(
            bind=get_engine(),
            autoflush=False,
            expire_on_commit=False,
            future=True,
        )
    return _session_factory


@contextmanager
def session_scope() -> Iterator[Session]:
    session = get_session_factory()()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def create_schema(engine: Engine | None = None) -> None:
    Base.metadata.create_all(bind=engine or get_engine())


def drop_schema(engine: Engine | None = None) -> None:
    Base.metadata.drop_all(bind=engine or get_engine())


def reset_engine() -> None:
    global _engine, _session_factory
    if _engine is not None:
        _engine.dispose()
    _engine = None
    _session_factory = None
    reset_settings()
