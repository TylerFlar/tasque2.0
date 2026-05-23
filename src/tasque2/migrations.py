from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy import inspect
from sqlalchemy.engine import Engine

from alembic import command
from tasque2.config import get_settings
from tasque2.db import database_url_for_path, get_engine
from tasque2.models import Base


class MigrationError(RuntimeError):
    """Raised when the configured database cannot be migrated safely."""


@dataclass(frozen=True)
class MigrationStatus:
    database_path: Path
    current_revisions: tuple[str, ...]
    head_revisions: tuple[str, ...]

    @property
    def is_current(self) -> bool:
        return set(self.current_revisions) == set(self.head_revisions)

    @property
    def current_display(self) -> str:
        return ", ".join(self.current_revisions) if self.current_revisions else "<none>"

    @property
    def head_display(self) -> str:
        return ", ".join(self.head_revisions) if self.head_revisions else "<none>"


def alembic_config() -> Config:
    root = Path(__file__).resolve().parents[2]
    config = Config(str(root / "alembic.ini"))
    config.set_main_option("script_location", str(root / "alembic"))
    config.set_main_option("prepend_sys_path", str(root))
    config.set_main_option("sqlalchemy.url", database_url_for_path(get_settings().database_path))
    config.attributes["skip_logging_config"] = True
    return config


def upgrade_database(revision: str = "head") -> MigrationStatus:
    settings = get_settings()
    settings.database_path.parent.mkdir(parents=True, exist_ok=True)

    config = alembic_config()
    if not _adopt_current_unversioned_schema(config):
        command.upgrade(config, revision)
    return schema_status()


def schema_status() -> MigrationStatus:
    config = alembic_config()
    script = ScriptDirectory.from_config(config)
    head_revisions = tuple(script.get_heads())

    with get_engine().connect() as connection:
        context = MigrationContext.configure(connection)
        current_revisions = tuple(context.get_current_heads())

    return MigrationStatus(
        database_path=get_settings().database_path,
        current_revisions=current_revisions,
        head_revisions=head_revisions,
    )


def _adopt_current_unversioned_schema(config: Config) -> bool:
    engine = get_engine()
    inspector = inspect(engine)
    table_names = _user_table_names(engine)
    if not table_names or "alembic_version" in table_names:
        return False

    if not _metadata_schema_is_present(inspector):
        raise MigrationError(
            "Database has unversioned tables that do not match the current Tasque schema. "
            "Back it up and use a fresh database, or add an explicit migration before startup."
        )

    _ensure_manual_schema_objects(engine)
    command.stamp(config, "head")
    return True


def _user_table_names(engine: Engine) -> set[str]:
    return {
        name
        for name in inspect(engine).get_table_names()
        if not name.startswith("sqlite_") and not name.startswith("memory_fts_")
    }


def _metadata_schema_is_present(inspector) -> bool:
    table_names = set(inspector.get_table_names())
    for table in Base.metadata.sorted_tables:
        if table.name not in table_names:
            return False
        actual_columns = {column["name"] for column in inspector.get_columns(table.name)}
        expected_columns = {column.name for column in table.columns}
        if not expected_columns.issubset(actual_columns):
            return False
    return True


def _ensure_manual_schema_objects(engine: Engine) -> None:
    with engine.begin() as connection:
        connection.exec_driver_sql(
            """
            CREATE VIRTUAL TABLE IF NOT EXISTS memory_fts
            USING fts5(memory_id UNINDEXED, namespace, kind, content, tags)
            """
        )
