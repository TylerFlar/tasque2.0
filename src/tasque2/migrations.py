"""Schema migrations: the core's Alembic revisions plus every extension's, upgraded together.

Extension revisions chain off a core revision, so one history upgrades to ``heads``. A
database that has Tasque tables but records no revision this code knows is adopted instead:
the tables, nullable columns and indexes the models define are added where missing, and the
database is stamped at the current heads. A database that records both known and unknown
revisions is left alone with an error, because the unknown ones usually belong to an
extension that is not loaded.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path

from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy import inspect
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.schema import CreateColumn

from alembic import command
from tasque2.config import get_settings
from tasque2.db import database_url_for_path, get_engine
from tasque2.extensions import registry as extension_registry
from tasque2.models import Base

logger = logging.getLogger(__name__)

CORE_TABLE = "work_items"
FTS_DDL = (
    "CREATE VIRTUAL TABLE IF NOT EXISTS memory_fts USING fts5(memory_id UNINDEXED, namespace, kind, content, tags)"
)


class MigrationError(RuntimeError):
    """The configured database cannot be migrated safely."""


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
    locations = [str(root / "alembic" / "versions")]
    locations.extend(str(location) for location in extension_registry().migration_locations)
    config.set_main_option("version_locations", os.pathsep.join(locations))
    config.attributes["skip_logging_config"] = True
    return config


def upgrade_database(revision: str = "heads") -> MigrationStatus:
    get_settings().database_path.parent.mkdir(parents=True, exist_ok=True)
    config = alembic_config()
    if not _adopt_unrecognized_schema(config):
        command.upgrade(config, revision)
    return schema_status()


def schema_status() -> MigrationStatus:
    config = alembic_config()
    head_revisions = tuple(ScriptDirectory.from_config(config).get_heads())
    with get_engine().connect() as connection:
        current_revisions = tuple(MigrationContext.configure(connection).get_current_heads())
    return MigrationStatus(
        database_path=get_settings().database_path,
        current_revisions=current_revisions,
        head_revisions=head_revisions,
    )


def _adopt_unrecognized_schema(config: Config) -> bool:
    engine = get_engine()
    tables = _user_table_names(engine)
    if not tables:
        return False
    with engine.connect() as connection:
        recorded = set(MigrationContext.configure(connection).get_current_heads())
    known = {script.revision for script in ScriptDirectory.from_config(config).walk_revisions()}
    if recorded and recorded <= known:
        return False
    if recorded & known:
        missing = ", ".join(sorted(recorded - known))
        raise MigrationError(
            f"The database records migrations this install does not have ({missing}). The extension that "
            "provides them is probably not loaded; check TASQUE2_EXTENSIONS_DIR."
        )
    if CORE_TABLE not in tables:
        raise MigrationError(f"{get_settings().database_path} has tables but is not a Tasque database.")
    with engine.begin() as connection:
        changes = reconcile_schema(connection)
        connection.exec_driver_sql(FTS_DDL)
    for change in changes:
        logger.info("Schema adoption: added %s", change)
    command.stamp(config, "heads", purge=True)
    return True


def reconcile_schema(connection: Connection) -> list[str]:
    """Add the tables, nullable columns and indexes the models define but the database lacks."""
    inspector = inspect(connection)
    existing = set(inspector.get_table_names())
    changes: list[str] = []
    missing_tables = [table for table in Base.metadata.sorted_tables if table.name not in existing]
    if missing_tables:
        Base.metadata.create_all(connection, tables=missing_tables)
        changes.extend(f"table {table.name}" for table in missing_tables)
    for table in Base.metadata.sorted_tables:
        if table.name not in existing:
            continue
        columns = {column["name"] for column in inspector.get_columns(table.name)}
        for column in table.columns:
            if column.name in columns:
                continue
            if not column.nullable and column.server_default is None:
                raise MigrationError(
                    f"{table.name}.{column.name} is missing and is NOT NULL without a server default; "
                    "add a migration for it."
                )
            ddl = CreateColumn(column).compile(dialect=connection.dialect)
            connection.exec_driver_sql(f'ALTER TABLE "{table.name}" ADD COLUMN {ddl}')
            changes.append(f"column {table.name}.{column.name}")
        indexes = {index["name"] for index in inspector.get_indexes(table.name)}
        for index in table.indexes:
            if index.name not in indexes:
                index.create(connection)
                changes.append(f"index {index.name}")
    return changes


def _user_table_names(engine: Engine) -> set[str]:
    return {
        name
        for name in inspect(engine).get_table_names()
        if not name.startswith("sqlite_") and not name.startswith("memory_fts") and name != "alembic_version"
    }
