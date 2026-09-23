from __future__ import annotations

import sys
from logging.config import fileConfig
from pathlib import Path

from sqlalchemy import engine_from_config, pool

from alembic import context

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from tasque2.config import get_settings  # noqa: E402
from tasque2.db import database_url_for_path  # noqa: E402
from tasque2.extensions import registry as extension_registry  # noqa: E402
from tasque2.models import Base, UTCDateTime  # noqa: E402

# Extension models join Base.metadata when their packages register. tasque2.migrations
# adds their version directories; the raw alembic CLI needs them in alembic.ini.
extension_registry()

config = context.config

if config.config_file_name is not None and not config.attributes.get("skip_logging_config"):
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def get_url() -> str:
    return database_url_for_path(get_settings().database_path)


def render_item(type_, obj, autogen_context):
    """Render custom column types as the plain SQL types they store."""
    if type_ == "type" and isinstance(obj, UTCDateTime):
        return "sa.DateTime()"
    return False


def include_object(obj, name, type_, reflected, compare_to):
    """Leave the FTS5 virtual table and its shadow tables to the migrations that create them."""
    return not (type_ == "table" and name and name.startswith("memory_fts"))


def run_migrations_offline() -> None:
    context.configure(
        url=get_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        render_item=render_item,
        include_object=include_object,
        render_as_batch=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    configuration = config.get_section(config.config_ini_section, {})
    configuration["sqlalchemy.url"] = get_url()
    connectable = engine_from_config(configuration, prefix="sqlalchemy.", poolclass=pool.NullPool)
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            render_item=render_item,
            include_object=include_object,
            render_as_batch=True,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
