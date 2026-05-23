from __future__ import annotations

from pathlib import Path

from sqlalchemy import inspect, select
from typer.testing import CliRunner

from tasque2.cli import app
from tasque2.db import create_schema, get_engine, reset_engine, session_scope
from tasque2.migrations import schema_status, upgrade_database
from tasque2.models import WorkItem


def test_upgrade_database_is_idempotent(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "tasque2.sqlite3"
    monkeypatch.setenv("TASQUE2_DB_PATH", str(db_path))
    reset_engine()

    first = upgrade_database()
    second = upgrade_database()

    assert first.is_current
    assert second.is_current
    assert first.head_revisions == second.head_revisions


def test_upgrade_database_adopts_current_unversioned_schema(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "unversioned.sqlite3"
    monkeypatch.setenv("TASQUE2_DB_PATH", str(db_path))
    reset_engine()
    create_schema()

    assert "alembic_version" not in inspect(get_engine()).get_table_names()

    status = upgrade_database()

    table_names = set(inspect(get_engine()).get_table_names())
    assert status.is_current
    assert "alembic_version" in table_names
    assert "memory_fts" in table_names


def test_init_db_and_db_status_cli_use_migrations(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "cli.sqlite3"
    monkeypatch.setenv("TASQUE2_DB_PATH", str(db_path))
    reset_engine()

    runner = CliRunner()
    init_result = runner.invoke(app, ["init-db"])
    status_result = runner.invoke(app, ["db-status"])

    assert init_result.exit_code == 0
    assert "Database schema is ready" in init_result.output
    assert status_result.exit_code == 0
    assert "is_current" in status_result.output
    assert "True" in status_result.output
    assert schema_status().is_current


def test_cli_session_commands_migrate_before_opening_sessions(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "auto-migrate.sqlite3"
    monkeypatch.setenv("TASQUE2_DB_PATH", str(db_path))
    reset_engine()

    result = CliRunner().invoke(app, ["queue", "Auto migrate", "Create the schema first."])

    assert result.exit_code == 0
    assert schema_status().is_current
    assert "alembic_version" in inspect(get_engine()).get_table_names()
    with session_scope() as session:
        work = session.scalar(select(WorkItem).where(WorkItem.title == "Auto migrate"))
        assert work is not None


def test_read_only_cli_session_commands_migrate_empty_databases(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "status.sqlite3"
    monkeypatch.setenv("TASQUE2_DB_PATH", str(db_path))
    reset_engine()

    result = CliRunner().invoke(app, ["status"])

    assert result.exit_code == 0
    assert "ready_work" in result.output
    assert schema_status().is_current
