from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from alembic.autogenerate import compare_metadata
from alembic.runtime.migration import MigrationContext
from sqlalchemy import inspect, select
from typer.testing import CliRunner

from tasque2.cli import app
from tasque2.db import create_schema, get_engine, session_scope
from tasque2.migrations import MigrationError, schema_status, upgrade_database
from tasque2.models import Base, WorkItem

CORE_TABLES = {
    "agent_results",
    "artifacts",
    "discord_messages",
    "discord_threads",
    "failed_work",
    "memories",
    "memory_embeddings",
    "provider_runs",
    "schedule_occurrences",
    "schedules",
    "work_attempts",
    "work_dependencies",
    "work_events",
    "work_items",
    "workflow_definitions",
    "workflow_edges",
    "workflow_nodes",
    "workflow_runs",
}

WORK_ITEMS_WITHOUT_LANE = [
    "id VARCHAR(36) NOT NULL PRIMARY KEY",
    "title VARCHAR(240) NOT NULL",
    "task_instruction TEXT NOT NULL",
    "worker_kind VARCHAR(80) NOT NULL",
    "runtime_contract_json JSON NOT NULL",
    "context_json JSON NOT NULL",
    "status VARCHAR(32) NOT NULL",
    "priority INTEGER NOT NULL",
    "not_before DATETIME",
    "deadline_at DATETIME",
    "retry_policy_json JSON NOT NULL",
    "max_attempts INTEGER NOT NULL",
    "attempt_count INTEGER NOT NULL",
    "idempotency_key VARCHAR(240)",
    "source_kind VARCHAR(80)",
    "source_id VARCHAR(240)",
    "workflow_run_id VARCHAR(36)",
    "workflow_node_id VARCHAR(36)",
    "schedule_id VARCHAR(36)",
    "schedule_occurrence_id VARCHAR(36)",
    "discord_thread_id VARCHAR(80)",
    "visible BOOLEAN NOT NULL",
    "created_at DATETIME NOT NULL",
    "updated_at DATETIME NOT NULL",
    "CONSTRAINT uq_work_items_idempotency_key UNIQUE (idempotency_key)",
]


def _schema_differences() -> list[Any]:
    # Extension models imported in the same session share Base.metadata; the core migrations answer
    # for the core's own tables only.
    core_tables = {
        mapper.local_table.name for mapper in Base.registry.mappers if mapper.class_.__module__.startswith("tasque2.")
    }

    def in_core(obj: Any, name: str | None, type_: str, reflected: bool, compare_to: Any) -> bool:
        return type_ != "table" or reflected or name in core_tables

    with get_engine().connect() as connection:
        context = MigrationContext.configure(connection, opts={"include_object": in_core})
        differences = compare_metadata(context, Base.metadata)
    return [
        difference
        for difference in differences
        if not (difference[0] == "remove_table" and difference[1].name.startswith("memory_fts"))
    ]


def _unrecognized_database(*, columns: list[str], revision: str = "unknown_0042") -> None:
    """Every Tasque table, work_items with only ``columns`` and one row, and a revision this code does not know."""
    with get_engine().begin() as connection:
        connection.exec_driver_sql(f"CREATE TABLE work_items ({', '.join(columns)})")
        names = [column.split()[0] for column in columns if not column.startswith("CONSTRAINT")]
        values = {
            "id": "work-1",
            "title": "Kept through adoption",
            "task_instruction": "Survive.",
            "worker_kind": "manual",
            "runtime_contract_json": "{}",
            "context_json": "{}",
            "status": "succeeded",
            "priority": 0,
            "retry_policy_json": "{}",
            "max_attempts": 1,
            "attempt_count": 1,
            "visible": 1,
            "created_at": "2026-01-02 03:04:05",
            "updated_at": "2026-01-02 03:04:05",
        }
        present = [name for name in names if name in values]
        connection.exec_driver_sql(
            f"INSERT INTO work_items ({', '.join(present)}) VALUES ({', '.join('?' for _ in present)})",
            tuple(values[name] for name in present),
        )
        Base.metadata.create_all(connection)
        connection.exec_driver_sql("CREATE TABLE alembic_version (version_num VARCHAR(32) NOT NULL PRIMARY KEY)")
        connection.exec_driver_sql("INSERT INTO alembic_version (version_num) VALUES (?)", (revision,))


def test_fresh_database_upgrades_to_heads_and_matches_the_models() -> None:
    status = upgrade_database()

    assert status.is_current
    assert status.head_revisions == ("core_0001",)
    assert status.current_revisions == ("core_0001",)
    tables = set(inspect(get_engine()).get_table_names())
    assert CORE_TABLES | {"alembic_version", "memory_fts"} <= tables
    assert _schema_differences() == []


def test_upgrade_database_is_idempotent() -> None:
    first = upgrade_database()
    second = upgrade_database()

    assert first.is_current
    assert second.is_current
    assert first.head_revisions == second.head_revisions


def test_schema_status_reports_an_empty_database_as_behind() -> None:
    status = schema_status()

    assert status.current_revisions == ()
    assert status.current_display == "<none>"
    assert status.head_display == "core_0001"
    assert not status.is_current


def test_unversioned_current_schema_is_adopted() -> None:
    create_schema()
    assert "alembic_version" not in inspect(get_engine()).get_table_names()

    status = upgrade_database()

    tables = set(inspect(get_engine()).get_table_names())
    assert status.is_current
    assert {"alembic_version", "memory_fts"} <= tables
    assert _schema_differences() == []


def test_schema_missing_columns_under_an_unknown_revision_is_adopted() -> None:
    _unrecognized_database(columns=WORK_ITEMS_WITHOUT_LANE)

    status = upgrade_database()

    inspector = inspect(get_engine())
    columns = {column["name"] for column in inspector.get_columns("work_items")}
    indexes = {index["name"] for index in inspector.get_indexes("work_items")}
    assert status.is_current
    assert status.current_revisions == ("core_0001",)
    assert {"lane", "traceparent"} <= columns
    assert {
        "ix_work_items_lane",
        "ix_work_items_ready",
        "ix_work_items_source",
        "ix_work_items_workflow_node",
    } <= indexes
    assert "memory_fts" in inspector.get_table_names()
    assert _schema_differences() == []
    with session_scope() as session:
        work = session.get(WorkItem, "work-1")
        assert work is not None
        assert work.title == "Kept through adoption"
        assert work.lane is None


def test_a_revision_from_an_unloaded_extension_stops_the_upgrade_untouched() -> None:
    upgrade_database()
    with get_engine().begin() as connection:
        connection.exec_driver_sql("INSERT INTO alembic_version (version_num) VALUES ('gone_0007')")

    with pytest.raises(MigrationError, match=r"gone_0007.*TASQUE2_EXTENSIONS_DIR"):
        upgrade_database()

    assert set(schema_status().current_revisions) == {"core_0001", "gone_0007"}


def test_adoption_creates_missing_tables() -> None:
    _unrecognized_database(columns=WORK_ITEMS_WITHOUT_LANE)
    with get_engine().begin() as connection:
        connection.exec_driver_sql("DROP TABLE memory_embeddings")

    upgrade_database()

    assert "memory_embeddings" in inspect(get_engine()).get_table_names()
    assert _schema_differences() == []


def test_adoption_refuses_a_missing_not_null_column_without_default() -> None:
    columns = [column for column in WORK_ITEMS_WITHOUT_LANE if not column.startswith("visible ")]
    _unrecognized_database(columns=columns)

    with pytest.raises(MigrationError, match=r"work_items\.visible is missing"):
        upgrade_database()

    assert schema_status().current_revisions == ("unknown_0042",)


def test_database_without_work_items_is_not_adopted() -> None:
    with get_engine().begin() as connection:
        connection.exec_driver_sql("CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)")

    with pytest.raises(MigrationError, match="not a Tasque database"):
        upgrade_database()

    assert "alembic_version" not in inspect(get_engine()).get_table_names()


def test_extension_revisions_upgrade_with_the_core(isolated: Path) -> None:
    package = isolated / "extensions" / "migration_sample_ext"
    versions = package / "versions"
    versions.mkdir(parents=True)
    (package / "__init__.py").write_text(
        "from pathlib import Path\n\n\ndef register(registry):\n"
        "    registry.add_migration_location(Path(__file__).parent / 'versions')\n",
        encoding="utf-8",
    )
    (versions / "sample_0001_notes.py").write_text(
        'import sqlalchemy as sa\nfrom alembic import op\n\nrevision = "sample_0001"\ndown_revision = "core_0001"\n'
        "branch_labels = None\ndepends_on = None\n\n\ndef upgrade():\n"
        "    op.create_table('sample_notes', sa.Column('id', sa.Integer(), primary_key=True))\n\n\n"
        "def downgrade():\n    op.drop_table('sample_notes')\n",
        encoding="utf-8",
    )

    status = upgrade_database()

    assert status.head_revisions == ("sample_0001",)
    assert status.is_current
    assert {"work_items", "sample_notes"} <= set(inspect(get_engine()).get_table_names())


def test_cli_commands_migrate_an_empty_database_before_opening_a_session() -> None:
    result = CliRunner().invoke(app, ["queue", "Auto migrate", "Create the schema first."])

    assert result.exit_code == 0, result.output
    assert schema_status().is_current
    with session_scope() as session:
        assert session.scalar(select(WorkItem).where(WorkItem.title == "Auto migrate")) is not None


def test_read_only_cli_commands_migrate_an_empty_database() -> None:
    result = CliRunner().invoke(app, ["status"])

    assert result.exit_code == 0, result.output
    assert "ready" in result.output
    assert schema_status().is_current
