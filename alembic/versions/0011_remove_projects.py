"""remove project subsystem

Revision ID: 0011_remove_projects
Revises: 0010_drop_work_item_timeout
Create Date: 2026-05-17 00:00:11
"""
from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy import inspect

from alembic import op

revision = "0011_remove_projects"
down_revision = "0010_drop_work_item_timeout"
branch_labels = None
depends_on = None


PROJECT_COLUMNS = {
    "work_items": ("project_id", ("ix_work_items_project_created",), ()),
    "work_events": ("project_id", ("ix_work_events_project",), ()),
    "artifacts": ("project_id", ("ix_artifacts_project",), ()),
    "schedule_occurrences": ("project_id", (), ()),
    "workflow_runs": ("project_id", ("ix_workflow_runs_project",), ()),
    "memories": ("project_id", ("ix_memories_project",), ()),
    "discord_threads": ("project_id", (), ("uq_discord_thread_project",)),
    "discord_messages": ("project_id", ("ix_discord_messages_project",), ()),
}


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)

    _drop_table_if_exists("project_updates")
    _drop_table_if_exists("projects")

    for table_name, (column_name, indexes, uniques) in PROJECT_COLUMNS.items():
        if table_name not in inspector.get_table_names():
            continue
        columns = _columns(table_name)
        if column_name not in columns:
            continue
        existing_indexes = _indexes(table_name)
        existing_uniques = _uniques(table_name)
        with op.batch_alter_table(table_name) as batch:
            for index_name in indexes:
                if index_name in existing_indexes:
                    batch.drop_index(index_name)
            for unique_name in uniques:
                if unique_name in existing_uniques:
                    batch.drop_constraint(unique_name, type_="unique")
            batch.drop_column(column_name)


def downgrade() -> None:
    _add_project_columns()
    _create_projects_tables()


def _add_project_columns() -> None:
    specs = {
        "work_items": ("ix_work_items_project_created", ["project_id", "created_at"]),
        "work_events": ("ix_work_events_project", ["project_id", "created_at"]),
        "artifacts": ("ix_artifacts_project", ["project_id", "created_at"]),
        "schedule_occurrences": (None, []),
        "workflow_runs": ("ix_workflow_runs_project", ["project_id", "created_at"]),
        "memories": ("ix_memories_project", ["project_id", "created_at"]),
        "discord_messages": ("ix_discord_messages_project", ["project_id", "created_at"]),
    }
    for table_name, (index_name, index_columns) in specs.items():
        if not _has_table(table_name):
            continue
        if "project_id" not in _columns(table_name):
            with op.batch_alter_table(table_name) as batch:
                batch.add_column(sa.Column("project_id", sa.String(length=36), nullable=True))
        if index_name and index_name not in _indexes(table_name):
            op.create_index(index_name, table_name, index_columns)

    if _has_table("discord_threads"):
        needs_column = "project_id" not in _columns("discord_threads")
        needs_unique = "uq_discord_thread_project" not in _uniques("discord_threads")
        if needs_column or needs_unique:
            with op.batch_alter_table("discord_threads") as batch:
                if needs_column:
                    batch.add_column(sa.Column("project_id", sa.String(length=36), nullable=True))
                if needs_unique:
                    batch.create_unique_constraint(
                        "uq_discord_thread_project",
                        ["purpose", "project_id"],
                    )


def _create_projects_tables() -> None:
    if not _has_table("projects"):
        op.create_table(
            "projects",
            sa.Column("id", sa.String(length=36), nullable=False),
            sa.Column("title", sa.String(length=240), nullable=False),
            sa.Column("goal", sa.Text(), nullable=False),
            sa.Column("completion_contract", sa.Text(), nullable=False),
            sa.Column("workspace_path", sa.Text(), nullable=True),
            sa.Column("status", sa.String(length=32), nullable=False),
            sa.Column("root_workflow_run_id", sa.String(length=36), nullable=True),
            sa.Column("discord_thread_id", sa.String(length=80), nullable=True),
            sa.Column("memory_namespace", sa.String(length=160), nullable=False),
            sa.Column("iteration_count", sa.Integer(), nullable=False),
            sa.Column("open_question", sa.Text(), nullable=True),
            sa.Column("completion_evidence_json", sa.JSON(), nullable=False),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
            sa.PrimaryKeyConstraint("id"),
        )
    if "ix_projects_memory_namespace" not in _indexes("projects"):
        op.create_index("ix_projects_memory_namespace", "projects", ["memory_namespace"])
    if "ix_projects_status" not in _indexes("projects"):
        op.create_index("ix_projects_status", "projects", ["status", "created_at"])

    if not _has_table("project_updates"):
        op.create_table(
            "project_updates",
            sa.Column("id", sa.String(length=36), nullable=False),
            sa.Column("project_id", sa.String(length=36), nullable=False),
            sa.Column("source", sa.String(length=80), nullable=False),
            sa.Column("content", sa.Text(), nullable=False),
            sa.Column("artifact_id", sa.String(length=36), nullable=True),
            sa.Column("discord_message_id", sa.String(length=80), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
            sa.ForeignKeyConstraint(["project_id"], ["projects.id"], ondelete="CASCADE"),
            sa.PrimaryKeyConstraint("id"),
        )
    if "ix_project_updates_project" not in _indexes("project_updates"):
        op.create_index("ix_project_updates_project", "project_updates", ["project_id", "created_at"])


def _drop_table_if_exists(table_name: str) -> None:
    if _has_table(table_name):
        op.drop_table(table_name)


def _has_table(table_name: str) -> bool:
    return table_name in inspect(op.get_bind()).get_table_names()


def _columns(table_name: str) -> set[str]:
    if not _has_table(table_name):
        return set()
    return {column["name"] for column in inspect(op.get_bind()).get_columns(table_name)}


def _indexes(table_name: str) -> set[str]:
    if not _has_table(table_name):
        return set()
    return {index["name"] for index in inspect(op.get_bind()).get_indexes(table_name)}


def _uniques(table_name: str) -> set[str]:
    if not _has_table(table_name):
        return set()
    return {
        unique["name"]
        for unique in inspect(op.get_bind()).get_unique_constraints(table_name)
        if unique.get("name")
    }
