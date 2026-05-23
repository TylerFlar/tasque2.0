"""initial core storage schema

Revision ID: 0001_initial_core
Revises:
Create Date: 2026-05-14 00:00:00
"""
from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0001_initial_core"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "work_items",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("title", sa.String(length=240), nullable=False),
        sa.Column("task_instruction", sa.Text(), nullable=False),
        sa.Column("worker_kind", sa.String(length=80), nullable=False),
        sa.Column("runtime_contract_json", sa.JSON(), nullable=False),
        sa.Column("context_json", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("priority", sa.Integer(), nullable=False),
        sa.Column("not_before", sa.DateTime(timezone=True), nullable=True),
        sa.Column("deadline_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("retry_policy_json", sa.JSON(), nullable=False),
        sa.Column("max_attempts", sa.Integer(), nullable=False),
        sa.Column("attempt_count", sa.Integer(), nullable=False),
        sa.Column("idempotency_key", sa.String(length=240), nullable=True),
        sa.Column("source_kind", sa.String(length=80), nullable=True),
        sa.Column("source_id", sa.String(length=240), nullable=True),
        sa.Column("workflow_run_id", sa.String(length=36), nullable=True),
        sa.Column("workflow_node_id", sa.String(length=36), nullable=True),
        sa.Column("project_id", sa.String(length=36), nullable=True),
        sa.Column("schedule_id", sa.String(length=36), nullable=True),
        sa.Column("schedule_occurrence_id", sa.String(length=36), nullable=True),
        sa.Column("discord_thread_id", sa.String(length=80), nullable=True),
        sa.Column("visible", sa.Boolean(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("idempotency_key", name="uq_work_items_idempotency_key"),
    )
    op.create_index("ix_work_items_project_created", "work_items", ["project_id", "created_at"])
    op.create_index("ix_work_items_ready", "work_items", ["status", "not_before", "priority", "created_at"])
    op.create_index("ix_work_items_source", "work_items", ["source_kind", "source_id"])
    op.create_index(
        "ix_work_items_workflow_node",
        "work_items",
        ["workflow_run_id", "workflow_node_id"],
    )

    op.create_table(
        "work_attempts",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("work_item_id", sa.String(length=36), nullable=False),
        sa.Column("attempt_number", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("lease_owner", sa.String(length=120), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("heartbeat_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("worker_kind", sa.String(length=80), nullable=False),
        sa.Column("provider", sa.String(length=80), nullable=True),
        sa.Column("provider_run_id", sa.String(length=120), nullable=True),
        sa.Column("summary", sa.Text(), nullable=True),
        sa.Column("report_artifact_id", sa.String(length=36), nullable=True),
        sa.Column("produces_json", sa.JSON(), nullable=False),
        sa.Column("error_type", sa.String(length=120), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("exit_code", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["work_item_id"], ["work_items.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("work_item_id", "attempt_number", name="uq_work_attempt_number"),
    )
    op.create_index(
        "ix_work_attempts_provider_run",
        "work_attempts",
        ["provider", "provider_run_id"],
    )
    op.create_index(
        "ix_work_attempts_status_lease",
        "work_attempts",
        ["status", "lease_expires_at"],
    )

    op.create_table(
        "work_dependencies",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("blocked_work_item_id", sa.String(length=36), nullable=False),
        sa.Column("dependency_work_item_id", sa.String(length=36), nullable=True),
        sa.Column("dependency_workflow_node_id", sa.String(length=36), nullable=True),
        sa.Column("condition", sa.String(length=80), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["blocked_work_item_id"], ["work_items.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["dependency_work_item_id"], ["work_items.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_work_dependencies_blocked",
        "work_dependencies",
        ["blocked_work_item_id"],
    )
    op.create_index(
        "ix_work_dependencies_dependency",
        "work_dependencies",
        ["dependency_work_item_id"],
    )

    op.create_table(
        "work_events",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("event_type", sa.String(length=120), nullable=False),
        sa.Column("entity_kind", sa.String(length=80), nullable=False),
        sa.Column("entity_id", sa.String(length=120), nullable=False),
        sa.Column("work_item_id", sa.String(length=36), nullable=True),
        sa.Column("attempt_id", sa.String(length=36), nullable=True),
        sa.Column("workflow_run_id", sa.String(length=36), nullable=True),
        sa.Column("project_id", sa.String(length=36), nullable=True),
        sa.Column("schedule_id", sa.String(length=36), nullable=True),
        sa.Column("source", sa.String(length=80), nullable=False),
        sa.Column("summary", sa.Text(), nullable=True),
        sa.Column("payload_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["attempt_id"], ["work_attempts.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["work_item_id"], ["work_items.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_work_events_entity", "work_events", ["entity_kind", "entity_id", "created_at"])
    op.create_index("ix_work_events_project", "work_events", ["project_id", "created_at"])
    op.create_index("ix_work_events_type", "work_events", ["event_type", "created_at"])
    op.create_index("ix_work_events_work_item", "work_events", ["work_item_id", "created_at"])
    op.create_index("ix_work_events_workflow", "work_events", ["workflow_run_id", "created_at"])

    op.create_table(
        "artifacts",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("kind", sa.String(length=80), nullable=False),
        sa.Column("title", sa.String(length=240), nullable=False),
        sa.Column("local_path", sa.Text(), nullable=False),
        sa.Column("content_type", sa.String(length=120), nullable=True),
        sa.Column("size_bytes", sa.Integer(), nullable=True),
        sa.Column("sha256", sa.String(length=64), nullable=True),
        sa.Column("summary", sa.Text(), nullable=True),
        sa.Column("tags_json", sa.JSON(), nullable=False),
        sa.Column("source_kind", sa.String(length=80), nullable=True),
        sa.Column("source_id", sa.String(length=240), nullable=True),
        sa.Column("project_id", sa.String(length=36), nullable=True),
        sa.Column("workflow_run_id", sa.String(length=36), nullable=True),
        sa.Column("work_item_id", sa.String(length=36), nullable=True),
        sa.Column("attempt_id", sa.String(length=36), nullable=True),
        sa.Column("archived_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["attempt_id"], ["work_attempts.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["work_item_id"], ["work_items.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_artifacts_project", "artifacts", ["project_id", "created_at"])
    op.create_index("ix_artifacts_source", "artifacts", ["source_kind", "source_id"])
    op.create_index("ix_artifacts_work_item", "artifacts", ["work_item_id", "created_at"])

    op.create_table(
        "failed_work",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("work_item_id", sa.String(length=36), nullable=False),
        sa.Column("attempt_id", sa.String(length=36), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("error_type", sa.String(length=120), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("retry_count", sa.Integer(), nullable=False),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("resolution_note", sa.Text(), nullable=True),
        sa.Column("discord_message_id", sa.String(length=80), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["attempt_id"], ["work_attempts.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["work_item_id"], ["work_items.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_failed_work_status", "failed_work", ["status", "created_at"])
    op.create_index("ix_failed_work_work_item", "failed_work", ["work_item_id"])


def downgrade() -> None:
    op.drop_index("ix_failed_work_work_item", table_name="failed_work")
    op.drop_index("ix_failed_work_status", table_name="failed_work")
    op.drop_table("failed_work")

    op.drop_index("ix_artifacts_work_item", table_name="artifacts")
    op.drop_index("ix_artifacts_source", table_name="artifacts")
    op.drop_index("ix_artifacts_project", table_name="artifacts")
    op.drop_table("artifacts")

    op.drop_index("ix_work_events_workflow", table_name="work_events")
    op.drop_index("ix_work_events_work_item", table_name="work_events")
    op.drop_index("ix_work_events_type", table_name="work_events")
    op.drop_index("ix_work_events_project", table_name="work_events")
    op.drop_index("ix_work_events_entity", table_name="work_events")
    op.drop_table("work_events")

    op.drop_index("ix_work_dependencies_dependency", table_name="work_dependencies")
    op.drop_index("ix_work_dependencies_blocked", table_name="work_dependencies")
    op.drop_table("work_dependencies")

    op.drop_index("ix_work_attempts_status_lease", table_name="work_attempts")
    op.drop_index("ix_work_attempts_provider_run", table_name="work_attempts")
    op.drop_table("work_attempts")

    op.drop_index("ix_work_items_workflow_node", table_name="work_items")
    op.drop_index("ix_work_items_source", table_name="work_items")
    op.drop_index("ix_work_items_ready", table_name="work_items")
    op.drop_index("ix_work_items_project_created", table_name="work_items")
    op.drop_table("work_items")
