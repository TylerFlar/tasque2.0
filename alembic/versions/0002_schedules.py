"""add durable schedules

Revision ID: 0002_schedules
Revises: 0001_initial_core
Create Date: 2026-05-14 00:00:01
"""
from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0002_schedules"
down_revision = "0001_initial_core"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "schedules",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("name", sa.String(length=240), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("schedule_type", sa.String(length=32), nullable=False),
        sa.Column("expression", sa.String(length=240), nullable=False),
        sa.Column("timezone", sa.String(length=80), nullable=False),
        sa.Column("payload_json", sa.JSON(), nullable=False),
        sa.Column("worker_kind", sa.String(length=80), nullable=False),
        sa.Column("runtime_contract_json", sa.JSON(), nullable=False),
        sa.Column("catchup_policy", sa.String(length=32), nullable=False),
        sa.Column("misfire_grace_seconds", sa.Integer(), nullable=True),
        sa.Column("max_backfill", sa.Integer(), nullable=False),
        sa.Column("max_active_runs", sa.Integer(), nullable=False),
        sa.Column("jitter_seconds", sa.Integer(), nullable=False),
        sa.Column("last_evaluated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_schedules_enabled", "schedules", ["enabled", "schedule_type"])
    op.create_index("ix_schedules_last_evaluated", "schedules", ["last_evaluated_at"])

    op.create_table(
        "schedule_occurrences",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("schedule_id", sa.String(length=36), nullable=False),
        sa.Column("scheduled_for", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("work_item_id", sa.String(length=36), nullable=True),
        sa.Column("dedupe_key", sa.String(length=320), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["schedule_id"], ["schedules.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["work_item_id"], ["work_items.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("dedupe_key", name="uq_schedule_occurrence_dedupe_key"),
        sa.UniqueConstraint("schedule_id", "scheduled_for", name="uq_schedule_occurrence_time"),
    )
    op.create_index(
        "ix_schedule_occurrences_schedule",
        "schedule_occurrences",
        ["schedule_id", "scheduled_for"],
    )
    op.create_index(
        "ix_schedule_occurrences_status",
        "schedule_occurrences",
        ["status", "created_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_schedule_occurrences_status", table_name="schedule_occurrences")
    op.drop_index("ix_schedule_occurrences_schedule", table_name="schedule_occurrences")
    op.drop_table("schedule_occurrences")
    op.drop_index("ix_schedules_last_evaluated", table_name="schedules")
    op.drop_index("ix_schedules_enabled", table_name="schedules")
    op.drop_table("schedules")
