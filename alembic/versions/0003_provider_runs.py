"""add provider run metadata

Revision ID: 0003_provider_runs
Revises: 0002_schedules
Create Date: 2026-05-14 00:00:02
"""
from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0003_provider_runs"
down_revision = "0002_schedules"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "provider_runs",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("attempt_id", sa.String(length=36), nullable=False),
        sa.Column("provider", sa.String(length=80), nullable=False),
        sa.Column("model", sa.String(length=120), nullable=True),
        sa.Column("cwd", sa.Text(), nullable=True),
        sa.Column("argv_json", sa.JSON(), nullable=False),
        sa.Column("env_keys_json", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("provider_session_id", sa.String(length=160), nullable=True),
        sa.Column("raw_stream_artifact_id", sa.String(length=36), nullable=True),
        sa.Column("stdout_artifact_id", sa.String(length=36), nullable=True),
        sa.Column("stderr_artifact_id", sa.String(length=36), nullable=True),
        sa.Column("usage_json", sa.JSON(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["attempt_id"], ["work_attempts.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_provider_runs_attempt", "provider_runs", ["attempt_id"])
    op.create_index(
        "ix_provider_runs_provider_status",
        "provider_runs",
        ["provider", "status", "created_at"],
    )
    op.create_index(
        "ix_provider_runs_session",
        "provider_runs",
        ["provider", "provider_session_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_provider_runs_session", table_name="provider_runs")
    op.drop_index("ix_provider_runs_provider_status", table_name="provider_runs")
    op.drop_index("ix_provider_runs_attempt", table_name="provider_runs")
    op.drop_table("provider_runs")
