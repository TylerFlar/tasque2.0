"""add workflow and project schedule targets

Revision ID: 0008_schedule_targets
Revises: 0007_discord
Create Date: 2026-05-14 00:00:07
"""
from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0008_schedule_targets"
down_revision = "0007_discord"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("schedule_occurrences", sa.Column("workflow_run_id", sa.String(length=36), nullable=True))
    op.add_column("schedule_occurrences", sa.Column("project_id", sa.String(length=36), nullable=True))


def downgrade() -> None:
    op.drop_column("schedule_occurrences", "project_id")
    op.drop_column("schedule_occurrences", "workflow_run_id")
