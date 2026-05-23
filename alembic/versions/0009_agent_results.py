"""agent result inbox

Revision ID: 0009_agent_results
Revises: 0008_schedule_targets
Create Date: 2026-05-14
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0009_agent_results"
down_revision = "0008_schedule_targets"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "agent_results",
        sa.Column("result_token", sa.String(length=64), nullable=False),
        sa.Column("agent_kind", sa.String(length=32), nullable=False),
        sa.Column("payload_json", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("result_token"),
    )
    op.create_index(
        "ix_agent_results_kind_created",
        "agent_results",
        ["agent_kind", "created_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_agent_results_kind_created", table_name="agent_results")
    op.drop_table("agent_results")
