"""The Workshop's ledger: one row per distinct issue its sweep saw, or idea a card offered.

Revision ID: core_0004
Revises: core_0003
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "core_0004"
down_revision = "core_0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "workshop_issues",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("key", sa.String(length=240), nullable=False),
        sa.Column("kind", sa.String(length=16), nullable=False),
        sa.Column("source", sa.String(length=40), nullable=False),
        sa.Column("title", sa.String(length=240), nullable=False),
        sa.Column("evidence_json", sa.JSON(), nullable=False),
        sa.Column("detail_json", sa.JSON(), nullable=True),
        sa.Column("first_seen_at", sa.DateTime(), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(), nullable=False),
        sa.Column("count", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("rest_until", sa.DateTime(), nullable=True),
        sa.Column("tried_at", sa.DateTime(), nullable=True),
        sa.Column("proposal_json", sa.JSON(), nullable=True),
        sa.Column("change_id", sa.String(length=40), nullable=True),
        sa.Column("workflow_run_id", sa.String(length=36), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("key", name="uq_workshop_issue_key"),
    )
    op.create_index("ix_workshop_issues_status", "workshop_issues", ["status", "last_seen_at"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_workshop_issues_status", table_name="workshop_issues")
    op.drop_table("workshop_issues")
