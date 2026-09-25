"""Discord sticky notes: the notes a worker keeps in a thread, shown above its upcoming runs.

Revision ID: core_0002
Revises: core_0001
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "core_0002"
down_revision = "core_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "discord_stickies",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("discord_thread_id", sa.String(length=80), nullable=False),
        sa.Column("notes", sa.Text(), nullable=False),
        sa.Column("notes_updated_at", sa.DateTime(), nullable=True),
        sa.Column("notes_work_item_id", sa.String(length=36), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("discord_message_id", sa.String(length=80), nullable=True),
        sa.Column("signature", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("discord_thread_id", name="uq_discord_sticky_thread"),
    )


def downgrade() -> None:
    op.drop_table("discord_stickies")
