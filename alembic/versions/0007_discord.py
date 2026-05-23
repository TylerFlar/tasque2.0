"""add discord bindings and messages

Revision ID: 0007_discord
Revises: 0006_memories
Create Date: 2026-05-14 00:00:06
"""
from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0007_discord"
down_revision = "0006_memories"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "discord_threads",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("purpose", sa.String(length=80), nullable=False),
        sa.Column("discord_channel_id", sa.String(length=80), nullable=False),
        sa.Column("discord_thread_id", sa.String(length=80), nullable=False),
        sa.Column("work_item_id", sa.String(length=36), nullable=True),
        sa.Column("workflow_run_id", sa.String(length=36), nullable=True),
        sa.Column("project_id", sa.String(length=36), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("discord_thread_id", name="uq_discord_thread_id"),
        sa.UniqueConstraint("purpose", "project_id", name="uq_discord_thread_project"),
        sa.UniqueConstraint("purpose", "work_item_id", name="uq_discord_thread_work"),
        sa.UniqueConstraint("purpose", "workflow_run_id", name="uq_discord_thread_workflow"),
    )
    op.create_index("ix_discord_threads_status", "discord_threads", ["status", "created_at"])

    op.create_table(
        "discord_messages",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("discord_message_id", sa.String(length=80), nullable=False),
        sa.Column("discord_channel_id", sa.String(length=80), nullable=False),
        sa.Column("discord_thread_id", sa.String(length=80), nullable=True),
        sa.Column("direction", sa.String(length=20), nullable=False),
        sa.Column("author", sa.String(length=160), nullable=True),
        sa.Column("content_artifact_id", sa.String(length=36), nullable=True),
        sa.Column("content_preview", sa.Text(), nullable=False),
        sa.Column("work_item_id", sa.String(length=36), nullable=True),
        sa.Column("workflow_run_id", sa.String(length=36), nullable=True),
        sa.Column("project_id", sa.String(length=36), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("discord_message_id", name="uq_discord_message_id"),
    )
    op.create_index("ix_discord_messages_project", "discord_messages", ["project_id", "created_at"])
    op.create_index("ix_discord_messages_thread", "discord_messages", ["discord_thread_id", "created_at"])
    op.create_index("ix_discord_messages_work_item", "discord_messages", ["work_item_id", "created_at"])


def downgrade() -> None:
    op.drop_index("ix_discord_messages_work_item", table_name="discord_messages")
    op.drop_index("ix_discord_messages_thread", table_name="discord_messages")
    op.drop_index("ix_discord_messages_project", table_name="discord_messages")
    op.drop_table("discord_messages")
    op.drop_index("ix_discord_threads_status", table_name="discord_threads")
    op.drop_table("discord_threads")
