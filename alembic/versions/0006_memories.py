"""add curated memories and fts

Revision ID: 0006_memories
Revises: 0005_projects
Create Date: 2026-05-14 00:00:05
"""
from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0006_memories"
down_revision = "0005_projects"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "memories",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("namespace", sa.String(length=160), nullable=False),
        sa.Column("kind", sa.String(length=80), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("tags_json", sa.JSON(), nullable=False),
        sa.Column("source_kind", sa.String(length=80), nullable=True),
        sa.Column("source_id", sa.String(length=240), nullable=True),
        sa.Column("project_id", sa.String(length=36), nullable=True),
        sa.Column("work_item_id", sa.String(length=36), nullable=True),
        sa.Column("canonical_key", sa.String(length=240), nullable=True),
        sa.Column("superseded_by", sa.String(length=36), nullable=True),
        sa.Column("archived_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("pinned", sa.Boolean(), nullable=False),
        sa.Column("ttl_days", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_memories_canonical_key", "memories", ["namespace", "canonical_key"])
    op.create_index("ix_memories_namespace", "memories", ["namespace", "created_at"])
    op.create_index("ix_memories_project", "memories", ["project_id", "created_at"])
    op.create_index("ix_memories_work_item", "memories", ["work_item_id", "created_at"])
    op.execute(
        """
        CREATE VIRTUAL TABLE IF NOT EXISTS memory_fts
        USING fts5(memory_id UNINDEXED, namespace, kind, content, tags)
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS memory_fts")
    op.drop_index("ix_memories_work_item", table_name="memories")
    op.drop_index("ix_memories_project", table_name="memories")
    op.drop_index("ix_memories_namespace", table_name="memories")
    op.drop_index("ix_memories_canonical_key", table_name="memories")
    op.drop_table("memories")
