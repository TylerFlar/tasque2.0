"""add memory embeddings + importance for hybrid retrieval

Revision ID: 0012_memory_embeddings
Revises: 0011_remove_projects
Create Date: 2026-06-28 00:00:12
"""
from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy import inspect

from alembic import op

revision = "0012_memory_embeddings"
down_revision = "0011_remove_projects"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    columns = {column["name"] for column in inspect(bind).get_columns("memories")}
    if "importance" not in columns:
        op.add_column("memories", sa.Column("importance", sa.Integer(), nullable=True))

    if "memory_embeddings" not in inspect(bind).get_table_names():
        op.create_table(
            "memory_embeddings",
            sa.Column("memory_id", sa.String(length=36), nullable=False),
            sa.Column("namespace", sa.String(length=160), nullable=False),
            sa.Column("model", sa.String(length=120), nullable=False),
            sa.Column("dim", sa.Integer(), nullable=False),
            sa.Column("vector", sa.LargeBinary(), nullable=False),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
            sa.PrimaryKeyConstraint("memory_id"),
        )
        op.create_index("ix_memory_embeddings_namespace", "memory_embeddings", ["namespace"])


def downgrade() -> None:
    bind = op.get_bind()
    if "memory_embeddings" in inspect(bind).get_table_names():
        op.drop_index("ix_memory_embeddings_namespace", table_name="memory_embeddings")
        op.drop_table("memory_embeddings")
    columns = {column["name"] for column in inspect(bind).get_columns("memories")}
    if "importance" in columns:
        op.drop_column("memories", "importance")
