"""drop work item timeout column

Revision ID: 0010_drop_work_item_timeout
Revises: 0009_agent_results
Create Date: 2026-05-14 00:00:10
"""
from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy import inspect

from alembic import op

revision = "0010_drop_work_item_timeout"
down_revision = "0009_agent_results"
branch_labels = None
depends_on = None


def upgrade() -> None:
    columns = {column["name"] for column in inspect(op.get_bind()).get_columns("work_items")}
    if "timeout_seconds" not in columns:
        return
    with op.batch_alter_table("work_items") as batch:
        batch.drop_column("timeout_seconds")


def downgrade() -> None:
    columns = {column["name"] for column in inspect(op.get_bind()).get_columns("work_items")}
    if "timeout_seconds" in columns:
        return
    with op.batch_alter_table("work_items") as batch:
        batch.add_column(sa.Column("timeout_seconds", sa.Integer(), nullable=True))
