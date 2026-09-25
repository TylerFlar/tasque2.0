"""Sticky notes remember when their message was pinned, and when a failed pin is tried again.

Revision ID: core_0003
Revises: core_0002
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "core_0003"
down_revision = "core_0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("discord_stickies") as batch:
        batch.add_column(sa.Column("pinned_at", sa.DateTime(), nullable=True))
        batch.add_column(sa.Column("pin_retry_at", sa.DateTime(), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("discord_stickies") as batch:
        batch.drop_column("pin_retry_at")
        batch.drop_column("pinned_at")
