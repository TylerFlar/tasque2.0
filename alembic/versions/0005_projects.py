"""add project state

Revision ID: 0005_projects
Revises: 0004_workflows
Create Date: 2026-05-14 00:00:04
"""
from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0005_projects"
down_revision = "0004_workflows"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "projects",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("title", sa.String(length=240), nullable=False),
        sa.Column("goal", sa.Text(), nullable=False),
        sa.Column("completion_contract", sa.Text(), nullable=False),
        sa.Column("workspace_path", sa.Text(), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("root_workflow_run_id", sa.String(length=36), nullable=True),
        sa.Column("discord_thread_id", sa.String(length=80), nullable=True),
        sa.Column("memory_namespace", sa.String(length=160), nullable=False),
        sa.Column("iteration_count", sa.Integer(), nullable=False),
        sa.Column("open_question", sa.Text(), nullable=True),
        sa.Column("completion_evidence_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_projects_memory_namespace", "projects", ["memory_namespace"])
    op.create_index("ix_projects_status", "projects", ["status", "created_at"])

    op.create_table(
        "project_updates",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("project_id", sa.String(length=36), nullable=False),
        sa.Column("source", sa.String(length=80), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("artifact_id", sa.String(length=36), nullable=True),
        sa.Column("discord_message_id", sa.String(length=80), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["project_id"], ["projects.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_project_updates_project", "project_updates", ["project_id", "created_at"])


def downgrade() -> None:
    op.drop_index("ix_project_updates_project", table_name="project_updates")
    op.drop_table("project_updates")
    op.drop_index("ix_projects_status", table_name="projects")
    op.drop_index("ix_projects_memory_namespace", table_name="projects")
    op.drop_table("projects")
