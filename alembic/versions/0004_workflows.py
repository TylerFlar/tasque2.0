"""add workflow definitions and runs

Revision ID: 0004_workflows
Revises: 0003_provider_runs
Create Date: 2026-05-14 00:00:03
"""
from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0004_workflows"
down_revision = "0003_provider_runs"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "workflow_definitions",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("name", sa.String(length=240), nullable=False),
        sa.Column("version", sa.String(length=80), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("definition_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name", "version", name="uq_workflow_definition_version"),
    )
    op.create_index(
        "ix_workflow_definitions_enabled",
        "workflow_definitions",
        ["enabled", "name"],
    )

    op.create_table(
        "workflow_runs",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("workflow_definition_id", sa.String(length=36), nullable=False),
        sa.Column("name", sa.String(length=240), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("input_json", sa.JSON(), nullable=False),
        sa.Column("state_json", sa.JSON(), nullable=False),
        sa.Column("project_id", sa.String(length=36), nullable=True),
        sa.Column("discord_thread_id", sa.String(length=80), nullable=True),
        sa.Column("lease_owner", sa.String(length=120), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["workflow_definition_id"],
            ["workflow_definitions.id"],
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_workflow_runs_project", "workflow_runs", ["project_id", "created_at"])
    op.create_index("ix_workflow_runs_status", "workflow_runs", ["status", "created_at"])

    op.create_table(
        "workflow_nodes",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("workflow_run_id", sa.String(length=36), nullable=False),
        sa.Column("node_key", sa.String(length=120), nullable=False),
        sa.Column("kind", sa.String(length=40), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("definition_json", sa.JSON(), nullable=False),
        sa.Column("input_json", sa.JSON(), nullable=False),
        sa.Column("output_json", sa.JSON(), nullable=False),
        sa.Column("work_item_id", sa.String(length=36), nullable=True),
        sa.Column("parent_node_id", sa.String(length=36), nullable=True),
        sa.Column("fanout_index", sa.Integer(), nullable=True),
        sa.Column("failure_reason", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["work_item_id"], ["work_items.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["workflow_run_id"], ["workflow_runs.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("workflow_run_id", "node_key", name="uq_workflow_run_node_key"),
    )
    op.create_index("ix_workflow_nodes_run_status", "workflow_nodes", ["workflow_run_id", "status"])
    op.create_index("ix_workflow_nodes_work_item", "workflow_nodes", ["work_item_id"])

    op.create_table(
        "workflow_edges",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("workflow_run_id", sa.String(length=36), nullable=False),
        sa.Column("from_node_id", sa.String(length=36), nullable=False),
        sa.Column("to_node_id", sa.String(length=36), nullable=False),
        sa.Column("condition", sa.String(length=80), nullable=False),
        sa.ForeignKeyConstraint(["from_node_id"], ["workflow_nodes.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["to_node_id"], ["workflow_nodes.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["workflow_run_id"], ["workflow_runs.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_workflow_edges_from_node", "workflow_edges", ["from_node_id"])
    op.create_index("ix_workflow_edges_to_node", "workflow_edges", ["to_node_id"])


def downgrade() -> None:
    op.drop_index("ix_workflow_edges_to_node", table_name="workflow_edges")
    op.drop_index("ix_workflow_edges_from_node", table_name="workflow_edges")
    op.drop_table("workflow_edges")
    op.drop_index("ix_workflow_nodes_work_item", table_name="workflow_nodes")
    op.drop_index("ix_workflow_nodes_run_status", table_name="workflow_nodes")
    op.drop_table("workflow_nodes")
    op.drop_index("ix_workflow_runs_status", table_name="workflow_runs")
    op.drop_index("ix_workflow_runs_project", table_name="workflow_runs")
    op.drop_table("workflow_runs")
    op.drop_index("ix_workflow_definitions_enabled", table_name="workflow_definitions")
    op.drop_table("workflow_definitions")
