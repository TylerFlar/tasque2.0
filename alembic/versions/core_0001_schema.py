"""The core schema: work queue, schedules, workflows, memory, artifacts, providers, Discord.

Revision ID: core_0001
Revises:
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "core_0001"
down_revision = None
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
    op.create_index("ix_agent_results_kind_created", "agent_results", ["agent_kind", "created_at"], unique=False)

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
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("discord_message_id", name="uq_discord_message_id"),
    )
    op.create_index("ix_discord_messages_thread", "discord_messages", ["discord_thread_id", "created_at"], unique=False)
    op.create_index("ix_discord_messages_work_item", "discord_messages", ["work_item_id", "created_at"], unique=False)

    op.create_table(
        "discord_threads",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("purpose", sa.String(length=80), nullable=False),
        sa.Column("discord_channel_id", sa.String(length=80), nullable=False),
        sa.Column("discord_thread_id", sa.String(length=80), nullable=False),
        sa.Column("work_item_id", sa.String(length=36), nullable=True),
        sa.Column("workflow_run_id", sa.String(length=36), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("discord_thread_id", name="uq_discord_thread_id"),
        sa.UniqueConstraint("purpose", "work_item_id", name="uq_discord_thread_work"),
        sa.UniqueConstraint("purpose", "workflow_run_id", name="uq_discord_thread_workflow"),
    )
    op.create_index("ix_discord_threads_status", "discord_threads", ["status", "created_at"], unique=False)

    op.create_table(
        "memories",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("namespace", sa.String(length=160), nullable=False),
        sa.Column("kind", sa.String(length=80), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("tags_json", sa.JSON(), nullable=False),
        sa.Column("source_kind", sa.String(length=80), nullable=True),
        sa.Column("source_id", sa.String(length=240), nullable=True),
        sa.Column("work_item_id", sa.String(length=36), nullable=True),
        sa.Column("canonical_key", sa.String(length=240), nullable=True),
        sa.Column("superseded_by", sa.String(length=36), nullable=True),
        sa.Column("archived_at", sa.DateTime(), nullable=True),
        sa.Column("pinned", sa.Boolean(), nullable=False),
        sa.Column("ttl_days", sa.Integer(), nullable=True),
        sa.Column("importance", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_memories_canonical_key", "memories", ["namespace", "canonical_key"], unique=False)
    op.create_index("ix_memories_namespace", "memories", ["namespace", "created_at"], unique=False)
    op.create_index("ix_memories_work_item", "memories", ["work_item_id", "created_at"], unique=False)

    op.create_table(
        "memory_embeddings",
        sa.Column("memory_id", sa.String(length=36), nullable=False),
        sa.Column("namespace", sa.String(length=160), nullable=False),
        sa.Column("model", sa.String(length=120), nullable=False),
        sa.Column("dim", sa.Integer(), nullable=False),
        sa.Column("vector", sa.LargeBinary(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("memory_id"),
    )
    op.create_index(op.f("ix_memory_embeddings_namespace"), "memory_embeddings", ["namespace"], unique=False)

    op.create_table(
        "schedules",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("name", sa.String(length=240), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("schedule_type", sa.String(length=32), nullable=False),
        sa.Column("expression", sa.String(length=240), nullable=False),
        sa.Column("timezone", sa.String(length=80), nullable=False),
        sa.Column("payload_json", sa.JSON(), nullable=False),
        sa.Column("worker_kind", sa.String(length=80), nullable=False),
        sa.Column("runtime_contract_json", sa.JSON(), nullable=False),
        sa.Column("catchup_policy", sa.String(length=32), nullable=False),
        sa.Column("misfire_grace_seconds", sa.Integer(), nullable=True),
        sa.Column("max_backfill", sa.Integer(), nullable=False),
        sa.Column("max_active_runs", sa.Integer(), nullable=False),
        sa.Column("jitter_seconds", sa.Integer(), nullable=False),
        sa.Column("last_evaluated_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_schedules_enabled", "schedules", ["enabled", "schedule_type"], unique=False)
    op.create_index("ix_schedules_last_evaluated", "schedules", ["last_evaluated_at"], unique=False)

    op.create_table(
        "work_items",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("title", sa.String(length=240), nullable=False),
        sa.Column("task_instruction", sa.Text(), nullable=False),
        sa.Column("worker_kind", sa.String(length=80), nullable=False),
        sa.Column("runtime_contract_json", sa.JSON(), nullable=False),
        sa.Column("context_json", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("priority", sa.Integer(), nullable=False),
        sa.Column("not_before", sa.DateTime(), nullable=True),
        sa.Column("deadline_at", sa.DateTime(), nullable=True),
        sa.Column("retry_policy_json", sa.JSON(), nullable=False),
        sa.Column("max_attempts", sa.Integer(), nullable=False),
        sa.Column("attempt_count", sa.Integer(), nullable=False),
        sa.Column("idempotency_key", sa.String(length=240), nullable=True),
        sa.Column("source_kind", sa.String(length=80), nullable=True),
        sa.Column("source_id", sa.String(length=240), nullable=True),
        sa.Column("workflow_run_id", sa.String(length=36), nullable=True),
        sa.Column("workflow_node_id", sa.String(length=36), nullable=True),
        sa.Column("schedule_id", sa.String(length=36), nullable=True),
        sa.Column("schedule_occurrence_id", sa.String(length=36), nullable=True),
        sa.Column("discord_thread_id", sa.String(length=80), nullable=True),
        sa.Column("visible", sa.Boolean(), nullable=False),
        sa.Column("lane", sa.String(length=120), nullable=True),
        sa.Column("traceparent", sa.String(length=64), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("idempotency_key", name="uq_work_items_idempotency_key"),
    )
    op.create_index("ix_work_items_lane", "work_items", ["lane", "created_at"], unique=False)
    op.create_index(
        "ix_work_items_ready", "work_items", ["status", "not_before", "priority", "created_at"], unique=False
    )
    op.create_index("ix_work_items_source", "work_items", ["source_kind", "source_id"], unique=False)
    op.create_index("ix_work_items_workflow_node", "work_items", ["workflow_run_id", "workflow_node_id"], unique=False)

    op.create_table(
        "workflow_definitions",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("name", sa.String(length=240), nullable=False),
        sa.Column("version", sa.String(length=80), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("definition_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name", "version", name="uq_workflow_definition_version"),
    )
    op.create_index("ix_workflow_definitions_enabled", "workflow_definitions", ["enabled", "name"], unique=False)

    op.create_table(
        "schedule_occurrences",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("schedule_id", sa.String(length=36), nullable=False),
        sa.Column("scheduled_for", sa.DateTime(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("work_item_id", sa.String(length=36), nullable=True),
        sa.Column("workflow_run_id", sa.String(length=36), nullable=True),
        sa.Column("dedupe_key", sa.String(length=320), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["schedule_id"], ["schedules.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["work_item_id"], ["work_items.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("dedupe_key", name="uq_schedule_occurrence_dedupe_key"),
        sa.UniqueConstraint("schedule_id", "scheduled_for", name="uq_schedule_occurrence_time"),
    )
    op.create_index(
        "ix_schedule_occurrences_schedule", "schedule_occurrences", ["schedule_id", "scheduled_for"], unique=False
    )
    op.create_index("ix_schedule_occurrences_status", "schedule_occurrences", ["status", "created_at"], unique=False)

    op.create_table(
        "work_attempts",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("work_item_id", sa.String(length=36), nullable=False),
        sa.Column("attempt_number", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("lease_owner", sa.String(length=120), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(), nullable=True),
        sa.Column("started_at", sa.DateTime(), nullable=True),
        sa.Column("heartbeat_at", sa.DateTime(), nullable=True),
        sa.Column("ended_at", sa.DateTime(), nullable=True),
        sa.Column("worker_kind", sa.String(length=80), nullable=False),
        sa.Column("provider", sa.String(length=80), nullable=True),
        sa.Column("provider_run_id", sa.String(length=120), nullable=True),
        sa.Column("summary", sa.Text(), nullable=True),
        sa.Column("report_artifact_id", sa.String(length=36), nullable=True),
        sa.Column("produces_json", sa.JSON(), nullable=False),
        sa.Column("error_type", sa.String(length=120), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("exit_code", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["work_item_id"], ["work_items.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("work_item_id", "attempt_number", name="uq_work_attempt_number"),
    )
    op.create_index("ix_work_attempts_provider_run", "work_attempts", ["provider", "provider_run_id"], unique=False)
    op.create_index("ix_work_attempts_status_lease", "work_attempts", ["status", "lease_expires_at"], unique=False)

    op.create_table(
        "work_dependencies",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("blocked_work_item_id", sa.String(length=36), nullable=False),
        sa.Column("dependency_work_item_id", sa.String(length=36), nullable=True),
        sa.Column("dependency_workflow_node_id", sa.String(length=36), nullable=True),
        sa.Column("condition", sa.String(length=80), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["blocked_work_item_id"], ["work_items.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["dependency_work_item_id"], ["work_items.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_work_dependencies_blocked", "work_dependencies", ["blocked_work_item_id"], unique=False)
    op.create_index("ix_work_dependencies_dependency", "work_dependencies", ["dependency_work_item_id"], unique=False)

    op.create_table(
        "workflow_runs",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("workflow_definition_id", sa.String(length=36), nullable=False),
        sa.Column("name", sa.String(length=240), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("input_json", sa.JSON(), nullable=False),
        sa.Column("state_json", sa.JSON(), nullable=False),
        sa.Column("discord_thread_id", sa.String(length=80), nullable=True),
        sa.Column("lease_owner", sa.String(length=120), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(), nullable=True),
        sa.Column("started_at", sa.DateTime(), nullable=True),
        sa.Column("ended_at", sa.DateTime(), nullable=True),
        sa.Column("traceparent", sa.String(length=64), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["workflow_definition_id"], ["workflow_definitions.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_workflow_runs_status", "workflow_runs", ["status", "created_at"], unique=False)

    op.create_table(
        "artifacts",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("kind", sa.String(length=80), nullable=False),
        sa.Column("title", sa.String(length=240), nullable=False),
        sa.Column("local_path", sa.Text(), nullable=False),
        sa.Column("content_type", sa.String(length=120), nullable=True),
        sa.Column("size_bytes", sa.Integer(), nullable=True),
        sa.Column("sha256", sa.String(length=64), nullable=True),
        sa.Column("summary", sa.Text(), nullable=True),
        sa.Column("tags_json", sa.JSON(), nullable=False),
        sa.Column("source_kind", sa.String(length=80), nullable=True),
        sa.Column("source_id", sa.String(length=240), nullable=True),
        sa.Column("workflow_run_id", sa.String(length=36), nullable=True),
        sa.Column("work_item_id", sa.String(length=36), nullable=True),
        sa.Column("attempt_id", sa.String(length=36), nullable=True),
        sa.Column("archived_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["attempt_id"], ["work_attempts.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["work_item_id"], ["work_items.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_artifacts_source", "artifacts", ["source_kind", "source_id"], unique=False)
    op.create_index("ix_artifacts_work_item", "artifacts", ["work_item_id", "created_at"], unique=False)

    op.create_table(
        "failed_work",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("work_item_id", sa.String(length=36), nullable=False),
        sa.Column("attempt_id", sa.String(length=36), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("error_type", sa.String(length=120), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("retry_count", sa.Integer(), nullable=False),
        sa.Column("resolved_at", sa.DateTime(), nullable=True),
        sa.Column("resolution_note", sa.Text(), nullable=True),
        sa.Column("discord_message_id", sa.String(length=80), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["attempt_id"], ["work_attempts.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["work_item_id"], ["work_items.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_failed_work_status", "failed_work", ["status", "created_at"], unique=False)
    op.create_index("ix_failed_work_work_item", "failed_work", ["work_item_id"], unique=False)

    op.create_table(
        "provider_runs",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("attempt_id", sa.String(length=36), nullable=False),
        sa.Column("provider", sa.String(length=80), nullable=False),
        sa.Column("model", sa.String(length=120), nullable=True),
        sa.Column("cwd", sa.Text(), nullable=True),
        sa.Column("argv_json", sa.JSON(), nullable=False),
        sa.Column("env_keys_json", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("provider_session_id", sa.String(length=160), nullable=True),
        sa.Column("stdout_artifact_id", sa.String(length=36), nullable=True),
        sa.Column("stderr_artifact_id", sa.String(length=36), nullable=True),
        sa.Column("usage_json", sa.JSON(), nullable=False),
        sa.Column("started_at", sa.DateTime(), nullable=True),
        sa.Column("ended_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["attempt_id"], ["work_attempts.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_provider_runs_attempt", "provider_runs", ["attempt_id"], unique=False)
    op.create_index(
        "ix_provider_runs_provider_status", "provider_runs", ["provider", "status", "created_at"], unique=False
    )
    op.create_index("ix_provider_runs_session", "provider_runs", ["provider", "provider_session_id"], unique=False)

    op.create_table(
        "work_events",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("event_type", sa.String(length=120), nullable=False),
        sa.Column("entity_kind", sa.String(length=80), nullable=False),
        sa.Column("entity_id", sa.String(length=120), nullable=False),
        sa.Column("work_item_id", sa.String(length=36), nullable=True),
        sa.Column("attempt_id", sa.String(length=36), nullable=True),
        sa.Column("workflow_run_id", sa.String(length=36), nullable=True),
        sa.Column("schedule_id", sa.String(length=36), nullable=True),
        sa.Column("source", sa.String(length=80), nullable=False),
        sa.Column("summary", sa.Text(), nullable=True),
        sa.Column("payload_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["attempt_id"], ["work_attempts.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["work_item_id"], ["work_items.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_work_events_entity", "work_events", ["entity_kind", "entity_id", "created_at"], unique=False)
    op.create_index("ix_work_events_type", "work_events", ["event_type", "created_at"], unique=False)
    op.create_index("ix_work_events_type_entity", "work_events", ["event_type", "entity_id"], unique=False)
    op.create_index("ix_work_events_work_item", "work_events", ["work_item_id", "created_at"], unique=False)
    op.create_index("ix_work_events_workflow", "work_events", ["workflow_run_id", "created_at"], unique=False)

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
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["work_item_id"], ["work_items.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["workflow_run_id"], ["workflow_runs.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("workflow_run_id", "node_key", name="uq_workflow_run_node_key"),
    )
    op.create_index("ix_workflow_nodes_run_status", "workflow_nodes", ["workflow_run_id", "status"], unique=False)
    op.create_index("ix_workflow_nodes_work_item", "workflow_nodes", ["work_item_id"], unique=False)

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
    op.create_index("ix_workflow_edges_from_node", "workflow_edges", ["from_node_id"], unique=False)
    op.create_index("ix_workflow_edges_to_node", "workflow_edges", ["to_node_id"], unique=False)
    op.execute(
        "CREATE VIRTUAL TABLE IF NOT EXISTS memory_fts USING fts5(memory_id UNINDEXED, namespace, kind, content, tags)"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS memory_fts")
    op.drop_table("workflow_edges")
    op.drop_table("workflow_nodes")
    op.drop_table("work_events")
    op.drop_table("provider_runs")
    op.drop_table("failed_work")
    op.drop_table("artifacts")
    op.drop_table("workflow_runs")
    op.drop_table("work_dependencies")
    op.drop_table("work_attempts")
    op.drop_table("schedule_occurrences")
    op.drop_table("workflow_definitions")
    op.drop_table("work_items")
    op.drop_table("schedules")
    op.drop_table("memory_embeddings")
    op.drop_table("memories")
    op.drop_table("discord_threads")
    op.drop_table("discord_messages")
    op.drop_table("agent_results")
