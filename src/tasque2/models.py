from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    TypeDecorator,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


def utc_now() -> datetime:
    return datetime.now(UTC)


def new_id() -> str:
    return str(uuid.uuid4())


class UTCDateTime(TypeDecorator[datetime]):
    """Store datetimes as naive UTC and return aware UTC values."""

    impl = DateTime
    cache_ok = True

    def process_bind_param(self, value: datetime | None, _dialect) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return value.astimezone(UTC).replace(tzinfo=None)

    def process_result_value(self, value: datetime | None, _dialect) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)


class Base(DeclarativeBase):
    pass


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(
        UTCDateTime(),
        default=utc_now,
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        UTCDateTime(),
        default=utc_now,
        onupdate=utc_now,
        nullable=False,
    )


class WorkItem(TimestampMixin, Base):
    __tablename__ = "work_items"
    __table_args__ = (
        Index("ix_work_items_ready", "status", "not_before", "priority", "created_at"),
        Index("ix_work_items_source", "source_kind", "source_id"),
        Index("ix_work_items_workflow_node", "workflow_run_id", "workflow_node_id"),
        UniqueConstraint("idempotency_key", name="uq_work_items_idempotency_key"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    title: Mapped[str] = mapped_column(String(240), nullable=False)
    task_instruction: Mapped[str] = mapped_column(Text, nullable=False)
    worker_kind: Mapped[str] = mapped_column(String(80), nullable=False)
    runtime_contract: Mapped[dict[str, Any]] = mapped_column(
        "runtime_contract_json",
        JSON,
        default=dict,
        nullable=False,
    )
    context: Mapped[dict[str, Any]] = mapped_column(
        "context_json",
        JSON,
        default=dict,
        nullable=False,
    )
    status: Mapped[str] = mapped_column(String(32), default="ready", nullable=False)
    priority: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    not_before: Mapped[datetime | None] = mapped_column(UTCDateTime())
    deadline_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    retry_policy: Mapped[dict[str, Any]] = mapped_column(
        "retry_policy_json",
        JSON,
        default=dict,
        nullable=False,
    )
    max_attempts: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    attempt_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    idempotency_key: Mapped[str | None] = mapped_column(String(240))
    source_kind: Mapped[str | None] = mapped_column(String(80))
    source_id: Mapped[str | None] = mapped_column(String(240))
    workflow_run_id: Mapped[str | None] = mapped_column(String(36))
    workflow_node_id: Mapped[str | None] = mapped_column(String(36))
    schedule_id: Mapped[str | None] = mapped_column(String(36))
    schedule_occurrence_id: Mapped[str | None] = mapped_column(String(36))
    discord_thread_id: Mapped[str | None] = mapped_column(String(80))
    visible: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    attempts: Mapped[list[WorkAttempt]] = relationship(
        back_populates="work_item",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    events: Mapped[list[WorkEvent]] = relationship(
        back_populates="work_item",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    artifacts: Mapped[list[Artifact]] = relationship(
        back_populates="work_item",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class WorkAttempt(TimestampMixin, Base):
    __tablename__ = "work_attempts"
    __table_args__ = (
        UniqueConstraint("work_item_id", "attempt_number", name="uq_work_attempt_number"),
        Index("ix_work_attempts_status_lease", "status", "lease_expires_at"),
        Index("ix_work_attempts_provider_run", "provider", "provider_run_id"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    work_item_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("work_items.id", ondelete="CASCADE"),
        nullable=False,
    )
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(32), default="running", nullable=False)
    lease_owner: Mapped[str | None] = mapped_column(String(120))
    lease_expires_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    started_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    heartbeat_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    ended_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    worker_kind: Mapped[str] = mapped_column(String(80), nullable=False)
    provider: Mapped[str | None] = mapped_column(String(80))
    provider_run_id: Mapped[str | None] = mapped_column(String(120))
    summary: Mapped[str | None] = mapped_column(Text)
    report_artifact_id: Mapped[str | None] = mapped_column(String(36))
    produces: Mapped[dict[str, Any]] = mapped_column(
        "produces_json",
        JSON,
        default=dict,
        nullable=False,
    )
    error_type: Mapped[str | None] = mapped_column(String(120))
    error_message: Mapped[str | None] = mapped_column(Text)
    exit_code: Mapped[int | None] = mapped_column(Integer)

    work_item: Mapped[WorkItem] = relationship(back_populates="attempts")
    artifacts: Mapped[list[Artifact]] = relationship(back_populates="attempt")
    failed_work: Mapped[list[FailedWork]] = relationship(back_populates="attempt")
    provider_runs: Mapped[list[ProviderRun]] = relationship(back_populates="attempt")


class WorkDependency(Base):
    __tablename__ = "work_dependencies"
    __table_args__ = (
        Index("ix_work_dependencies_blocked", "blocked_work_item_id"),
        Index("ix_work_dependencies_dependency", "dependency_work_item_id"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    blocked_work_item_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("work_items.id", ondelete="CASCADE"),
        nullable=False,
    )
    dependency_work_item_id: Mapped[str | None] = mapped_column(
        String(36),
        ForeignKey("work_items.id", ondelete="CASCADE"),
    )
    dependency_workflow_node_id: Mapped[str | None] = mapped_column(String(36))
    condition: Mapped[str] = mapped_column(String(80), default="succeeded", nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        UTCDateTime(),
        default=utc_now,
        nullable=False,
    )


class WorkEvent(Base):
    __tablename__ = "work_events"
    __table_args__ = (
        Index("ix_work_events_entity", "entity_kind", "entity_id", "created_at"),
        Index("ix_work_events_work_item", "work_item_id", "created_at"),
        Index("ix_work_events_workflow", "workflow_run_id", "created_at"),
        Index("ix_work_events_type", "event_type", "created_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_type: Mapped[str] = mapped_column(String(120), nullable=False)
    entity_kind: Mapped[str] = mapped_column(String(80), nullable=False)
    entity_id: Mapped[str] = mapped_column(String(120), nullable=False)
    work_item_id: Mapped[str | None] = mapped_column(
        String(36),
        ForeignKey("work_items.id", ondelete="CASCADE"),
    )
    attempt_id: Mapped[str | None] = mapped_column(
        String(36),
        ForeignKey("work_attempts.id", ondelete="SET NULL"),
    )
    workflow_run_id: Mapped[str | None] = mapped_column(String(36))
    schedule_id: Mapped[str | None] = mapped_column(String(36))
    source: Mapped[str] = mapped_column(String(80), default="tasque", nullable=False)
    summary: Mapped[str | None] = mapped_column(Text)
    payload: Mapped[dict[str, Any]] = mapped_column(
        "payload_json",
        JSON,
        default=dict,
        nullable=False,
    )
    created_at: Mapped[datetime] = mapped_column(
        UTCDateTime(),
        default=utc_now,
        nullable=False,
    )

    work_item: Mapped[WorkItem | None] = relationship(back_populates="events")


class Artifact(TimestampMixin, Base):
    __tablename__ = "artifacts"
    __table_args__ = (
        Index("ix_artifacts_work_item", "work_item_id", "created_at"),
        Index("ix_artifacts_source", "source_kind", "source_id"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    kind: Mapped[str] = mapped_column(String(80), nullable=False)
    title: Mapped[str] = mapped_column(String(240), nullable=False)
    local_path: Mapped[str] = mapped_column(Text, nullable=False)
    content_type: Mapped[str | None] = mapped_column(String(120))
    size_bytes: Mapped[int | None] = mapped_column(Integer)
    sha256: Mapped[str | None] = mapped_column(String(64))
    summary: Mapped[str | None] = mapped_column(Text)
    tags: Mapped[list[str]] = mapped_column("tags_json", JSON, default=list, nullable=False)
    source_kind: Mapped[str | None] = mapped_column(String(80))
    source_id: Mapped[str | None] = mapped_column(String(240))
    workflow_run_id: Mapped[str | None] = mapped_column(String(36))
    work_item_id: Mapped[str | None] = mapped_column(
        String(36),
        ForeignKey("work_items.id", ondelete="CASCADE"),
    )
    attempt_id: Mapped[str | None] = mapped_column(
        String(36),
        ForeignKey("work_attempts.id", ondelete="SET NULL"),
    )
    archived_at: Mapped[datetime | None] = mapped_column(UTCDateTime())

    work_item: Mapped[WorkItem | None] = relationship(back_populates="artifacts")
    attempt: Mapped[WorkAttempt | None] = relationship(back_populates="artifacts")


class FailedWork(TimestampMixin, Base):
    __tablename__ = "failed_work"
    __table_args__ = (
        Index("ix_failed_work_status", "status", "created_at"),
        Index("ix_failed_work_work_item", "work_item_id"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    work_item_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("work_items.id", ondelete="CASCADE"),
        nullable=False,
    )
    attempt_id: Mapped[str | None] = mapped_column(
        String(36),
        ForeignKey("work_attempts.id", ondelete="SET NULL"),
    )
    status: Mapped[str] = mapped_column(String(32), default="unresolved", nullable=False)
    error_type: Mapped[str | None] = mapped_column(String(120))
    error_message: Mapped[str | None] = mapped_column(Text)
    retry_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    resolved_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    resolution_note: Mapped[str | None] = mapped_column(Text)
    discord_message_id: Mapped[str | None] = mapped_column(String(80))

    work_item: Mapped[WorkItem] = relationship()
    attempt: Mapped[WorkAttempt | None] = relationship(back_populates="failed_work")


class Schedule(TimestampMixin, Base):
    __tablename__ = "schedules"
    __table_args__ = (
        Index("ix_schedules_enabled", "enabled", "schedule_type"),
        Index("ix_schedules_last_evaluated", "last_evaluated_at"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    name: Mapped[str] = mapped_column(String(240), nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    schedule_type: Mapped[str] = mapped_column(String(32), nullable=False)
    expression: Mapped[str] = mapped_column(String(240), nullable=False)
    timezone: Mapped[str] = mapped_column(String(80), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(
        "payload_json",
        JSON,
        default=dict,
        nullable=False,
    )
    worker_kind: Mapped[str] = mapped_column(String(80), nullable=False)
    runtime_contract: Mapped[dict[str, Any]] = mapped_column(
        "runtime_contract_json",
        JSON,
        default=dict,
        nullable=False,
    )
    catchup_policy: Mapped[str] = mapped_column(String(32), default="coalesce", nullable=False)
    misfire_grace_seconds: Mapped[int | None] = mapped_column(Integer)
    max_backfill: Mapped[int] = mapped_column(Integer, default=10, nullable=False)
    max_active_runs: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    jitter_seconds: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    last_evaluated_at: Mapped[datetime | None] = mapped_column(UTCDateTime())

    occurrences: Mapped[list[ScheduleOccurrence]] = relationship(
        back_populates="schedule",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class ScheduleOccurrence(TimestampMixin, Base):
    __tablename__ = "schedule_occurrences"
    __table_args__ = (
        UniqueConstraint("schedule_id", "scheduled_for", name="uq_schedule_occurrence_time"),
        UniqueConstraint("dedupe_key", name="uq_schedule_occurrence_dedupe_key"),
        Index("ix_schedule_occurrences_schedule", "schedule_id", "scheduled_for"),
        Index("ix_schedule_occurrences_status", "status", "created_at"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    schedule_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("schedules.id", ondelete="CASCADE"),
        nullable=False,
    )
    scheduled_for: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)
    status: Mapped[str] = mapped_column(String(32), default="pending", nullable=False)
    work_item_id: Mapped[str | None] = mapped_column(
        String(36),
        ForeignKey("work_items.id", ondelete="SET NULL"),
    )
    workflow_run_id: Mapped[str | None] = mapped_column(String(36))
    dedupe_key: Mapped[str] = mapped_column(String(320), nullable=False)

    schedule: Mapped[Schedule] = relationship(back_populates="occurrences")
    work_item: Mapped[WorkItem | None] = relationship()


class ProviderRun(Base):
    __tablename__ = "provider_runs"
    __table_args__ = (
        Index("ix_provider_runs_attempt", "attempt_id"),
        Index("ix_provider_runs_provider_status", "provider", "status", "created_at"),
        Index("ix_provider_runs_session", "provider", "provider_session_id"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    attempt_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("work_attempts.id", ondelete="CASCADE"),
        nullable=False,
    )
    provider: Mapped[str] = mapped_column(String(80), nullable=False)
    model: Mapped[str | None] = mapped_column(String(120))
    cwd: Mapped[str | None] = mapped_column(Text)
    argv: Mapped[list[str]] = mapped_column("argv_json", JSON, default=list, nullable=False)
    env_keys: Mapped[list[str]] = mapped_column("env_keys_json", JSON, default=list, nullable=False)
    status: Mapped[str] = mapped_column(String(32), default="running", nullable=False)
    provider_session_id: Mapped[str | None] = mapped_column(String(160))
    raw_stream_artifact_id: Mapped[str | None] = mapped_column(String(36))
    stdout_artifact_id: Mapped[str | None] = mapped_column(String(36))
    stderr_artifact_id: Mapped[str | None] = mapped_column(String(36))
    usage: Mapped[dict[str, Any]] = mapped_column("usage_json", JSON, default=dict, nullable=False)
    started_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    ended_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    created_at: Mapped[datetime] = mapped_column(
        UTCDateTime(),
        default=utc_now,
        nullable=False,
    )

    attempt: Mapped[WorkAttempt] = relationship(back_populates="provider_runs")


class AgentResult(Base):
    """Transient inbox for provider-submitted structured results."""

    __tablename__ = "agent_results"
    __table_args__ = (Index("ix_agent_results_kind_created", "agent_kind", "created_at"),)

    result_token: Mapped[str] = mapped_column(String(64), primary_key=True)
    agent_kind: Mapped[str] = mapped_column(String(32), nullable=False)
    payload_json: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        UTCDateTime(),
        default=utc_now,
        nullable=False,
    )


class WorkflowDefinition(TimestampMixin, Base):
    __tablename__ = "workflow_definitions"
    __table_args__ = (
        UniqueConstraint("name", "version", name="uq_workflow_definition_version"),
        Index("ix_workflow_definitions_enabled", "enabled", "name"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    name: Mapped[str] = mapped_column(String(240), nullable=False)
    version: Mapped[str] = mapped_column(String(80), nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    definition: Mapped[dict[str, Any]] = mapped_column(
        "definition_json",
        JSON,
        default=dict,
        nullable=False,
    )

    runs: Mapped[list[WorkflowRun]] = relationship(back_populates="definition")


class WorkflowRun(TimestampMixin, Base):
    __tablename__ = "workflow_runs"
    __table_args__ = (
        Index("ix_workflow_runs_status", "status", "created_at"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    workflow_definition_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("workflow_definitions.id", ondelete="RESTRICT"),
        nullable=False,
    )
    name: Mapped[str] = mapped_column(String(240), nullable=False)
    status: Mapped[str] = mapped_column(String(32), default="active", nullable=False)
    input: Mapped[dict[str, Any]] = mapped_column("input_json", JSON, default=dict, nullable=False)
    state: Mapped[dict[str, Any]] = mapped_column("state_json", JSON, default=dict, nullable=False)
    discord_thread_id: Mapped[str | None] = mapped_column(String(80))
    lease_owner: Mapped[str | None] = mapped_column(String(120))
    lease_expires_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    started_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    ended_at: Mapped[datetime | None] = mapped_column(UTCDateTime())

    definition: Mapped[WorkflowDefinition] = relationship(back_populates="runs")
    nodes: Mapped[list[WorkflowNode]] = relationship(
        back_populates="workflow_run",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    edges: Mapped[list[WorkflowEdge]] = relationship(
        back_populates="workflow_run",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class WorkflowNode(TimestampMixin, Base):
    __tablename__ = "workflow_nodes"
    __table_args__ = (
        UniqueConstraint("workflow_run_id", "node_key", name="uq_workflow_run_node_key"),
        Index("ix_workflow_nodes_run_status", "workflow_run_id", "status"),
        Index("ix_workflow_nodes_work_item", "work_item_id"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    workflow_run_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("workflow_runs.id", ondelete="CASCADE"),
        nullable=False,
    )
    node_key: Mapped[str] = mapped_column(String(120), nullable=False)
    kind: Mapped[str] = mapped_column(String(40), nullable=False)
    status: Mapped[str] = mapped_column(String(32), default="pending", nullable=False)
    definition: Mapped[dict[str, Any]] = mapped_column(
        "definition_json",
        JSON,
        default=dict,
        nullable=False,
    )
    input: Mapped[dict[str, Any]] = mapped_column("input_json", JSON, default=dict, nullable=False)
    output: Mapped[dict[str, Any]] = mapped_column("output_json", JSON, default=dict, nullable=False)
    work_item_id: Mapped[str | None] = mapped_column(
        String(36),
        ForeignKey("work_items.id", ondelete="SET NULL"),
    )
    parent_node_id: Mapped[str | None] = mapped_column(String(36))
    fanout_index: Mapped[int | None] = mapped_column(Integer)
    failure_reason: Mapped[str | None] = mapped_column(Text)

    workflow_run: Mapped[WorkflowRun] = relationship(back_populates="nodes")
    work_item: Mapped[WorkItem | None] = relationship()


class WorkflowEdge(Base):
    __tablename__ = "workflow_edges"
    __table_args__ = (
        Index("ix_workflow_edges_to_node", "to_node_id"),
        Index("ix_workflow_edges_from_node", "from_node_id"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    workflow_run_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("workflow_runs.id", ondelete="CASCADE"),
        nullable=False,
    )
    from_node_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("workflow_nodes.id", ondelete="CASCADE"),
        nullable=False,
    )
    to_node_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("workflow_nodes.id", ondelete="CASCADE"),
        nullable=False,
    )
    condition: Mapped[str] = mapped_column(String(80), default="succeeded", nullable=False)

    workflow_run: Mapped[WorkflowRun] = relationship(back_populates="edges")


class Memory(TimestampMixin, Base):
    __tablename__ = "memories"
    __table_args__ = (
        Index("ix_memories_namespace", "namespace", "created_at"),
        Index("ix_memories_work_item", "work_item_id", "created_at"),
        Index("ix_memories_canonical_key", "namespace", "canonical_key"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    namespace: Mapped[str] = mapped_column(String(160), nullable=False)
    kind: Mapped[str] = mapped_column(String(80), nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    tags: Mapped[list[str]] = mapped_column("tags_json", JSON, default=list, nullable=False)
    source_kind: Mapped[str | None] = mapped_column(String(80))
    source_id: Mapped[str | None] = mapped_column(String(240))
    work_item_id: Mapped[str | None] = mapped_column(String(36))
    canonical_key: Mapped[str | None] = mapped_column(String(240))
    superseded_by: Mapped[str | None] = mapped_column(String(36))
    archived_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    pinned: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    ttl_days: Mapped[int | None] = mapped_column(Integer)


class DiscordThread(TimestampMixin, Base):
    __tablename__ = "discord_threads"
    __table_args__ = (
        UniqueConstraint("discord_thread_id", name="uq_discord_thread_id"),
        UniqueConstraint("purpose", "work_item_id", name="uq_discord_thread_work"),
        UniqueConstraint("purpose", "workflow_run_id", name="uq_discord_thread_workflow"),
        Index("ix_discord_threads_status", "status", "created_at"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    purpose: Mapped[str] = mapped_column(String(80), nullable=False)
    discord_channel_id: Mapped[str] = mapped_column(String(80), nullable=False)
    discord_thread_id: Mapped[str] = mapped_column(String(80), nullable=False)
    work_item_id: Mapped[str | None] = mapped_column(String(36))
    workflow_run_id: Mapped[str | None] = mapped_column(String(36))
    status: Mapped[str] = mapped_column(String(32), default="active", nullable=False)


class DiscordMessage(Base):
    __tablename__ = "discord_messages"
    __table_args__ = (
        UniqueConstraint("discord_message_id", name="uq_discord_message_id"),
        Index("ix_discord_messages_thread", "discord_thread_id", "created_at"),
        Index("ix_discord_messages_work_item", "work_item_id", "created_at"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    discord_message_id: Mapped[str] = mapped_column(String(80), nullable=False)
    discord_channel_id: Mapped[str] = mapped_column(String(80), nullable=False)
    discord_thread_id: Mapped[str | None] = mapped_column(String(80))
    direction: Mapped[str] = mapped_column(String(20), nullable=False)
    author: Mapped[str | None] = mapped_column(String(160))
    content_artifact_id: Mapped[str | None] = mapped_column(String(36))
    content_preview: Mapped[str] = mapped_column(Text, nullable=False)
    work_item_id: Mapped[str | None] = mapped_column(String(36))
    workflow_run_id: Mapped[str | None] = mapped_column(String(36))
    created_at: Mapped[datetime] = mapped_column(
        UTCDateTime(),
        default=utc_now,
        nullable=False,
    )
