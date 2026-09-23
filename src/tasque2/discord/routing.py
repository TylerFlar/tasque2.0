"""Inbound Discord routing: intake messages, thread replies, and reply follow-up work.

A message in the intake channel becomes a new work item. A reply in a thread Tasque owns
(or a reply to one of its messages) becomes follow-up work for the work item that owns the
thread: its reply processor when it declares one (``reply_followup_work``), otherwise a
generic continuation of the parent. A thread bound to a workflow run answers its single
open gate, or goes to the run's newest leaf work item.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from tasque2.artifacts import ArtifactStore
from tasque2.discord.conversation import MESSAGE_LIMIT, ROW_FETCH_CAP, collapse_conversation
from tasque2.events import record_event
from tasque2.lane_context import effective_context
from tasque2.memory import MemoryService
from tasque2.models import (
    Artifact,
    DiscordMessage,
    DiscordThread,
    Memory,
    WorkAttempt,
    WorkflowEdge,
    WorkflowNode,
    WorkflowRun,
    WorkItem,
)
from tasque2.telemetry import instruments
from tasque2.templates import read_template_file
from tasque2.work.repository import WorkRepository
from tasque2.workflows import WorkflowService

logger = logging.getLogger(__name__)

INTAKE_LANE = "discord-intake"
ARCHIVED_THREAD_STATUS = "archived"
INHERITED_MEMORY_KEYS = (
    "memory_namespace",
    "memory_namespaces",
    "memory_canonical_keys",
    "memory_queries",
    "memory_tags",
    "memory_query_limit",
    "memory_kinds",
)
MODEL_CONTRACT_KEYS = ("model_profile", "model", "effort")
INHERITED_CONTRACT_KEYS = ("mcp_servers", "disallowed_tools", "max_turns", "max_budget_usd")

INTAKE_INSTRUCTION = """\
# Discord intake

## Goal
Handle this Discord message as Tasque's general assistant.

## Message
{message}

Attached files, if any, are listed below as local artifact paths.

## Instructions
Work out what the user wants and do it. Answer directly when the request is a question or a
small task. For the standing kinds of ask (reminders, calendar entries, carts, drafts, admin,
trips) follow `global/desk` (`memory_get_canonical`). When durable state should change, use the
Tasque tools: `reminder_set` for a reminder, `schedule_create_work` for recurring or future work,
`workflow_start` for an existing workflow, `schedule_fire_now` for an existing job,
`work_enqueue` for follow-up work, memory tools for durable notes, and `system_status` for
status questions. Look up names with `workflow_list` or `schedule_list` when they are unclear.

## Output
The report is posted back to the intake channel, so write it as a normal reply: say what
changed and what happens next. Keep internal ids in `produces` unless the user asked for them.
"""

DEFAULT_REPLY_INSTRUCTION = """\
# Reply follow-up

## Goal
The user replied in the thread of a Tasque work item that has no dedicated reply processor.
Act as that work's continuation: understand the reply in the context of the parent work, do
what it asks, and answer in the thread.

## Context
- `task_context.source_reply`: the reply, its attachments, and pointers to the parent work.
- `task_context.conversation.recent_messages`: the thread transcript.
- `parent_work`: the parent work item, its latest result and its report artifact. Read the
  report before interpreting the reply.
- The memory context inherited from the parent is the domain's state and doctrine.

## Instructions
- Carry out the request within the parent work's domain, conventions and authority.
- When the reply corrects a preference, plan or rule, record the correction in the domain's
  doctrine so future runs follow it.
- When the request is outside this work's scope, say so and route it (follow-up work, a
  schedule, or a workflow) instead of dropping it.

## Output
A one-sentence summary and a report written as a direct answer to the reply; it is posted
back to the same thread.
"""


@dataclass(frozen=True)
class DiscordAttachmentPayload:
    filename: str
    content_type: str | None
    data: bytes


@dataclass(frozen=True)
class DiscordRouteResult:
    action: str
    entity_id: str | None = None
    summary: str | None = None


class DiscordService:
    def __init__(self, session: Session) -> None:
        self.session = session

    def handle_intake_message(
        self,
        *,
        discord_message_id: str,
        discord_channel_id: str,
        author: str,
        content: str,
        attachments: Sequence[DiscordAttachmentPayload] | None = None,
    ) -> DiscordRouteResult:
        existing = self._message(discord_message_id)
        if existing is not None and (existing.work_item_id or existing.workflow_run_id):
            return DiscordRouteResult(
                action="intake_already_recorded",
                entity_id=existing.work_item_id or existing.workflow_run_id,
                summary="Discord intake message was already recorded.",
            )
        work_item = self.ingest_intake_message(
            discord_message_id=discord_message_id,
            discord_channel_id=discord_channel_id,
            author=author,
            content=content,
            worker_kind="provider.default",
            attachments=attachments,
            task_instruction=INTAKE_INSTRUCTION.format(message=content.strip() or "(empty message)"),
            context={
                "discord_intake": {
                    "discord_message_id": discord_message_id,
                    "discord_channel_id": discord_channel_id,
                    "author": author,
                }
            },
        )
        return DiscordRouteResult(
            action="work_queued", entity_id=work_item.id, summary=f"Queued work: {work_item.title}"
        )

    def ingest_intake_message(
        self,
        *,
        discord_message_id: str,
        discord_channel_id: str,
        author: str,
        content: str,
        worker_kind: str = "manual",
        attachments: Sequence[DiscordAttachmentPayload] | None = None,
        task_instruction: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> WorkItem:
        message = self.record_message(
            discord_message_id=discord_message_id,
            discord_channel_id=discord_channel_id,
            discord_thread_id=None,
            direction="inbound",
            author=author,
            content_preview=content,
        )
        title = next((line.strip() for line in content.splitlines() if line.strip()), "Discord intake")[:120]
        work_item = WorkRepository(self.session).create_work_item(
            title=title,
            task_instruction=task_instruction or content,
            worker_kind=worker_kind,
            context=dict(context or {}),
            source_kind="discord",
            source_id=discord_message_id,
            idempotency_key=f"discord:intake:{discord_message_id}",
            lane=INTAKE_LANE,
        )
        refs = self._record_attachments(
            attachments or [], discord_message_id=discord_message_id, work_item_id=work_item.id
        )
        known = {
            item.get("artifact_id")
            for item in (work_item.context or {}).get("attachments") or []
            if isinstance(item, dict)
        }
        new_refs = [ref for ref in refs if ref["artifact_id"] not in known]
        if new_refs:
            updated = dict(work_item.context or {})
            updated["attachments"] = [*list(updated.get("attachments") or []), *new_refs]
            updated["input_artifacts"] = [*list(updated.get("input_artifacts") or []), *new_refs]
            work_item.context = updated
            work_item.task_instruction = _with_attachment_block(work_item.task_instruction, new_refs)
        message.work_item_id = work_item.id
        self.session.flush()
        self._event(
            "discord.intake_queued",
            ("work_item", work_item.id),
            work_item_id=work_item.id,
            summary=f"Queued Discord intake from {author}",
            payload={
                "discord_message_id": discord_message_id,
                "attachment_artifact_ids": [r["artifact_id"] for r in new_refs],
            },
        )
        return work_item

    def bind_thread(
        self,
        *,
        purpose: str,
        discord_channel_id: str,
        discord_thread_id: str,
        work_item_id: str | None = None,
        workflow_run_id: str | None = None,
        status: str = "active",
    ) -> DiscordThread:
        existing = self.session.scalar(
            select(DiscordThread).where(DiscordThread.discord_thread_id == discord_thread_id)
        )
        if existing is None and work_item_id is not None:
            existing = self.session.scalar(
                select(DiscordThread).where(
                    DiscordThread.purpose == purpose, DiscordThread.work_item_id == work_item_id
                )
            )
        if existing is None and workflow_run_id is not None:
            existing = self.session.scalar(
                select(DiscordThread).where(
                    DiscordThread.purpose == purpose, DiscordThread.workflow_run_id == workflow_run_id
                )
            )
        if existing is not None:
            existing.discord_channel_id = discord_channel_id
            existing.work_item_id = work_item_id or existing.work_item_id
            existing.workflow_run_id = workflow_run_id or existing.workflow_run_id
            existing.status = status
            self.session.flush()
            return existing
        binding = DiscordThread(
            purpose=purpose,
            discord_channel_id=discord_channel_id,
            discord_thread_id=discord_thread_id,
            work_item_id=work_item_id,
            workflow_run_id=workflow_run_id,
            status=status,
        )
        self.session.add(binding)
        self.session.flush()
        return binding

    def handle_thread_reply(
        self,
        *,
        discord_message_id: str,
        discord_channel_id: str,
        discord_thread_id: str,
        author: str,
        content: str,
        attachments: Sequence[DiscordAttachmentPayload] | None = None,
        referenced_discord_message_id: str | None = None,
    ) -> DiscordRouteResult:
        binding = self.session.scalar(select(DiscordThread).where(DiscordThread.discord_thread_id == discord_thread_id))
        if binding is None:
            if referenced_discord_message_id:
                routed = self.handle_channel_message(
                    discord_message_id=discord_message_id,
                    discord_channel_id=discord_channel_id,
                    author=author,
                    content=content,
                    attachments=attachments,
                    referenced_discord_message_id=referenced_discord_message_id,
                    discord_thread_id=discord_thread_id,
                )
                if routed.action != "unbound_channel":
                    return routed
            self.record_message(
                discord_message_id=discord_message_id,
                discord_channel_id=discord_channel_id,
                discord_thread_id=discord_thread_id,
                direction="inbound",
                author=author,
                content_preview=content,
            )
            return DiscordRouteResult(action="unbound_thread", summary="No Tasque binding for thread.")
        if binding.status == ARCHIVED_THREAD_STATUS:
            self.record_message(
                discord_message_id=discord_message_id,
                discord_channel_id=discord_channel_id,
                discord_thread_id=discord_thread_id,
                direction="inbound",
                author=author,
                content_preview=content,
            )
            return DiscordRouteResult(action="archived_thread", summary="The thread's lane is archived.")
        return self._route_reply(
            owner_work_item_id=binding.work_item_id,
            owner_workflow_run_id=binding.workflow_run_id,
            discord_message_id=discord_message_id,
            discord_channel_id=discord_channel_id,
            discord_thread_id=discord_thread_id,
            author=author,
            content=content,
            attachments=attachments,
            referenced_discord_message_id=referenced_discord_message_id,
            source="thread",
        )

    def handle_channel_message(
        self,
        *,
        discord_message_id: str,
        discord_channel_id: str,
        author: str,
        content: str,
        attachments: Sequence[DiscordAttachmentPayload] | None = None,
        referenced_discord_message_id: str | None = None,
        discord_thread_id: str | None = None,
    ) -> DiscordRouteResult:
        referenced = self._message(referenced_discord_message_id)
        if referenced is None:
            self.record_message(
                discord_message_id=discord_message_id,
                discord_channel_id=discord_channel_id,
                discord_thread_id=discord_thread_id,
                direction="inbound",
                author=author,
                content_preview=content,
            )
            return DiscordRouteResult(action="unbound_channel", summary="No referenced Tasque message.")
        return self._route_reply(
            owner_work_item_id=referenced.work_item_id,
            owner_workflow_run_id=referenced.workflow_run_id,
            discord_message_id=discord_message_id,
            discord_channel_id=discord_channel_id,
            discord_thread_id=discord_thread_id,
            author=author,
            content=content,
            attachments=attachments,
            referenced_discord_message_id=referenced_discord_message_id,
            source="channel",
        )

    def record_message(
        self,
        *,
        discord_message_id: str,
        discord_channel_id: str,
        discord_thread_id: str | None,
        direction: str,
        author: str | None,
        content_preview: str,
        content_artifact_id: str | None = None,
        work_item_id: str | None = None,
        workflow_run_id: str | None = None,
    ) -> DiscordMessage:
        existing = self._message(discord_message_id)
        if existing is not None:
            return existing
        message = DiscordMessage(
            discord_message_id=discord_message_id,
            discord_channel_id=discord_channel_id,
            discord_thread_id=discord_thread_id,
            direction=direction,
            author=author,
            content_artifact_id=content_artifact_id,
            content_preview=content_preview[:1900],
            work_item_id=work_item_id,
            workflow_run_id=workflow_run_id,
        )
        self.session.add(message)
        self.session.flush()
        return message

    def _route_reply(
        self,
        *,
        owner_work_item_id: str | None,
        owner_workflow_run_id: str | None,
        discord_message_id: str,
        discord_channel_id: str,
        discord_thread_id: str | None,
        author: str,
        content: str,
        attachments: Sequence[DiscordAttachmentPayload] | None,
        referenced_discord_message_id: str | None,
        source: str,
    ) -> DiscordRouteResult:
        refs = self._record_attachments(
            attachments or [],
            discord_message_id=discord_message_id,
            discord_thread_id=discord_thread_id,
            work_item_id=owner_work_item_id,
            workflow_run_id=owner_workflow_run_id,
        )
        routed_content = _with_attachment_block(content, refs)
        self.record_message(
            discord_message_id=discord_message_id,
            discord_channel_id=discord_channel_id,
            discord_thread_id=discord_thread_id,
            direction="inbound",
            author=author,
            content_preview=routed_content,
            work_item_id=owner_work_item_id,
            workflow_run_id=owner_workflow_run_id,
        )
        if owner_work_item_id is not None:
            owner = self.session.get(WorkItem, owner_work_item_id)
            memory_id = self._record_reply_memory(
                owner, discord_message_id=discord_message_id, author=author, content=routed_content
            )
            followup_id = self._enqueue_followup(
                owner,
                discord_message_id=discord_message_id,
                author=author,
                content=routed_content,
                refs=refs,
                discord_channel_id=discord_channel_id,
                discord_thread_id=discord_thread_id,
                referenced_discord_message_id=referenced_discord_message_id,
            )
            self._event(
                f"discord.{source}_reply",
                ("work_item", owner_work_item_id),
                work_item_id=owner_work_item_id,
                summary=f"Reply from {author}",
                payload={
                    "discord_message_id": discord_message_id,
                    "referenced_discord_message_id": referenced_discord_message_id,
                    "attachment_artifact_ids": [ref["artifact_id"] for ref in refs],
                    "memory_id": memory_id,
                    "followup_work_item_id": followup_id,
                },
            )
            return DiscordRouteResult(
                action="work_reply_recorded", entity_id=followup_id or owner_work_item_id, summary="Recorded reply."
            )
        if owner_workflow_run_id is not None:
            return self._workflow_reply(
                workflow_run_id=owner_workflow_run_id,
                discord_message_id=discord_message_id,
                discord_channel_id=discord_channel_id,
                discord_thread_id=discord_thread_id,
                author=author,
                content=routed_content,
                refs=refs,
                source=source,
            )
        return DiscordRouteResult(action="message_recorded", summary="Recorded reply.")

    def _workflow_reply(
        self,
        *,
        workflow_run_id: str,
        discord_message_id: str,
        discord_channel_id: str,
        discord_thread_id: str | None,
        author: str,
        content: str,
        refs: list[dict[str, Any]],
        source: str,
    ) -> DiscordRouteResult:
        run = self.session.get(WorkflowRun, workflow_run_id)
        if run is None:
            return DiscordRouteResult(
                action="workflow_missing", entity_id=workflow_run_id, summary="Workflow run is gone."
            )
        gates = self.session.scalars(
            select(WorkflowNode).where(
                WorkflowNode.workflow_run_id == run.id,
                WorkflowNode.kind == "gate",
                WorkflowNode.status == "awaiting_input",
            )
        ).all()
        if run.status == "awaiting_input" and len(gates) == 1:
            node = WorkflowService(self.session).answer_gate(
                workflow_run_id=run.id, node_key=gates[0].node_key, answer=content
            )
            return DiscordRouteResult(
                action="workflow_gate_answered", entity_id=node.id, summary=f"Answered gate {node.node_key}."
            )
        parent = self.workflow_reply_owner(run.id)
        memory_id = followup_id = None
        if parent is not None:
            memory_id = self._record_reply_memory(
                parent, discord_message_id=discord_message_id, author=author, content=content
            )
            followup_id = self._enqueue_followup(
                parent,
                discord_message_id=discord_message_id,
                author=author,
                content=content,
                refs=refs,
                discord_channel_id=discord_channel_id,
                discord_thread_id=discord_thread_id,
                referenced_discord_message_id=None,
            )
        self._event(
            f"discord.workflow_{source}_reply",
            ("workflow_run", run.id),
            workflow_run_id=run.id,
            summary=f"Workflow reply from {author}",
            payload={
                "discord_message_id": discord_message_id,
                "attachment_artifact_ids": [ref["artifact_id"] for ref in refs],
                "memory_id": memory_id,
                "followup_work_item_id": followup_id,
                "parent_work_item_id": parent.id if parent is not None else None,
            },
        )
        if followup_id is not None:
            return DiscordRouteResult(
                action="workflow_reply_followup_recorded", entity_id=followup_id, summary="Queued follow-up."
            )
        return DiscordRouteResult(
            action="workflow_reply_recorded", entity_id=run.id, summary="Recorded workflow reply."
        )

    def workflow_reply_owner(self, workflow_run_id: str) -> WorkItem | None:
        """The work item replies in a workflow's thread go to: the newest leaf work item,
        preferring one that declares a reply processor."""
        nodes = list(
            self.session.scalars(
                select(WorkflowNode)
                .where(
                    WorkflowNode.workflow_run_id == workflow_run_id,
                    WorkflowNode.work_item_id.is_not(None),
                    WorkflowNode.kind == "work",
                )
                .order_by(WorkflowNode.created_at, WorkflowNode.node_key)
            ).all()
        )
        if not nodes:
            return None
        upstream = {
            edge.from_node_id
            for edge in self.session.scalars(
                select(WorkflowEdge).where(WorkflowEdge.workflow_run_id == workflow_run_id)
            ).all()
        }
        leaves = [node for node in nodes if node.id not in upstream] or nodes
        fallback: WorkItem | None = None
        for node in reversed(leaves):
            work_item = self.session.get(WorkItem, node.work_item_id)
            if work_item is None:
                continue
            context = effective_context(work_item.context)
            if reply_followup_config(context) is not None:
                return work_item
            if fallback is None and not reply_followup_disabled(context):
                fallback = work_item
        return fallback

    def _record_reply_memory(
        self, owner: WorkItem | None, *, discord_message_id: str, author: str, content: str
    ) -> str | None:
        if owner is None:
            return None
        owner_context = effective_context(owner.context)
        config = owner_context.get("reply_memory")
        if not isinstance(config, dict) or config.get("enabled", True) is False:
            return None
        existing = self.session.scalar(
            select(Memory).where(Memory.source_kind == "discord_reply", Memory.source_id == discord_message_id)
        )
        if existing is not None:
            return existing.id
        namespace = str(config.get("namespace") or context_namespace(owner_context) or "global")
        template = str(config.get("content_template") or "Discord reply from {author}:\n{content}")
        memory_content = template.format(author=author, content=content, work_title=owner.title)
        tags = list(dict.fromkeys([*_string_list(config.get("tags")), "discord", "reply"]))
        service = MemoryService(self.session)
        common = {
            "namespace": namespace,
            "kind": str(config.get("kind") or "note"),
            "content": memory_content,
            "tags": tags,
            "source_kind": "discord_reply",
            "source_id": discord_message_id,
            "work_item_id": owner.id,
            "ttl_days": _optional_int(config.get("ttl_days")),
        }
        canonical_key = str(config.get("canonical_key") or "").strip()
        memory = (
            service.upsert_canonical(canonical_key=canonical_key, **common)
            if canonical_key
            else service.create_memory(**common)
        )
        return memory.id

    def _enqueue_followup(
        self,
        owner: WorkItem | None,
        *,
        discord_message_id: str,
        author: str,
        content: str,
        refs: list[dict[str, Any]],
        discord_channel_id: str,
        discord_thread_id: str | None,
        referenced_discord_message_id: str | None,
    ) -> str | None:
        if owner is None:
            return None
        owner_context = effective_context(owner.context)
        config = reply_followup_config(owner_context)
        default_processor = config is None
        if default_processor:
            if reply_followup_disabled(owner_context):
                return None
            config = _default_reply_config(owner, owner_context)
        try:
            instruction = _reply_instruction(config)
        except (OSError, ValueError) as exc:
            # A reply is never dropped: a thread whose processor template is gone falls back to the
            # default processor, still at the thread's configured tier.
            logger.warning("Reply processor for %s is unavailable (%s); using the default processor", owner.id, exc)
            default_processor = True
            config = {
                **_default_reply_config(owner, owner_context),
                "runtime_contract": dict(config.get("runtime_contract") or {}),
            }
            instruction = DEFAULT_REPLY_INSTRUCTION
        parent_context = owner_context
        child_context = dict(config.get("context") or {})
        if "memory_namespace" not in child_context and context_namespace(parent_context):
            child_context["memory_namespace"] = context_namespace(parent_context)
        for key in ("reply_followup_work", "reply_memory"):
            if key not in child_context and isinstance(parent_context.get(key), dict):
                child_context[key] = parent_context[key]
        if default_processor:
            child_context["reply_default_processor"] = True
        subject = self._reply_subject(
            owner, discord_thread_id=discord_thread_id, referenced_discord_message_id=referenced_discord_message_id
        )
        report_ref = self._parent_report_ref(subject)
        output_thread_id = discord_thread_id or owner.discord_thread_id
        child_context.setdefault("parent_work_item_id", subject.id)
        child_context.setdefault("parent_report_artifact_id", report_ref["artifact_id"] if report_ref else None)
        if output_thread_id:
            child_context.setdefault("parent_discord_thread_id", output_thread_id)
        child_context["conversation"] = {
            "scope": "thread" if discord_thread_id else "channel",
            "discord_channel_id": discord_channel_id,
            "discord_thread_id": discord_thread_id,
            "current_discord_message_id": discord_message_id,
            "referenced_discord_message_id": referenced_discord_message_id,
            "recent_messages": self._recent_messages(
                discord_channel_id=discord_channel_id, discord_thread_id=discord_thread_id
            ),
        }
        input_artifacts = _dedupe_refs(
            [*_ref_list(child_context.get("input_artifacts")), *([report_ref] if report_ref else []), *refs]
        )
        if input_artifacts:
            child_context["input_artifacts"] = input_artifacts
        if refs:
            child_context["attachments"] = refs
        child_context["source_reply"] = {
            "discord_message_id": discord_message_id,
            "author": author,
            "parent_work_item_id": subject.id,
            "parent_report_artifact_id": report_ref["artifact_id"] if report_ref else None,
            "content": content,
            "artifact_refs": refs,
            "referenced_discord_message_id": referenced_discord_message_id,
        }
        reply_block = (
            f"Reply to process:\n\nAuthor: {author}\nParent work item: {subject.id} - {subject.title}\n\n{content}"
        )
        # Work in a run posts only through the run's final result, so the default processor, which
        # answers in the thread, stays out of the run; a reply processor stays in unless it sets
        # detach_from_workflow.
        detached = default_processor or bool(config.get("detach_from_workflow"))
        child = WorkRepository(self.session).create_work_item(
            title=str(config.get("title") or f"Reply: {owner.title}")[:240],
            task_instruction=f"{instruction}\n\n{reply_block}",
            worker_kind=str(config.get("worker_kind") or "provider.default"),
            runtime_contract=followup_contract(
                owner.runtime_contract or {}, dict(config.get("runtime_contract") or {})
            ),
            context=child_context,
            priority=int(config.get("priority", owner.priority)),
            max_attempts=int(config.get("max_attempts", 1)),
            idempotency_key=f"discord:reply-followup:{discord_message_id}",
            source_kind="discord_reply_followup",
            source_id=discord_message_id,
            workflow_run_id=None if detached else owner.workflow_run_id,
            discord_thread_id=output_thread_id,
            lane=owner.lane,
        )
        return child.id

    def _recent_messages(self, *, discord_channel_id: str, discord_thread_id: str | None) -> list[dict[str, Any]]:
        statement = select(DiscordMessage).order_by(DiscordMessage.created_at.desc()).limit(ROW_FETCH_CAP)
        if discord_thread_id:
            statement = statement.where(DiscordMessage.discord_thread_id == discord_thread_id)
        else:
            statement = statement.where(
                DiscordMessage.discord_channel_id == discord_channel_id, DiscordMessage.discord_thread_id.is_(None)
            )
        rows = list(reversed(self.session.scalars(statement).all()))
        return collapse_conversation([_message_data(row) for row in rows], limit=MESSAGE_LIMIT)

    def _reply_subject(
        self, owner: WorkItem, *, discord_thread_id: str | None, referenced_discord_message_id: str | None
    ) -> WorkItem:
        """The work whose post a reply answers: the message it quotes, else the thread's newest post.

        A long-lived lane thread is owned by its opener, which decides how replies run; the post
        being answered is usually a later run's, and that run is the reply's parent.
        """
        message = self._message(referenced_discord_message_id) if referenced_discord_message_id else None
        if (message is None or not message.work_item_id) and discord_thread_id:
            message = self.session.scalar(
                select(DiscordMessage)
                .where(
                    DiscordMessage.discord_thread_id == discord_thread_id,
                    DiscordMessage.direction == "outbound",
                    DiscordMessage.work_item_id.is_not(None),
                )
                .order_by(DiscordMessage.created_at.desc())
            )
        subject = self.session.get(WorkItem, message.work_item_id) if message and message.work_item_id else None
        return subject or owner

    def _parent_report_ref(self, owner: WorkItem) -> dict[str, Any] | None:
        attempt = self.session.scalar(
            select(WorkAttempt)
            .where(WorkAttempt.work_item_id == owner.id)
            .order_by(WorkAttempt.attempt_number.desc(), WorkAttempt.created_at.desc())
        )
        if attempt is None or not attempt.report_artifact_id:
            return None
        artifact = self.session.get(Artifact, attempt.report_artifact_id)
        if artifact is None:
            return None
        return {
            "artifact_id": artifact.id,
            "filename": artifact.title,
            "content_type": artifact.content_type,
            "size_bytes": artifact.size_bytes,
            "local_path": artifact.local_path,
            "kind": artifact.kind,
        }

    def _record_attachments(
        self,
        attachments: Sequence[DiscordAttachmentPayload],
        *,
        discord_message_id: str,
        discord_thread_id: str | None = None,
        work_item_id: str | None = None,
        workflow_run_id: str | None = None,
    ) -> list[dict[str, Any]]:
        refs: list[dict[str, Any]] = []
        for index, attachment in enumerate(attachments):
            source_id = f"{discord_message_id}:{index}:{attachment.filename}"
            artifact = self.session.scalar(
                select(Artifact).where(Artifact.source_kind == "discord_attachment", Artifact.source_id == source_id)
            ) or ArtifactStore().write_bytes(
                self.session,
                kind="discord_attachment",
                title=attachment.filename,
                content=attachment.data,
                content_type=attachment.content_type,
                work_item_id=work_item_id,
                workflow_run_id=workflow_run_id,
                tags=["discord", "attachment"],
                source_kind="discord_attachment",
                source_id=source_id,
            )
            refs.append(
                {
                    "artifact_id": artifact.id,
                    "filename": attachment.filename,
                    "content_type": artifact.content_type,
                    "size_bytes": artifact.size_bytes,
                    "local_path": artifact.local_path,
                    "discord_message_id": discord_message_id,
                    "discord_thread_id": discord_thread_id,
                }
            )
        return refs

    def _message(self, discord_message_id: str | None) -> DiscordMessage | None:
        if not discord_message_id:
            return None
        return self.session.scalar(
            select(DiscordMessage).where(DiscordMessage.discord_message_id == discord_message_id)
        )

    def _event(
        self,
        event_type: str,
        entity: tuple[str, str],
        *,
        summary: str,
        work_item_id: str | None = None,
        workflow_run_id: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        record_event(
            self.session,
            event_type=event_type,
            entity_kind=entity[0],
            entity_id=entity[1],
            work_item_id=work_item_id,
            workflow_run_id=workflow_run_id,
            source="discord",
            summary=summary,
            payload=payload,
        )
        instruments().discord_messages.add(
            1, {"tasque.discord.direction": "inbound", "tasque.discord.route": event_type}
        )


def reply_followup_config(context: dict[str, Any]) -> dict[str, Any] | None:
    value = context.get("reply_followup_work")
    if not isinstance(value, dict) or value.get("enabled", True) is False:
        return None
    return dict(value)


def reply_followup_disabled(context: dict[str, Any]) -> bool:
    value = context.get("reply_followup_work")
    if isinstance(value, dict) and value.get("enabled", True) is False:
        return True
    return context.get("reply_followup_disabled") is True


def followup_contract(parent_contract: dict[str, Any], config_contract: dict[str, Any]) -> dict[str, Any]:
    """A reply child's contract: the parent's model and tool limits unless the reply config sets them."""
    contract: dict[str, Any] = {}
    if not any(key in config_contract for key in MODEL_CONTRACT_KEYS):
        contract.update({key: parent_contract[key] for key in MODEL_CONTRACT_KEYS if key in parent_contract})
    contract.update({key: parent_contract[key] for key in INHERITED_CONTRACT_KEYS if key in parent_contract})
    contract.update(config_contract)
    return contract


def context_namespace(context: dict[str, Any]) -> str | None:
    value = context.get("memory_namespace")
    if isinstance(value, str) and value.strip():
        return value.strip()
    for namespace in context.get("memory_namespaces") or []:
        if str(namespace).strip():
            return str(namespace).strip()
    return None


def _default_reply_config(owner: WorkItem, owner_context: dict[str, Any]) -> dict[str, Any]:
    return {
        "title": f"Reply: {owner.title}",
        "task_instruction": DEFAULT_REPLY_INSTRUCTION,
        "context": {key: owner_context[key] for key in INHERITED_MEMORY_KEYS if key in owner_context},
    }


def _reply_instruction(config: dict[str, Any]) -> str:
    template_path = config.get("task_template_path")
    if template_path:
        base_dir = str(config.get("template_base_dir") or "").strip()
        return read_template_file(str(template_path), base_dir=Path(base_dir) if base_dir else None)
    return str(config.get("task_instruction") or "Process this Discord reply.").strip()


def _with_attachment_block(content: str, refs: list[dict[str, Any]]) -> str:
    if not refs:
        return content
    lines = ["Attached files available locally:"]
    for ref in refs:
        size = f", {ref['size_bytes']} bytes" if ref.get("size_bytes") is not None else ""
        lines.append(
            f"- {ref['filename']} ({ref.get('content_type') or 'application/octet-stream'}{size}): {ref['local_path']}"
        )
    base = content.strip()
    return f"{base}\n\n" + "\n".join(lines) if base else "\n".join(lines)


def _message_data(message: DiscordMessage) -> dict[str, Any]:
    return {
        "discord_message_id": message.discord_message_id,
        "discord_thread_id": message.discord_thread_id,
        "direction": message.direction,
        "author": message.author,
        "content": message.content_preview,
        "work_item_id": message.work_item_id,
        "created_at": message.created_at.isoformat(),
    }


def _ref_list(value: Any) -> list[dict[str, Any]]:
    return (
        [dict(item) for item in value if isinstance(item, dict) and isinstance(item.get("artifact_id"), str)]
        if isinstance(value, list)
        else []
    )


def _dedupe_refs(values: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    result: list[dict[str, Any]] = []
    for value in values:
        artifact_id = value.get("artifact_id")
        if isinstance(artifact_id, str) and artifact_id not in seen:
            seen.add(artifact_id)
            result.append(value)
    return result


def _string_list(value: Any) -> list[str]:
    if isinstance(value, str):
        value = [value]
    return [str(item).strip() for item in value or [] if str(item).strip()] if isinstance(value, list) else []


def _optional_int(value: Any) -> int | None:
    if value is None or str(value).strip() == "":
        return None
    return int(value)
