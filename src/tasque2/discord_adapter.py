from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from tasque2.artifacts import ArtifactStore
from tasque2.memory import MemoryService
from tasque2.memory_ingest import MemoryIngestService
from tasque2.models import (
    Artifact,
    DiscordMessage,
    DiscordThread,
    Memory,
    WorkAttempt,
    WorkEvent,
    WorkflowNode,
    WorkflowRun,
    WorkItem,
)
from tasque2.repo import WorkRepository
from tasque2.templates import read_template_file
from tasque2.workflows import WorkflowService


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
        existing = self._referenced_message(discord_message_id)
        if existing is not None:
            entity_id = existing.work_item_id or existing.workflow_run_id
            return DiscordRouteResult(
                action="intake_already_recorded",
                entity_id=entity_id,
                summary="Discord intake message was already recorded.",
            )

        work_item = self.ingest_intake_message(
            discord_message_id=discord_message_id,
            discord_channel_id=discord_channel_id,
            author=author,
            content=content,
            worker_kind="provider.default",
            attachments=attachments,
            task_instruction=_general_intake_instruction(content),
            context={
                "discord_intake": {
                    "discord_message_id": discord_message_id,
                    "discord_channel_id": discord_channel_id,
                    "author": author,
                }
            },
        )
        return DiscordRouteResult(
            action="work_queued",
            entity_id=work_item.id,
            summary=f"Queued work: {work_item.title}",
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
        title = self._title_from_content(content)
        work_item = WorkRepository(self.session).create_work_item(
            title=title,
            task_instruction=task_instruction or content,
            worker_kind=worker_kind,
            context=dict(context or {}),
            source_kind="discord",
            source_id=discord_message_id,
            idempotency_key=f"discord:intake:{discord_message_id}",
        )
        artifact_refs = self._record_attachments(
            attachments or [],
            discord_message_id=discord_message_id,
            work_item_id=work_item.id,
        )
        new_artifact_refs = self._new_attachment_refs(work_item.context, artifact_refs)
        if new_artifact_refs:
            work_item.context = self._context_with_attachments(work_item.context, new_artifact_refs)
            work_item.task_instruction = self._content_with_attachment_block(
                work_item.task_instruction,
                new_artifact_refs,
            )
        message.work_item_id = work_item.id
        self.session.flush()
        self._emit_event(
            event_type="discord.intake_queued",
            entity_kind="work_item",
            entity_id=work_item.id,
            work_item_id=work_item.id,
            summary=f"Queued Discord intake from {author}",
            payload={
                "discord_message_id": discord_message_id,
                "attachment_artifact_ids": [ref["artifact_id"] for ref in new_artifact_refs],
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
        if existing is not None:
            self._refresh_thread_binding(
                existing,
                discord_channel_id=discord_channel_id,
                work_item_id=work_item_id,
                workflow_run_id=workflow_run_id,
                status=status,
            )
            return existing
        existing = self._logical_thread_binding(
            purpose=purpose,
            work_item_id=work_item_id,
            workflow_run_id=workflow_run_id,
        )
        if existing is not None:
            self._refresh_thread_binding(
                existing,
                discord_channel_id=discord_channel_id,
                work_item_id=work_item_id,
                workflow_run_id=workflow_run_id,
                status=status,
            )
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

    def _logical_thread_binding(
        self,
        *,
        purpose: str,
        work_item_id: str | None,
        workflow_run_id: str | None,
    ) -> DiscordThread | None:
        if work_item_id is not None:
            existing = self.session.scalar(
                select(DiscordThread).where(
                    DiscordThread.purpose == purpose,
                    DiscordThread.work_item_id == work_item_id,
                )
            )
            if existing is not None:
                return existing
        if workflow_run_id is not None:
            existing = self.session.scalar(
                select(DiscordThread).where(
                    DiscordThread.purpose == purpose,
                    DiscordThread.workflow_run_id == workflow_run_id,
                )
            )
            if existing is not None:
                return existing
        return None

    def _refresh_thread_binding(
        self,
        thread: DiscordThread,
        *,
        discord_channel_id: str,
        work_item_id: str | None,
        workflow_run_id: str | None,
        status: str,
    ) -> None:
        thread.discord_channel_id = discord_channel_id
        if work_item_id is not None:
            thread.work_item_id = work_item_id
        if workflow_run_id is not None:
            thread.workflow_run_id = workflow_run_id
        thread.status = status
        self.session.flush()

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
        binding = self.session.scalar(
            select(DiscordThread).where(DiscordThread.discord_thread_id == discord_thread_id)
        )
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

        artifact_refs = self._record_attachments(
            attachments or [],
            discord_message_id=discord_message_id,
            discord_thread_id=discord_thread_id,
            work_item_id=binding.work_item_id,
            workflow_run_id=binding.workflow_run_id,
        )
        routed_content = self._content_with_attachment_block(content, artifact_refs)
        self.record_message(
            discord_message_id=discord_message_id,
            discord_channel_id=discord_channel_id,
            discord_thread_id=discord_thread_id,
            direction="inbound",
            author=author,
            content_preview=routed_content,
            work_item_id=binding.work_item_id,
            workflow_run_id=binding.workflow_run_id,
        )

        if binding.work_item_id is not None:
            work_item = self.session.get(WorkItem, binding.work_item_id)
            memory_id = self._record_reply_memory(
                work_item,
                discord_message_id=discord_message_id,
                author=author,
                content=routed_content,
            )
            followup_work_id = self._enqueue_reply_followup(
                work_item,
                discord_message_id=discord_message_id,
                author=author,
                content=routed_content,
                artifact_refs=artifact_refs,
                discord_channel_id=discord_channel_id,
                discord_thread_id=discord_thread_id,
                referenced_discord_message_id=referenced_discord_message_id,
            )
            self._emit_event(
                event_type="discord.thread_reply",
                entity_kind="work_item",
                entity_id=binding.work_item_id,
                work_item_id=binding.work_item_id,
                summary=f"Thread reply from {author}",
                payload={
                    "discord_message_id": discord_message_id,
                    "attachment_artifact_ids": [ref["artifact_id"] for ref in artifact_refs],
                    "memory_id": memory_id,
                    "followup_work_item_id": followup_work_id,
                },
            )
            return DiscordRouteResult(
                action="work_reply_recorded",
                entity_id=followup_work_id or binding.work_item_id,
                summary="Recorded work thread reply.",
            )

        if binding.workflow_run_id is not None:
            return self._handle_workflow_reply(
                workflow_run_id=binding.workflow_run_id,
                discord_message_id=discord_message_id,
                author=author,
                content=routed_content,
                artifact_refs=artifact_refs,
                source="thread",
            )

        return DiscordRouteResult(action="message_recorded", summary="Recorded thread reply.")

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
        referenced = self._referenced_message(referenced_discord_message_id)
        if referenced is None:
            self.record_message(
                discord_message_id=discord_message_id,
                discord_channel_id=discord_channel_id,
                discord_thread_id=discord_thread_id,
                direction="inbound",
                author=author,
                content_preview=content,
            )
            return DiscordRouteResult(
                action="unbound_channel",
                summary="No referenced Tasque message for channel conversation.",
            )

        artifact_refs = self._record_attachments(
            attachments or [],
            discord_message_id=discord_message_id,
            discord_thread_id=discord_thread_id,
            work_item_id=referenced.work_item_id,
            workflow_run_id=referenced.workflow_run_id,
        )
        routed_content = self._content_with_attachment_block(content, artifact_refs)
        self.record_message(
            discord_message_id=discord_message_id,
            discord_channel_id=discord_channel_id,
            discord_thread_id=discord_thread_id,
            direction="inbound",
            author=author,
            content_preview=routed_content,
            work_item_id=referenced.work_item_id,
            workflow_run_id=referenced.workflow_run_id,
        )

        if referenced.work_item_id is not None:
            work_item = self.session.get(WorkItem, referenced.work_item_id)
            memory_id = self._record_reply_memory(
                work_item,
                discord_message_id=discord_message_id,
                author=author,
                content=routed_content,
            )
            followup_work_id = self._enqueue_reply_followup(
                work_item,
                discord_message_id=discord_message_id,
                author=author,
                content=routed_content,
                artifact_refs=artifact_refs,
                discord_channel_id=discord_channel_id,
                discord_thread_id=discord_thread_id,
                referenced_discord_message_id=referenced_discord_message_id,
            )
            self._emit_event(
                event_type="discord.channel_reply",
                entity_kind="work_item",
                entity_id=referenced.work_item_id,
                work_item_id=referenced.work_item_id,
                summary=f"Channel reply from {author}",
                payload={
                    "discord_message_id": discord_message_id,
                    "referenced_discord_message_id": referenced_discord_message_id,
                    "attachment_artifact_ids": [ref["artifact_id"] for ref in artifact_refs],
                    "memory_id": memory_id,
                    "followup_work_item_id": followup_work_id,
                },
            )
            return DiscordRouteResult(
                action="work_reply_recorded",
                entity_id=followup_work_id or referenced.work_item_id,
                summary="Recorded work channel reply.",
            )

        if referenced.workflow_run_id is not None:
            return self._handle_workflow_reply(
                workflow_run_id=referenced.workflow_run_id,
                discord_message_id=discord_message_id,
                author=author,
                content=routed_content,
                artifact_refs=artifact_refs,
                source="channel",
            )

        return DiscordRouteResult(action="message_recorded", summary="Recorded channel reply.")

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
        existing = self.session.scalar(
            select(DiscordMessage).where(DiscordMessage.discord_message_id == discord_message_id)
        )
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
        if direction == "inbound" and content_preview.strip():
            MemoryIngestService(self.session).ingest_text(
                namespace="discord",
                title=f"Discord message from {author or 'unknown'}",
                content=content_preview,
                source_kind="discord_message",
                source_id=f"message:{discord_message_id}",
                tags=["discord", "message", direction],
                work_item_id=work_item_id,
            )
        return message

    def _handle_workflow_reply(
        self,
        *,
        workflow_run_id: str,
        discord_message_id: str,
        author: str,
        content: str,
        artifact_refs: list[dict[str, Any]],
        source: str,
    ) -> DiscordRouteResult:
        run = self.session.get(WorkflowRun, workflow_run_id)
        if run is None:
            return DiscordRouteResult(
                action="workflow_missing",
                entity_id=workflow_run_id,
                summary="Workflow run no longer exists.",
            )
        gates = self._awaiting_workflow_gates(run.id)
        if run.status == "awaiting_input" and len(gates) == 1:
            node = WorkflowService(self.session).answer_gate(
                workflow_run_id=run.id,
                node_key=gates[0].node_key,
                answer=content,
            )
            return DiscordRouteResult(
                action="workflow_gate_answered",
                entity_id=node.id,
                summary=f"Answered workflow gate {node.node_key}.",
            )

        self._emit_event(
            event_type=f"discord.workflow_{source}_reply",
            entity_kind="workflow_run",
            entity_id=run.id,
            workflow_run_id=run.id,
            summary=f"Workflow reply from {author}",
            payload={
                "discord_message_id": discord_message_id,
                "attachment_artifact_ids": [ref["artifact_id"] for ref in artifact_refs],
            },
        )
        return DiscordRouteResult(
            action="workflow_reply_recorded",
            entity_id=run.id,
            summary="Recorded workflow reply.",
        )

    def _awaiting_workflow_gates(self, workflow_run_id: str) -> list[WorkflowNode]:
        return list(
            self.session.scalars(
                select(WorkflowNode).where(
                    WorkflowNode.workflow_run_id == workflow_run_id,
                    WorkflowNode.kind == "gate",
                    WorkflowNode.status == "awaiting_input",
                )
            ).all()
        )

    def _title_from_content(self, content: str) -> str:
        first_line = next((line.strip() for line in content.splitlines() if line.strip()), "Discord intake")
        return first_line[:120]

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
            existing = self.session.scalar(
                select(Artifact).where(
                    Artifact.source_kind == "discord_attachment",
                    Artifact.source_id == source_id,
                )
            )
            artifact = existing or ArtifactStore().write_bytes(
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
            MemoryIngestService(self.session).ingest_artifact(
                artifact.id,
                namespace="discord",
                tags=["discord", "attachment"],
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

    def _context_with_attachments(
        self,
        context: dict[str, Any],
        refs: list[dict[str, Any]],
    ) -> dict[str, Any]:
        updated = dict(context or {})
        updated["attachments"] = [*list(updated.get("attachments") or []), *refs]
        updated["input_artifacts"] = [*list(updated.get("input_artifacts") or []), *refs]
        return updated

    def _new_attachment_refs(
        self,
        context: dict[str, Any],
        refs: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        existing_ids = {
            item.get("artifact_id")
            for item in list((context or {}).get("attachments") or [])
            if isinstance(item, dict)
        }
        return [ref for ref in refs if ref.get("artifact_id") not in existing_ids]

    def _content_with_attachment_block(
        self,
        content: str,
        refs: list[dict[str, Any]],
    ) -> str:
        if not refs:
            return content
        base = content.strip()
        lines = ["Attached files available locally:"]
        for ref in refs:
            size = ref.get("size_bytes")
            content_type = ref.get("content_type") or "application/octet-stream"
            suffix = f", {size} bytes" if size is not None else ""
            lines.append(f"- {ref['filename']} ({content_type}{suffix}): {ref['local_path']}")
        block = "\n".join(lines)
        return f"{base}\n\n{block}" if base else block

    def _record_reply_memory(
        self,
        work_item: WorkItem | None,
        *,
        discord_message_id: str,
        author: str,
        content: str,
    ) -> str | None:
        if work_item is None:
            return None
        config = _reply_memory_config(work_item.context or {})
        if config is None:
            return None

        existing = self.session.scalar(
            select(Memory).where(
                Memory.source_kind == "discord_reply",
                Memory.source_id == discord_message_id,
            )
        )
        if existing is not None:
            return existing.id

        namespace = str(config.get("namespace") or _context_memory_namespace(work_item.context) or "global")
        kind = str(config.get("kind") or "note")
        tags = _dedupe_strings([*_string_list(config.get("tags")), "discord", "reply"])
        ttl_days = _optional_int(config.get("ttl_days"))
        body = str(config.get("content_template") or "Discord reply from {author}:\n{content}")
        memory_content = body.format(author=author, content=content, work_title=work_item.title)
        canonical_key = _optional_string(config.get("canonical_key"))
        service = MemoryService(self.session)
        if canonical_key:
            memory = service.upsert_canonical(
                namespace=namespace,
                canonical_key=canonical_key,
                kind=kind,
                content=memory_content,
                tags=tags,
                source_kind="discord_reply",
                source_id=discord_message_id,
                work_item_id=work_item.id,
                ttl_days=ttl_days,
            )
        else:
            memory = service.create_memory(
                namespace=namespace,
                kind=kind,
                content=memory_content,
                tags=tags,
                source_kind="discord_reply",
                source_id=discord_message_id,
                work_item_id=work_item.id,
                ttl_days=ttl_days,
            )
        return memory.id

    def _enqueue_reply_followup(
        self,
        work_item: WorkItem | None,
        *,
        discord_message_id: str,
        author: str,
        content: str,
        artifact_refs: list[dict[str, Any]],
        discord_channel_id: str,
        discord_thread_id: str | None,
        referenced_discord_message_id: str | None,
    ) -> str | None:
        if work_item is None:
            return None
        config = _reply_followup_config(work_item.context or {})
        if config is None:
            return None

        base_instruction = _reply_followup_instruction(config)
        reply_block = "\n\n".join(
            [
                "Discord reply to process:",
                f"Author: {author}",
                f"Parent work item: {work_item.id} - {work_item.title}",
                "Content:",
                content,
            ]
        )
        child_context = dict(config.get("context") or {})
        if "memory_namespace" not in child_context:
            namespace = _context_memory_namespace(work_item.context)
            if namespace:
                child_context["memory_namespace"] = namespace
        child_context.setdefault("parent_work_item_id", work_item.id)
        parent_context = self._parent_reply_context(work_item)
        for key, value in parent_context.items():
            child_context.setdefault(key, value)
        output_thread_id = discord_thread_id or work_item.discord_thread_id
        if output_thread_id and not child_context.get("parent_discord_thread_id"):
            child_context["parent_discord_thread_id"] = output_thread_id
        child_context["conversation"] = self._conversation_context(
            discord_channel_id=discord_channel_id,
            discord_thread_id=discord_thread_id,
            current_discord_message_id=discord_message_id,
            referenced_discord_message_id=referenced_discord_message_id,
        )

        input_artifacts = _dedupe_artifact_refs(
            [
                *_artifact_ref_list(child_context.get("input_artifacts")),
                *_artifact_ref_list(parent_context.get("related_artifacts")),
                *artifact_refs,
            ]
        )
        if input_artifacts:
            child_context["input_artifacts"] = input_artifacts
        if artifact_refs:
            child_context["attachments"] = artifact_refs
        child_context["source_reply"] = {
            "discord_message_id": discord_message_id,
            "author": author,
            "parent_work_item_id": work_item.id,
            "parent_report_artifact_id": parent_context.get("parent_report_artifact_id"),
            "content": content,
            "artifact_refs": artifact_refs,
            "referenced_discord_message_id": referenced_discord_message_id,
        }
        child = WorkRepository(self.session).create_work_item(
            title=str(config.get("title") or f"Process reply: {work_item.title}")[:240],
            task_instruction=f"{base_instruction}\n\n{reply_block}",
            worker_kind=str(config.get("worker_kind") or "provider.default"),
            runtime_contract=_reply_followup_runtime_contract(
                parent_contract=work_item.runtime_contract or {},
                config_contract=dict(config.get("runtime_contract") or {}),
            ),
            context=child_context,
            priority=int(config.get("priority", work_item.priority)),
            max_attempts=int(config.get("max_attempts", 1)),
            idempotency_key=f"discord:reply-followup:{discord_message_id}",
            source_kind="discord_reply_followup",
            source_id=discord_message_id,
            workflow_run_id=work_item.workflow_run_id,
            discord_thread_id=output_thread_id,
        )
        return child.id

    def _conversation_context(
        self,
        *,
        discord_channel_id: str,
        discord_thread_id: str | None,
        current_discord_message_id: str,
        referenced_discord_message_id: str | None,
        limit: int = 20,
    ) -> dict[str, Any]:
        return {
            "scope": "thread" if discord_thread_id else "channel",
            "discord_channel_id": discord_channel_id,
            "discord_thread_id": discord_thread_id,
            "current_discord_message_id": current_discord_message_id,
            "referenced_discord_message_id": referenced_discord_message_id,
            "recent_messages": self._recent_messages(
                discord_channel_id=discord_channel_id,
                discord_thread_id=discord_thread_id,
                limit=limit,
            ),
        }

    def _recent_messages(
        self,
        *,
        discord_channel_id: str,
        discord_thread_id: str | None,
        limit: int,
    ) -> list[dict[str, Any]]:
        statement = select(DiscordMessage).order_by(DiscordMessage.created_at.desc()).limit(limit)
        if discord_thread_id:
            statement = statement.where(DiscordMessage.discord_thread_id == discord_thread_id)
        else:
            statement = statement.where(
                DiscordMessage.discord_channel_id == discord_channel_id,
                DiscordMessage.discord_thread_id.is_(None),
            )
        rows = list(reversed(self.session.scalars(statement).all()))
        return [_message_context(message) for message in rows]

    def _parent_reply_context(self, work_item: WorkItem) -> dict[str, Any]:
        latest_attempt = self._latest_attempt(work_item.id)
        parent_report_artifact_id = latest_attempt.report_artifact_id if latest_attempt else None
        report_ref = self._artifact_ref(parent_report_artifact_id) if parent_report_artifact_id else None
        related_artifacts = [report_ref] if report_ref is not None else []
        return {
            "parent_work_item_id": work_item.id,
            "parent_discord_thread_id": work_item.discord_thread_id,
            "parent_report_artifact_id": parent_report_artifact_id,
            "parent_work": {
                "id": work_item.id,
                "title": work_item.title,
                "status": work_item.status,
                "worker_kind": work_item.worker_kind,
                "source_kind": work_item.source_kind,
                "source_id": work_item.source_id,
                "latest_attempt": _attempt_context(latest_attempt),
            },
            "related_artifacts": related_artifacts,
        }

    def _latest_attempt(self, work_item_id: str) -> WorkAttempt | None:
        return self.session.scalar(
            select(WorkAttempt)
            .where(WorkAttempt.work_item_id == work_item_id)
            .order_by(WorkAttempt.attempt_number.desc(), WorkAttempt.created_at.desc())
        )

    def _artifact_ref(self, artifact_id: str | None) -> dict[str, Any] | None:
        if not artifact_id:
            return None
        artifact = self.session.get(Artifact, artifact_id)
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

    def _referenced_message(self, referenced_discord_message_id: str | None) -> DiscordMessage | None:
        if not referenced_discord_message_id:
            return None
        return self.session.scalar(
            select(DiscordMessage).where(
                DiscordMessage.discord_message_id == referenced_discord_message_id,
            )
        )

    def _emit_event(
        self,
        *,
        event_type: str,
        entity_kind: str,
        entity_id: str,
        work_item_id: str | None = None,
        workflow_run_id: str | None = None,
        summary: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> WorkEvent:
        event = WorkEvent(
            event_type=event_type,
            entity_kind=entity_kind,
            entity_id=entity_id,
            work_item_id=work_item_id,
            workflow_run_id=workflow_run_id,
            source="discord",
            summary=summary,
            payload=payload or {},
        )
        self.session.add(event)
        self.session.flush()
        return event


def _reply_memory_config(context: dict[str, Any]) -> dict[str, Any] | None:
    value = context.get("reply_memory")
    if isinstance(value, dict):
        enabled = value.get("enabled", True)
        return dict(value) if enabled else None
    if context.get("reply_memory_enabled") is True:
        return {
            "namespace": _context_memory_namespace(context) or "global",
            "kind": context.get("reply_memory_kind") or "note",
            "tags": _string_list(context.get("reply_memory_tags")),
            "ttl_days": context.get("reply_memory_ttl_days"),
        }
    return None


def _general_intake_instruction(content: str) -> str:
    message = content.strip() or "(empty Discord message)"
    return (
        "# Discord Intake\n\n"
        "## Goal\n"
        "Handle this Discord message naturally as Tasque's general intake worker.\n\n"
        "## Context\n"
        "The user sent this message in the intake channel:\n\n"
        f"{message}\n\n"
        "Attached files, if any, are listed below as local artifact paths.\n\n"
        "## Instructions\n"
        "Decide what the user is asking for and use Tasque MCP tools when durable "
        "or operational state should change. Examples: use `schedule_create_work` "
        "for recurring or future work, `workflow_start` for an existing named "
        "workflow/chain that should run once, `work_enqueue` for follow-up work, "
        "`memory_create` or `memory_upsert_canonical` for durable notes, and "
        "`system_status` for status questions. Use `workflow_list` or "
        "`schedule_list` when the user names an existing chain/job but the exact "
        "id is unclear. For a broad open-ended coding/research request, answer "
        "directly or queue a normal focused WorkItem.\n\n"
        "If you start a workflow, schedule, memory update, or follow-up work, "
        "reply in natural language with what changed and what will happen next. Keep "
        "internal ids in `produces`; mention them in the user-facing report only when "
        "the user asked for ids or they are genuinely useful. Do not depend on fixed "
        "command phrases; infer the right Tasque action from the user's natural language.\n\n"
        "## Output\n"
        "Submit a concise summary and a useful Markdown report. The report will be "
        "posted directly back to the intake channel, so make it read like a normal "
        "assistant response rather than a queue/debug status."
    )

def _reply_followup_config(context: dict[str, Any]) -> dict[str, Any] | None:
    value = context.get("reply_followup_work") or context.get("reply_processor")
    if not isinstance(value, dict):
        return None
    if value.get("enabled", True) is False:
        return None
    return dict(value)


def _reply_followup_instruction(config: dict[str, Any]) -> str:
    template_path = config.get("task_template_path") or config.get("instruction_template_path")
    if template_path:
        base_dir = _optional_string(config.get("template_base_dir"))
        return read_template_file(
            str(template_path),
            base_dir=Path(base_dir) if base_dir else None,
        )
    return str(
        config.get("task_instruction")
        or config.get("instruction")
        or "Process this Discord thread reply and update any relevant Tasque memory."
    ).strip()


_MODEL_ROUTING_CONTRACT_KEYS = (
    "model",
    "model_profile",
    "native_worker_model",
    "native_worker_model_profile",
    "worker_model",
    "worker_model_profile",
    "tier",
)


def _reply_followup_runtime_contract(
    *,
    parent_contract: dict[str, Any],
    config_contract: dict[str, Any],
) -> dict[str, Any]:
    runtime_contract: dict[str, Any] = {}
    config_sets_model = any(key in config_contract for key in _MODEL_ROUTING_CONTRACT_KEYS)
    if not config_sets_model:
        for key in _MODEL_ROUTING_CONTRACT_KEYS:
            if key in parent_contract:
                runtime_contract[key] = parent_contract[key]
    runtime_contract.update(config_contract)
    return runtime_contract


def _context_memory_namespace(context: dict[str, Any]) -> str | None:
    value = context.get("memory_namespace")
    if isinstance(value, str) and value.strip():
        return value.strip()
    namespaces = context.get("memory_namespaces")
    if isinstance(namespaces, list):
        for namespace in namespaces:
            text = str(namespace).strip()
            if text:
                return text
    return None


def _string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        values = [value]
    elif isinstance(value, list):
        values = value
    else:
        return []
    return [str(item).strip() for item in values if str(item).strip()]


def _dedupe_strings(values: list[str]) -> list[str]:
    result: list[str] = []
    for value in values:
        if value not in result:
            result.append(value)
    return result


def _artifact_ref_list(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    refs: list[dict[str, Any]] = []
    for item in value:
        if isinstance(item, dict) and isinstance(item.get("artifact_id"), str):
            refs.append(dict(item))
    return refs


def _dedupe_artifact_refs(values: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for value in values:
        artifact_id = value.get("artifact_id")
        if not isinstance(artifact_id, str) or artifact_id in seen:
            continue
        seen.add(artifact_id)
        result.append(value)
    return result


def _attempt_context(attempt: WorkAttempt | None) -> dict[str, Any] | None:
    if attempt is None:
        return None
    return {
        "id": attempt.id,
        "attempt_number": attempt.attempt_number,
        "status": attempt.status,
        "summary": attempt.summary,
        "report_artifact_id": attempt.report_artifact_id,
        "produces": attempt.produces or {},
        "provider": attempt.provider,
        "error_type": attempt.error_type,
        "error_message": attempt.error_message,
    }


def _message_context(message: DiscordMessage) -> dict[str, Any]:
    return {
        "discord_message_id": message.discord_message_id,
        "discord_channel_id": message.discord_channel_id,
        "discord_thread_id": message.discord_thread_id,
        "direction": message.direction,
        "author": message.author,
        "content": message.content_preview,
        "work_item_id": message.work_item_id,
        "workflow_run_id": message.workflow_run_id,
        "created_at": message.created_at.isoformat(),
    }


def _optional_string(value: Any) -> str | None:
    if value is None or str(value).strip() == "":
        return None
    return str(value).strip()


def _optional_int(value: Any) -> int | None:
    if value is None or str(value).strip() == "":
        return None
    return int(value)
