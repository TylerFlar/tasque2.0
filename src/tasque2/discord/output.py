"""Outbound Discord: post finished work and workflow results, keep status panels and sticky notes current.

Posting state lives in the event log (``discord.*_posted`` events), so every post happens
once per status and survives restarts. Work that carries a thread Tasque already owns posts
into that thread; other work opens a new thread under the jobs channel (dead letters under
the DLQ channel); intake work answers in the intake channel. A thread's sticky note is posted
once, pinned, and edited in place; its message id, last rendering and pin state live on its row.
"""

from __future__ import annotations

import json
import logging
import mimetypes
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from tasque2.discord.gateway import MESSAGE_LIMIT, DiscordGateway, DiscordMessageGone, DiscordSentMessage
from tasque2.discord.routing import DiscordService
from tasque2.discord.ui import (
    CONTROL_PANEL_ENTITY_ID,
    CONTROL_PANEL_VERSION,
    build_ops_embed,
    build_sticky_embed,
    build_work_controls_view,
    build_workflow_controls_view,
    build_workflow_status_panel_embed,
)
from tasque2.discord.uploads import DiscordFileUpload
from tasque2.events import record_event
from tasque2.models import (
    Artifact,
    DiscordThread,
    ProviderRun,
    WorkAttempt,
    WorkEvent,
    WorkflowEdge,
    WorkflowNode,
    WorkflowRun,
    WorkItem,
    utc_now,
)
from tasque2.ops.status import get_system_status
from tasque2.sticky import StickyService

logger = logging.getLogger(__name__)

WORK_OUTPUT_STATUSES = {"succeeded", "dead_letter", "canceled"}
ACTIVE_RUN_STATUSES = {"active", "awaiting_input", "paused"}
TERMINAL_RUN_STATUSES = {"completed", "failed", "canceled"}
TEXT_SUFFIXES = {".md", ".txt", ".json", ".log"}
STICKY_PIN_RETRY = timedelta(minutes=10)


@dataclass(frozen=True)
class OutputChannels:
    ops: str
    jobs: str
    chains: str
    dlq: str


class DiscordOutputService:
    def __init__(self, session: Session) -> None:
        self.session = session

    def post_pending_updates(self, *, gateway: DiscordGateway, channels: OutputChannels, limit: int = 50) -> int:
        self.refresh_control_panel(channel_id=channels.ops, gateway=gateway)
        self.refresh_workflow_panels(channel_id=channels.chains, gateway=gateway, limit=limit)
        posted = 0
        for run in self._pending_workflow_runs(limit):
            if self.post_workflow_result(workflow_run_id=run.id, channel_id=channels.jobs, gateway=gateway):
                posted += 1
        for work_item in self._pending_work_items(max(limit - posted, 0)):
            channel_id = channels.dlq if work_item.status == "dead_letter" else channels.jobs
            self.post_work_result(work_item_id=work_item.id, channel_id=channel_id, gateway=gateway)
            posted += 1
        self.refresh_stickies(gateway=gateway)
        return posted

    def ensure_control_panel(self, *, channel_id: str, gateway: DiscordGateway) -> DiscordSentMessage | None:
        if self._control_panel_event(channel_id) is not None:
            return None
        embed = build_ops_embed(get_system_status(self.session))
        sent = gateway.send_embed(channel_id=channel_id, embed=embed)
        self._record_outbound(sent, content=str(embed.get("title") or "ops panel"))
        self._event(
            "discord.control_panel_posted",
            "discord",
            CONTROL_PANEL_ENTITY_ID,
            summary="Posted Discord ops panel",
            payload={
                "discord_channel_id": channel_id,
                "discord_message_id": sent.message_id,
                "panel_version": CONTROL_PANEL_VERSION,
                "signature": _signature(embed),
            },
        )
        return sent

    def refresh_control_panel(self, *, channel_id: str, gateway: DiscordGateway) -> bool:
        event = self._control_panel_event(channel_id)
        if event is None:
            return False
        payload = dict(event.payload or {})
        embed = build_ops_embed(get_system_status(self.session))
        signature = _signature(embed)
        if not payload.get("discord_message_id") or payload.get("signature") == signature:
            return False
        try:
            gateway.edit_message(channel_id=channel_id, message_id=str(payload["discord_message_id"]), embed=embed)
        except Exception:  # noqa: BLE001 - the stored signature is unchanged, so the next pass retries the edit
            return False
        event.payload = {**payload, "signature": signature}
        event.summary = "Updated Discord ops panel"
        return True

    def refresh_workflow_panels(self, *, channel_id: str, gateway: DiscordGateway, limit: int = 50) -> int:
        written = 0
        runs = self.session.scalars(
            select(WorkflowRun)
            .where(WorkflowRun.status.in_(ACTIVE_RUN_STATUSES | TERMINAL_RUN_STATUSES))
            .order_by(WorkflowRun.updated_at.desc())
            .limit(limit)
        ).all()
        for run in runs:
            event = self._latest_event("discord.workflow_status_panel_posted", run.id)
            if run.status in TERMINAL_RUN_STATUSES and event is None:
                continue
            embed = build_workflow_status_panel_embed(run, self._nodes(run.id))
            signature = _signature(embed)
            if event is None:
                view = build_workflow_controls_view(run)
                try:
                    sent = gateway.send_embed(channel_id=channel_id, embed=embed, view=view)
                except Exception:  # noqa: BLE001 - nothing is recorded, so the next pass posts it again
                    continue
                self._event(
                    "discord.workflow_status_panel_posted",
                    "workflow_run",
                    run.id,
                    workflow_run_id=run.id,
                    summary=f"Posted workflow status panel: {run.status}",
                    payload={
                        "discord_channel_id": channel_id,
                        "discord_message_id": sent.message_id,
                        "status": run.status,
                        "signature": signature,
                    },
                )
                written += 1
                continue
            payload = dict(event.payload or {})
            if (payload.get("signature") == signature and payload.get("status") == run.status) or not payload.get(
                "discord_message_id"
            ):
                continue
            panel_channel = str(payload.get("discord_channel_id") or channel_id)
            try:
                gateway.edit_message(
                    channel_id=panel_channel,
                    message_id=str(payload["discord_message_id"]),
                    embed=embed,
                    view=build_workflow_controls_view(run),
                )
            except Exception:  # noqa: BLE001 - the stored signature is unchanged, so the next pass retries the edit
                continue
            event.payload = {
                **payload,
                "discord_channel_id": panel_channel,
                "status": run.status,
                "signature": signature,
            }
            event.summary = f"Updated workflow status panel: {run.status}"
            written += 1
        return written

    def refresh_stickies(self, *, gateway: DiscordGateway, now: datetime | None = None) -> int:
        """Post each thread's sticky note once (silently), keep it pinned, and edit it in place when it changes.

        A sticky note whose message was deleted is turned off, so the user's delete keeps it gone. A pin
        Discord refuses is tried again every ``STICKY_PIN_RETRY``; a note pinned by hand counts as pinned.
        """
        now = now or utc_now()
        stickies = StickyService(self.session)
        written = 0
        refused: list[str] = []
        reason = ""
        for view in stickies.showable(now=now):
            embed = build_sticky_embed(view)
            signature = json.dumps(embed, sort_keys=True, default=str)
            sticky = stickies.sticky(view.thread_id)
            if sticky is None or not sticky.discord_message_id:
                try:
                    sent = gateway.send_embed(channel_id=view.thread_id, embed=embed, silent=True)
                except Exception:  # noqa: BLE001 - nothing is recorded, so the next pass posts it again
                    continue
                sticky = stickies.record_posted(view.thread_id, message_id=sent.message_id, signature=signature)
                written += 1
            elif sticky.signature != signature:
                try:
                    gateway.edit_message(channel_id=view.thread_id, message_id=sticky.discord_message_id, embed=embed)
                except DiscordMessageGone:
                    stickies.turn_off(view.thread_id)
                    continue
                except Exception:  # noqa: BLE001 - the stored signature is unchanged, so the next pass retries the edit
                    continue
                sticky.signature = signature
                written += 1
            if sticky.pinned_at is not None or (sticky.pin_retry_at is not None and sticky.pin_retry_at > now):
                continue
            try:
                gateway.pin_message(channel_id=view.thread_id, message_id=sticky.discord_message_id)
            except DiscordMessageGone:
                stickies.turn_off(view.thread_id)
                continue
            except Exception as exc:  # noqa: BLE001 - an unpinned sticky note still works; the pin is tried again
                if sticky.pin_retry_at is None:
                    refused.append(view.thread_id)
                    reason = str(exc)
                sticky.pin_retry_at = now + STICKY_PIN_RETRY
                continue
            sticky.pinned_at, sticky.pin_retry_at = now, None
        if refused:
            logger.warning(
                "Could not pin %d sticky note(s) (%s); a pin needs the bot's Pin Messages permission, and it is "
                "tried again every %d minutes. Threads: %s",
                len(refused),
                reason,
                STICKY_PIN_RETRY.total_seconds() // 60,
                ", ".join(refused),
            )
        return written

    def post_work_result(
        self, *, work_item_id: str, channel_id: str, gateway: DiscordGateway
    ) -> DiscordSentMessage | None:
        work_item = self.session.get(WorkItem, work_item_id)
        if work_item is None:
            raise KeyError(f"Unknown work item: {work_item_id}")
        if self._already_posted("discord.work_status_posted", work_item.id, work_item.status):
            return None
        attempt = self._latest_attempt(work_item.id)
        if work_item.status == "succeeded" and attempt is not None and (attempt.produces or {}).get("silent"):
            self._event(
                "discord.work_status_posted",
                "work_item",
                work_item.id,
                work_item_id=work_item.id,
                workflow_run_id=work_item.workflow_run_id,
                summary="Suppressed output of a silent run",
                payload={"status": work_item.status, "silent": True},
            )
            return None
        intake_channel = _intake_channel(work_item)
        if intake_channel is not None:
            return self._post_intake_answer(work_item, attempt, intake_channel, gateway)

        thread = self.ensure_work_thread(work_item=work_item, channel_id=channel_id, gateway=gateway)
        uploads = self._work_uploads(work_item, attempt)
        chunks = split_markdown(
            _with_attachment_note(self._work_content(work_item, attempt), uploads), MESSAGE_LIMIT
        ) or ["Done."]
        sent_messages: list[DiscordSentMessage] = []
        for index, content in enumerate(chunks):
            last = index == len(chunks) - 1
            sent = gateway.send_message(
                channel_id=thread.discord_thread_id,
                content=content,
                view=build_work_controls_view(work_item) if last else None,
                attachments=uploads if last else None,
            )
            sent_messages.append(sent)
            self._record_outbound(
                sent,
                content=content,
                thread_id=thread.discord_thread_id,
                work_item_id=work_item.id,
                workflow_run_id=work_item.workflow_run_id,
            )
        self._event(
            "discord.work_status_posted",
            "work_item",
            work_item.id,
            work_item_id=work_item.id,
            workflow_run_id=work_item.workflow_run_id,
            summary=f"Posted work status to Discord: {work_item.status}",
            payload={
                "status": work_item.status,
                "discord_message_id": sent_messages[-1].message_id,
                "discord_message_ids": [message.message_id for message in sent_messages],
                "upload_artifact_ids": [upload.artifact_id for upload in uploads if upload.artifact_id],
            },
        )
        return sent_messages[-1]

    def post_workflow_result(
        self, *, workflow_run_id: str, channel_id: str, gateway: DiscordGateway
    ) -> DiscordSentMessage | None:
        run = self.session.get(WorkflowRun, workflow_run_id)
        if run is None:
            raise KeyError(f"Unknown workflow run: {workflow_run_id}")
        if run.status not in TERMINAL_RUN_STATUSES or self._already_posted(
            "discord.workflow_final_posted", run.id, run.status
        ):
            return None
        finals = self._final_attempts(run)
        silent = all((attempt.produces or {}).get("silent") for _, attempt in finals)
        if run.status == "completed" and finals and silent:
            self._event(
                "discord.workflow_final_posted",
                "workflow_run",
                run.id,
                workflow_run_id=run.id,
                summary="Suppressed output of a silent workflow run",
                payload={"status": run.status, "silent": True},
            )
            return None
        thread = self.ensure_workflow_thread(run=run, channel_id=channel_id, gateway=gateway)
        uploads = self._uploads(
            [
                artifact_id
                for _node, attempt in finals
                for artifact_id in _string_list((attempt.produces or {}).get("discord_upload_artifact_ids"))
            ]
            + self._tagged_upload_ids(Artifact.workflow_run_id == run.id)
        )
        chunks = split_markdown(_with_attachment_note(self._workflow_content(run, finals), uploads), MESSAGE_LIMIT) or [
            "Done."
        ]
        sent_messages: list[DiscordSentMessage] = []
        for index, content in enumerate(chunks):
            last = index == len(chunks) - 1
            sent = gateway.send_message(
                channel_id=thread.discord_thread_id,
                content=content,
                view=build_workflow_controls_view(run) if last else None,
                attachments=uploads if last else None,
            )
            sent_messages.append(sent)
            self._record_outbound(sent, content=content, thread_id=thread.discord_thread_id, workflow_run_id=run.id)
        self._event(
            "discord.workflow_final_posted",
            "workflow_run",
            run.id,
            workflow_run_id=run.id,
            summary=f"Posted workflow result to Discord: {run.status}",
            payload={
                "status": run.status,
                "discord_message_id": sent_messages[-1].message_id,
                "discord_message_ids": [message.message_id for message in sent_messages],
                "upload_artifact_ids": [upload.artifact_id for upload in uploads if upload.artifact_id],
            },
        )
        return sent_messages[-1]

    def ensure_work_thread(self, *, work_item: WorkItem, channel_id: str, gateway: DiscordGateway) -> DiscordThread:
        """The thread this work posts into.

        Work carrying the id of a thread Tasque owns posts into that thread without taking
        over its binding, so replies keep routing to the thread's original owner. Work with
        no thread id, or with ``context.discord_new_thread``, opens a new thread.
        """
        existing = self.session.scalar(
            select(DiscordThread).where(DiscordThread.purpose == "work", DiscordThread.work_item_id == work_item.id)
        )
        if existing is not None:
            return existing
        if work_item.discord_thread_id and not (work_item.context or {}).get("discord_new_thread"):
            bound = self.session.scalar(
                select(DiscordThread).where(DiscordThread.discord_thread_id == work_item.discord_thread_id)
            )
            if bound is not None:
                return bound
        embed = self._work_embed(work_item)
        ref = gateway.create_thread(
            parent_channel_id=channel_id,
            name=_thread_name("work", work_item.id, work_item.title),
            initial_message="",
            initial_embed=embed,
        )
        thread = DiscordService(self.session).bind_thread(
            purpose="work", discord_channel_id=channel_id, discord_thread_id=ref.thread_id, work_item_id=work_item.id
        )
        if ref.starter_message_id is not None:
            self._record_outbound(
                DiscordSentMessage(ref.starter_message_id, channel_id),
                content=_embed_preview(embed),
                work_item_id=work_item.id,
                workflow_run_id=work_item.workflow_run_id,
            )
        self._event(
            "discord.thread_created",
            "work_item",
            work_item.id,
            work_item_id=work_item.id,
            workflow_run_id=work_item.workflow_run_id,
            summary="Created Discord work thread",
            payload={"discord_thread_id": ref.thread_id},
        )
        return thread

    def ensure_workflow_thread(self, *, run: WorkflowRun, channel_id: str, gateway: DiscordGateway) -> DiscordThread:
        existing = self.session.scalar(
            select(DiscordThread).where(DiscordThread.purpose == "workflow", DiscordThread.workflow_run_id == run.id)
        )
        if existing is not None:
            run.discord_thread_id = existing.discord_thread_id
            return existing
        if run.discord_thread_id:
            bound = self.session.scalar(
                select(DiscordThread).where(DiscordThread.discord_thread_id == run.discord_thread_id)
            )
            if bound is not None:
                return bound
        embed = self._workflow_embed(run)
        ref = gateway.create_thread(
            parent_channel_id=channel_id,
            name=_thread_name("workflow", run.id, run.name),
            initial_message="",
            initial_embed=embed,
        )
        thread = DiscordService(self.session).bind_thread(
            purpose="workflow", discord_channel_id=channel_id, discord_thread_id=ref.thread_id, workflow_run_id=run.id
        )
        run.discord_thread_id = thread.discord_thread_id
        if ref.starter_message_id is not None:
            self._record_outbound(
                DiscordSentMessage(ref.starter_message_id, channel_id),
                content=_embed_preview(embed),
                workflow_run_id=run.id,
            )
        self._event(
            "discord.thread_created",
            "workflow_run",
            run.id,
            workflow_run_id=run.id,
            summary="Created Discord workflow thread",
            payload={"discord_thread_id": ref.thread_id},
        )
        return thread

    def _post_intake_answer(
        self, work_item: WorkItem, attempt: WorkAttempt | None, channel_id: str, gateway: DiscordGateway
    ) -> DiscordSentMessage:
        uploads = self._work_uploads(work_item, attempt)
        if work_item.status == "succeeded":
            content = self._report_text(attempt) or (attempt.summary if attempt and attempt.summary else "Done.")
        elif work_item.status == "canceled":
            content = "Canceled."
        elif attempt is not None and attempt.error_message:
            content = f"I hit an error while handling that: {attempt.error_message}"
        else:
            content = f"I could not complete that request. Status: {work_item.status}"
        if uploads:
            content += "\n\nAttached: " + ", ".join(upload.display_name for upload in uploads[:5])
        content = _truncate(content, MESSAGE_LIMIT)
        sent = gateway.send_message(channel_id=channel_id, content=content, attachments=uploads)
        self._record_outbound(
            sent, content=content, work_item_id=work_item.id, workflow_run_id=work_item.workflow_run_id
        )
        self._event(
            "discord.work_status_posted",
            "work_item",
            work_item.id,
            work_item_id=work_item.id,
            workflow_run_id=work_item.workflow_run_id,
            summary=f"Posted intake response to Discord: {work_item.status}",
            payload={
                "mode": "intake_response",
                "status": work_item.status,
                "discord_message_id": sent.message_id,
                "discord_channel_id": sent.channel_id,
            },
        )
        return sent

    def _pending_workflow_runs(self, limit: int) -> list[WorkflowRun]:
        if limit <= 0:
            return []
        candidates = self.session.scalars(
            select(WorkflowRun)
            .where(WorkflowRun.status.in_(TERMINAL_RUN_STATUSES))
            .order_by(WorkflowRun.updated_at.desc())
            .limit(limit * 4)
        ).all()
        return [
            run
            for run in candidates
            if not self._already_posted("discord.workflow_final_posted", run.id, run.status)
            and not self._hidden_cancel(run)
        ][:limit]

    def _hidden_cancel(self, run: WorkflowRun) -> bool:
        """A canceled run whose work items are all hidden posts nothing."""
        if run.status != "canceled":
            return False
        visible = self.session.scalar(
            select(func.count())
            .select_from(WorkItem)
            .where(WorkItem.workflow_run_id == run.id, WorkItem.visible.is_(True))
        )
        return not visible

    def _pending_work_items(self, limit: int) -> list[WorkItem]:
        if limit <= 0:
            return []
        candidates = self.session.scalars(
            select(WorkItem)
            .where(
                WorkItem.status.in_(WORK_OUTPUT_STATUSES),
                WorkItem.visible.is_(True),
                WorkItem.workflow_run_id.is_(None),
            )
            .order_by(WorkItem.updated_at.desc())
            .limit(limit * 4)
        ).all()
        return [
            work_item
            for work_item in candidates
            if not self._already_posted("discord.work_status_posted", work_item.id, work_item.status)
        ][:limit]

    def _already_posted(self, event_type: str, entity_id: str, status: str) -> bool:
        events = self.session.scalars(
            select(WorkEvent).where(WorkEvent.event_type == event_type, WorkEvent.entity_id == entity_id)
        ).all()
        return any((event.payload or {}).get("status") == status for event in events)

    def _latest_event(self, event_type: str, entity_id: str) -> WorkEvent | None:
        return self.session.scalar(
            select(WorkEvent)
            .where(WorkEvent.event_type == event_type, WorkEvent.entity_id == entity_id)
            .order_by(WorkEvent.created_at.desc(), WorkEvent.id.desc())
            .limit(1)
        )

    def _control_panel_event(self, channel_id: str) -> WorkEvent | None:
        events = self.session.scalars(
            select(WorkEvent).where(
                WorkEvent.event_type == "discord.control_panel_posted", WorkEvent.entity_id == CONTROL_PANEL_ENTITY_ID
            )
        ).all()
        for event in reversed(events):
            payload = event.payload or {}
            if (
                payload.get("discord_channel_id") == channel_id
                and payload.get("panel_version") == CONTROL_PANEL_VERSION
            ):
                return event
        return None

    def _latest_attempt(self, work_item_id: str) -> WorkAttempt | None:
        return self.session.scalar(
            select(WorkAttempt)
            .where(WorkAttempt.work_item_id == work_item_id)
            .order_by(WorkAttempt.attempt_number.desc())
        )

    def _work_content(self, work_item: WorkItem, attempt: WorkAttempt | None) -> str:
        if work_item.status == "succeeded":
            return self._report_text(attempt, full=True) or (
                attempt.summary if attempt and attempt.summary else "Done."
            )
        if work_item.status == "canceled":
            return "Canceled."
        lines = [f"Work update: {work_item.title}", f"Status: {work_item.status}", f"Worker: {work_item.worker_kind}"]
        if attempt is not None and attempt.summary:
            lines.append(f"Summary: {attempt.summary}")
        if attempt is not None and attempt.error_message:
            lines.append(f"Error: {attempt.error_message}")
        if attempt is not None and work_item.status == "dead_letter":
            run = self.session.get(ProviderRun, attempt.provider_run_id) if attempt.provider_run_id else None
            if run is not None:
                lines.append("")
                lines.append(f"Provider run: {run.provider} `{run.id}` ({run.status})")
                for artifact in self._run_log_artifacts(run):
                    lines.append(f"- {artifact.title}: `{artifact.id}`")
        return _truncate("\n".join(lines), MESSAGE_LIMIT)

    def _workflow_content(self, run: WorkflowRun, finals: list[tuple[WorkflowNode, WorkAttempt]]) -> str:
        if run.status == "canceled":
            return "Canceled."
        if run.status == "completed":
            sections = []
            for node, attempt in finals:
                content = self._report_text(attempt, full=True) or attempt.summary or "Done."
                sections.append(content if len(finals) == 1 else f"## {node.node_key}\n\n{content}")
            return "\n\n".join(section.strip() for section in sections if section.strip()) or "Done."
        nodes = self._nodes(run.id)
        lines = [
            f"Workflow ended: {run.name}",
            f"Status: {run.status}",
            f"Run ID: {run.id}",
            f"Nodes: {_counts(nodes)}",
        ]
        failed = [node for node in nodes if node.status in {"failed", "canceled"}]
        if failed:
            lines += ["", "Failed or canceled steps:"]
            lines += [f"- {node.node_key}: {node.failure_reason or node.status}" for node in failed[:5]]
        if finals:
            lines += ["", "Final output:"]
            lines += [f"- {node.node_key}: {attempt.summary}" for node, attempt in finals[:3] if attempt.summary]
        return _truncate("\n".join(lines), MESSAGE_LIMIT)

    def _final_attempts(self, run: WorkflowRun) -> list[tuple[WorkflowNode, WorkAttempt]]:
        nodes = [node for node in self._nodes(run.id) if node.work_item_id is not None and node.kind == "work"]
        if not nodes:
            return []
        upstream = {
            edge.from_node_id
            for edge in self.session.scalars(select(WorkflowEdge).where(WorkflowEdge.workflow_run_id == run.id)).all()
        }
        leaves = [node for node in nodes if node.id not in upstream] or nodes[-3:]
        finals = []
        for node in leaves:
            attempt = self._latest_attempt(node.work_item_id)
            if attempt is not None:
                finals.append((node, attempt))
        return finals

    def _work_uploads(self, work_item: WorkItem, attempt: WorkAttempt | None) -> list[DiscordFileUpload]:
        ids: list[str] = []
        if attempt is not None:
            ids += _string_list((attempt.produces or {}).get("discord_upload_artifact_ids"))
            if work_item.status == "dead_letter" and attempt.provider_run_id:
                run = self.session.get(ProviderRun, attempt.provider_run_id)
                if run is not None:
                    ids += [
                        artifact.id
                        for artifact in self._run_log_artifacts(run)
                        if "stream" not in (artifact.tags or [])
                    ]
        ids += self._tagged_upload_ids(Artifact.work_item_id == work_item.id)
        return self._uploads(ids)

    def _run_log_artifacts(self, run: ProviderRun) -> list[Artifact]:
        return list(
            self.session.scalars(
                select(Artifact)
                .where(
                    Artifact.source_kind == "provider_run", Artifact.source_id == run.id, Artifact.archived_at.is_(None)
                )
                .order_by(Artifact.created_at)
            ).all()
        )

    def _tagged_upload_ids(self, clause) -> list[str]:
        return [
            artifact.id
            for artifact in self.session.scalars(select(Artifact).where(clause, Artifact.archived_at.is_(None))).all()
            if "discord_upload" in (artifact.tags or [])
        ]

    def _uploads(self, artifact_ids: Sequence[str]) -> list[DiscordFileUpload]:
        uploads: list[DiscordFileUpload] = []
        for artifact_id in dict.fromkeys(artifact_ids):
            artifact = self.session.get(Artifact, artifact_id)
            if artifact is None:
                continue
            path = Path(artifact.local_path)
            if not path.is_file():
                continue
            filename = Path(str(artifact.title)).name if artifact.title else path.name
            if path.suffix and not _names_file_type(filename, path.name):
                filename = f"{filename}{path.suffix}"
            uploads.append(DiscordFileUpload(path=str(path), filename=filename or path.name, artifact_id=artifact.id))
        return uploads

    def _report_text(self, attempt: WorkAttempt | None, *, full: bool = False) -> str:
        if attempt is None or not attempt.report_artifact_id:
            return ""
        artifact = self.session.get(Artifact, attempt.report_artifact_id)
        if artifact is None:
            return ""
        path = Path(artifact.local_path)
        if not path.is_file() or path.suffix.lower() not in TEXT_SUFFIXES:
            return ""
        try:
            text = path.read_text(encoding="utf-8", errors="replace").strip()
        except OSError:
            return ""
        return text if full else _truncate(text, 1800)

    def _work_embed(self, work_item: WorkItem) -> dict[str, Any]:
        attempt = self._latest_attempt(work_item.id)
        if work_item.status == "succeeded":
            description = (
                (attempt.summary if attempt and attempt.summary else "")
                or _one_line(self._report_text(attempt), 500)
                or "Done."
            )
        elif work_item.status == "canceled":
            description = "Canceled."
        elif attempt is not None and (attempt.error_message or attempt.summary):
            description = attempt.error_message or attempt.summary or ""
        else:
            description = f"Status: {work_item.status}"
        return {
            "title": _truncate(f"Work: {work_item.title}", 256),
            "description": _truncate(description, 900),
            "color": _status_color(work_item.status),
            "fields": [
                {"name": "Status", "value": work_item.status, "inline": True},
                {"name": "Worker", "value": work_item.worker_kind, "inline": True},
            ],
        }

    def _workflow_embed(self, run: WorkflowRun) -> dict[str, Any]:
        finals = self._final_attempts(run)
        if run.status == "completed":
            description = next((attempt.summary for _node, attempt in finals if attempt.summary), "") or "Done."
        elif run.status == "canceled":
            description = "Canceled."
        else:
            failed = [node for node in self._nodes(run.id) if node.status in {"failed", "canceled"}]
            description = (
                f"{failed[0].node_key}: {failed[0].failure_reason or failed[0].status}"
                if failed
                else f"Status: {run.status}"
            )
        return {
            "title": _truncate(f"Workflow: {run.name}", 256),
            "description": _truncate(description, 900),
            "color": _status_color(run.status),
            "fields": [
                {"name": "Status", "value": run.status, "inline": True},
                {"name": "Run ID", "value": run.id, "inline": False},
            ],
        }

    def _nodes(self, workflow_run_id: str) -> list[WorkflowNode]:
        return list(
            self.session.scalars(
                select(WorkflowNode)
                .where(WorkflowNode.workflow_run_id == workflow_run_id)
                .order_by(WorkflowNode.created_at)
            ).all()
        )

    def _record_outbound(
        self,
        sent: DiscordSentMessage,
        *,
        content: str,
        thread_id: str | None = None,
        work_item_id: str | None = None,
        workflow_run_id: str | None = None,
    ) -> None:
        DiscordService(self.session).record_message(
            discord_message_id=sent.message_id,
            discord_channel_id=sent.channel_id,
            discord_thread_id=thread_id,
            direction="outbound",
            author="tasque",
            content_preview=content,
            work_item_id=work_item_id,
            workflow_run_id=workflow_run_id,
        )

    def _event(
        self,
        event_type: str,
        entity_kind: str,
        entity_id: str,
        *,
        summary: str,
        work_item_id: str | None = None,
        workflow_run_id: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        record_event(
            self.session,
            event_type=event_type,
            entity_kind=entity_kind,
            entity_id=entity_id,
            work_item_id=work_item_id,
            workflow_run_id=workflow_run_id,
            source="discord",
            summary=summary,
            payload=payload,
        )


def split_markdown(content: str, limit: int) -> list[str]:
    """Split on line boundaries into chunks of at most ``limit`` characters."""
    stripped = content.strip()
    if not stripped:
        return []
    chunks: list[str] = []
    current = ""
    for line in stripped.splitlines(keepends=True):
        if len(line) > limit:
            if current.strip():
                chunks.append(current.rstrip())
                current = ""
            chunks.extend(
                chunk for start in range(0, len(line), limit) if (chunk := line[start : start + limit].rstrip())
            )
            continue
        if len(current) + len(line) > limit and current.strip():
            chunks.append(current.rstrip())
            current = line
            continue
        current += line
    if current.strip():
        chunks.append(current.rstrip())
    return chunks


def _intake_channel(work_item: WorkItem) -> str | None:
    if work_item.source_kind != "discord":
        return None
    intake = (work_item.context or {}).get("discord_intake")
    channel_id = intake.get("discord_channel_id") if isinstance(intake, dict) else None
    return str(channel_id).strip() if channel_id else None


def _with_attachment_note(content: str, uploads: list[DiscordFileUpload]) -> str:
    if not uploads:
        return content
    names = ", ".join(upload.display_name for upload in uploads[:5])
    more = f", and {len(uploads) - 5} more" if len(uploads) > 5 else ""
    note = f"Attached files: {names}{more}"
    return f"{content.strip()}\n\n{note}" if content.strip() else note


def _names_file_type(filename: str, stored_name: str) -> bool:
    """Whether ``filename`` already carries the stored file's type, so no extension is appended."""
    guessed = mimetypes.guess_type(filename)[0]
    if guessed is None:
        return Path(filename).suffix.lower() == Path(stored_name).suffix.lower()
    return guessed == mimetypes.guess_type(stored_name)[0]


def _thread_name(purpose: str, entity_id: str, title: str) -> str:
    return f"{purpose}-{entity_id[:8]} {' '.join(title.split())[:72]}"[:100]


def _embed_preview(embed: dict[str, Any]) -> str:
    return _truncate(
        "\n".join(part for part in (str(embed.get("title") or ""), str(embed.get("description") or "")) if part),
        MESSAGE_LIMIT,
    )


def _counts(nodes: Sequence[WorkflowNode]) -> str:
    counts: dict[str, int] = {}
    for node in nodes:
        counts[node.status] = counts.get(node.status, 0) + 1
    return ", ".join(f"{key}={value}" for key, value in sorted(counts.items())) or "(none)"


def _status_color(status: str) -> int:
    if status in {"succeeded", "completed"}:
        return 0x57F287
    if status in {"dead_letter", "failed"}:
        return 0xED4245
    if status == "canceled":
        return 0x747F8D
    return 0x5865F2


def _signature(embed: dict[str, Any]) -> str:
    return json.dumps(
        {key: embed.get(key) for key in ("title", "description", "color", "fields")}, sort_keys=True, default=str
    )


def _one_line(content: str, limit: int) -> str:
    compact = " ".join(content.split())
    return compact if len(compact) <= limit else compact[: limit - 20] + " [truncated]"


def _truncate(content: str, limit: int) -> str:
    return content if len(content) <= limit else content[: limit - 20] + "\n[truncated]"


def _string_list(value: object) -> list[str]:
    if isinstance(value, str):
        return [value]
    return [str(item) for item in value if item] if isinstance(value, list) else []
