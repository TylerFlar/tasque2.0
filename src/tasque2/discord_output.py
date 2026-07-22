from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from tasque2.discord_adapter import DiscordService
from tasque2.discord_ui import (
    CONTROL_PANEL_ENTITY_ID,
    CONTROL_PANEL_VERSION,
    build_control_panel_view,
    build_ops_embed,
    build_work_controls_view,
    build_workflow_controls_view,
    build_workflow_status_panel_embed,
)
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
)
from tasque2.status import get_system_status

WORK_OUTPUT_STATUSES = {"succeeded", "dead_letter", "canceled"}
ACTIVE_WORKFLOW_PANEL_STATUSES = {"active", "awaiting_input", "paused"}
TERMINAL_WORKFLOW_PANEL_STATUSES = {"completed", "failed", "canceled"}
WORKFLOW_WORK_EVENT_TYPES = {
    "work.cancel_requested",
    "work.canceled",
    "work.claimed",
    "work.dead_lettered",
    "work.failed",
    "work.paused",
    "work.resumed",
    "work.retry_scheduled",
    "work.succeeded",
}
DISCORD_MESSAGE_LIMIT = 1900


@dataclass(frozen=True)
class DiscordThreadRef:
    thread_id: str
    starter_message_id: str | None = None


@dataclass(frozen=True)
class DiscordSentMessage:
    message_id: str
    channel_id: str


@dataclass(frozen=True)
class DiscordFileUpload:
    path: str
    filename: str | None = None
    artifact_id: str | None = None


class DiscordOutputGateway(Protocol):
    async def create_thread(
        self,
        *,
        parent_channel_id: str,
        name: str,
        initial_message: str,
        initial_embed: dict[str, Any] | None = None,
    ) -> DiscordThreadRef:
        ...

    async def send_message(
        self,
        *,
        channel_id: str,
        content: str,
        view: object | None = None,
        attachments: Sequence[DiscordFileUpload] | None = None,
    ) -> DiscordSentMessage:
        ...

    async def send_embed(
        self,
        *,
        channel_id: str,
        embed: dict[str, Any],
        view: object | None = None,
    ) -> DiscordSentMessage:
        ...

    async def edit_message(
        self,
        *,
        channel_id: str,
        message_id: str,
        content: str | None = None,
        embed: dict[str, Any] | None = None,
        view: object | None = None,
    ) -> None:
        ...


class FakeDiscordOutputGateway:
    def __init__(self) -> None:
        self.created_threads: list[tuple[str, str, str]] = []
        self.created_thread_embeds: list[dict[str, Any] | None] = []
        self.sent_messages: list[tuple[str, str]] = []
        self.sent_embeds: list[tuple[str, dict[str, Any], object | None]] = []
        self.edited_messages: list[tuple[str, str, str | None, dict[str, Any] | None, object | None]] = []
        self.sent_views: list[object | None] = []
        self.sent_attachments: list[list[DiscordFileUpload]] = []
        self._thread_counter = 0
        self._message_counter = 0

    async def create_thread(
        self,
        *,
        parent_channel_id: str,
        name: str,
        initial_message: str,
        initial_embed: dict[str, Any] | None = None,
    ) -> DiscordThreadRef:
        self._thread_counter += 1
        self._message_counter += 1
        thread_id = f"fake-thread-{self._thread_counter}"
        message_id = f"fake-message-{self._message_counter}"
        self.created_threads.append((parent_channel_id, name, initial_message))
        self.created_thread_embeds.append(initial_embed)
        return DiscordThreadRef(thread_id=thread_id, starter_message_id=message_id)

    async def send_message(
        self,
        *,
        channel_id: str,
        content: str,
        view: object | None = None,
        attachments: Sequence[DiscordFileUpload] | None = None,
    ) -> DiscordSentMessage:
        self._message_counter += 1
        message_id = f"fake-message-{self._message_counter}"
        self.sent_messages.append((channel_id, content))
        self.sent_views.append(view)
        self.sent_attachments.append(list(attachments or []))
        return DiscordSentMessage(message_id=message_id, channel_id=channel_id)

    async def send_embed(
        self,
        *,
        channel_id: str,
        embed: dict[str, Any],
        view: object | None = None,
    ) -> DiscordSentMessage:
        self._message_counter += 1
        message_id = f"fake-message-{self._message_counter}"
        self.sent_embeds.append((channel_id, embed, view))
        self.sent_views.append(view)
        self.sent_attachments.append([])
        return DiscordSentMessage(message_id=message_id, channel_id=channel_id)

    async def edit_message(
        self,
        *,
        channel_id: str,
        message_id: str,
        content: str | None = None,
        embed: dict[str, Any] | None = None,
        view: object | None = None,
    ) -> None:
        self.edited_messages.append((channel_id, message_id, content, embed, view))


class DiscordPyOutputGateway:
    def __init__(self, client) -> None:
        self.client = client

    async def create_thread(
        self,
        *,
        parent_channel_id: str,
        name: str,
        initial_message: str,
        initial_embed: dict[str, Any] | None = None,
    ) -> DiscordThreadRef:
        import discord

        channel = self.client.get_channel(int(parent_channel_id))
        if channel is None:
            channel = await self.client.fetch_channel(int(parent_channel_id))
        if not isinstance(channel, discord.TextChannel):
            raise TypeError("Discord output channel must be a text channel in this version.")
        kwargs: dict[str, Any] = {}
        if initial_message.strip():
            kwargs["content"] = initial_message[:DISCORD_MESSAGE_LIMIT]
        if initial_embed is not None:
            kwargs["embed"] = discord.Embed.from_dict(initial_embed)
        if not kwargs:
            kwargs["content"] = "Started."
        message = await channel.send(**kwargs)
        thread = await message.create_thread(
            name=name[:100],
            auto_archive_duration=10080,
        )
        return DiscordThreadRef(thread_id=str(thread.id), starter_message_id=str(message.id))

    async def send_message(
        self,
        *,
        channel_id: str,
        content: str,
        view: object | None = None,
        attachments: Sequence[DiscordFileUpload] | None = None,
    ) -> DiscordSentMessage:
        import discord

        channel = self.client.get_channel(int(channel_id))
        if channel is None:
            channel = await self.client.fetch_channel(int(channel_id))
        files = [
            discord.File(upload.path, filename=upload.filename)
            for upload in list(attachments or [])[:10]
        ]
        message = await channel.send(content[:1900], view=view, files=files or None)
        return DiscordSentMessage(message_id=str(message.id), channel_id=str(channel.id))

    async def send_embed(
        self,
        *,
        channel_id: str,
        embed: dict[str, Any],
        view: object | None = None,
    ) -> DiscordSentMessage:
        import discord

        channel = self.client.get_channel(int(channel_id))
        if channel is None:
            channel = await self.client.fetch_channel(int(channel_id))
        message = await channel.send(embed=discord.Embed.from_dict(embed), view=view)
        return DiscordSentMessage(message_id=str(message.id), channel_id=str(channel.id))

    async def edit_message(
        self,
        *,
        channel_id: str,
        message_id: str,
        content: str | None = None,
        embed: dict[str, Any] | None = None,
        view: object | None = None,
    ) -> None:
        import discord

        channel = self.client.get_channel(int(channel_id))
        if channel is None:
            channel = await self.client.fetch_channel(int(channel_id))
        message = await channel.fetch_message(int(message_id))
        kwargs: dict[str, Any] = {"view": view}
        if content is not None:
            kwargs["content"] = content
        if embed is not None:
            kwargs["embed"] = discord.Embed.from_dict(embed)
        await message.edit(**kwargs)


class DiscordOutputService:
    def __init__(self, session: Session) -> None:
        self.session = session

    async def post_pending_updates(
        self,
        *,
        parent_channel_id: str,
        gateway: DiscordOutputGateway,
        limit: int = 50,
        ops_channel_id: str | None = None,
        jobs_channel_id: str | None = None,
        chains_channel_id: str | None = None,
        dlq_channel_id: str | None = None,
    ) -> int:
        ops_channel = _required_channel("ops_channel_id", ops_channel_id)
        jobs_channel = _required_channel("jobs_channel_id", jobs_channel_id)
        chains_channel = _required_channel("chains_channel_id", chains_channel_id)
        dlq_channel = _required_channel("dlq_channel_id", dlq_channel_id)

        await self.refresh_control_panel(
            parent_channel_id=ops_channel,
            gateway=gateway,
        )
        await self.refresh_workflow_status_panels(
            parent_channel_id=chains_channel,
            gateway=gateway,
            limit=limit,
        )

        posted = 0
        for workflow_run in self._pending_workflow_runs(limit):
            sent = await self.post_workflow_status(
                workflow_run_id=workflow_run.id,
                parent_channel_id=jobs_channel,
                gateway=gateway,
            )
            if sent is not None:
                posted += 1

        remaining = max(limit - posted, 0)
        for work_item in self._pending_work_items(remaining):
            await self.post_work_status(
                work_item_id=work_item.id,
                parent_channel_id=dlq_channel if work_item.status == "dead_letter" else jobs_channel,
                gateway=gateway,
            )
            posted += 1

        return posted

    async def ensure_control_panel(
        self,
        *,
        parent_channel_id: str,
        gateway: DiscordOutputGateway,
    ) -> DiscordSentMessage | None:
        if self._control_panel_message_id(parent_channel_id) is not None:
            return None

        embed = build_ops_embed(get_system_status(self.session))
        signature = _embed_signature(embed)
        sent = await gateway.send_embed(
            channel_id=parent_channel_id,
            embed=embed,
            view=build_control_panel_view(),
        )
        DiscordService(self.session).record_message(
            discord_message_id=sent.message_id,
            discord_channel_id=sent.channel_id,
            discord_thread_id=None,
            direction="outbound",
            author="tasque",
            content_preview=str(embed.get("title") or "tasque ops panel"),
        )
        self._emit_event(
            event_type="discord.control_panel_posted",
            entity_kind="discord",
            entity_id=CONTROL_PANEL_ENTITY_ID,
            summary="Posted Discord ops panel",
            payload={
                "discord_channel_id": parent_channel_id,
                "discord_message_id": sent.message_id,
                "panel_version": CONTROL_PANEL_VERSION,
                "signature": signature,
            },
        )
        return sent

    async def refresh_control_panel(
        self,
        *,
        parent_channel_id: str,
        gateway: DiscordOutputGateway,
    ) -> DiscordSentMessage | None:
        panel_event = self._control_panel_event(parent_channel_id)
        if panel_event is None:
            return None
        payload = dict(panel_event.payload or {})
        message_id = payload.get("discord_message_id")
        if not message_id:
            return None

        embed = build_ops_embed(get_system_status(self.session))
        signature = _embed_signature(embed)
        if payload.get("signature") == signature:
            return None

        try:
            await gateway.edit_message(
                channel_id=parent_channel_id,
                message_id=str(message_id),
                embed=embed,
                view=build_control_panel_view(),
            )
        except Exception:
            return None
        panel_event.payload = {
            **payload,
            "discord_channel_id": parent_channel_id,
            "discord_message_id": str(message_id),
            "panel_version": CONTROL_PANEL_VERSION,
            "signature": signature,
        }
        panel_event.summary = "Updated Discord ops panel"
        return DiscordSentMessage(message_id=str(message_id), channel_id=parent_channel_id)

    async def refresh_workflow_status_panels(
        self,
        *,
        parent_channel_id: str,
        gateway: DiscordOutputGateway,
        limit: int = 50,
    ) -> int:
        written = 0
        runs = self._workflow_panel_candidates(limit)
        for run in runs:
            panel_event = self._workflow_panel_event(run.id)
            if run.status in TERMINAL_WORKFLOW_PANEL_STATUSES and panel_event is None:
                continue
            nodes = self._workflow_nodes(run.id)
            embed = build_workflow_status_panel_embed(run, nodes)
            signature = _embed_signature(embed)
            if panel_event is None:
                sent = await gateway.send_embed(
                    channel_id=parent_channel_id,
                    embed=embed,
                    view=build_workflow_controls_view(run),
                )
                self._emit_event(
                    event_type="discord.workflow_status_panel_posted",
                    entity_kind="workflow_run",
                    entity_id=run.id,
                    workflow_run_id=run.id,
                    summary=f"Posted workflow status panel: {run.status}",
                    payload={
                        "discord_channel_id": parent_channel_id,
                        "discord_message_id": sent.message_id,
                        "status": run.status,
                        "signature": signature,
                    },
                )
                written += 1
                continue

            payload = dict(panel_event.payload or {})
            if payload.get("signature") == signature and payload.get("status") == run.status:
                continue
            message_id = payload.get("discord_message_id")
            channel_id = str(payload.get("discord_channel_id") or parent_channel_id)
            if not message_id:
                continue
            await gateway.edit_message(
                channel_id=channel_id,
                message_id=str(message_id),
                embed=embed,
                view=build_workflow_controls_view(run),
            )
            panel_event.payload = {
                **payload,
                "discord_channel_id": channel_id,
                "discord_message_id": str(message_id),
                "status": run.status,
                "signature": signature,
            }
            panel_event.summary = f"Updated workflow status panel: {run.status}"
            written += 1
        return written

    async def post_work_status(
        self,
        *,
        work_item_id: str,
        parent_channel_id: str,
        gateway: DiscordOutputGateway,
    ) -> DiscordSentMessage | None:
        work_item = self.session.get(WorkItem, work_item_id)
        if work_item is None:
            raise KeyError(f"Unknown work item: {work_item_id}")
        if self._has_status_post("discord.work_status_posted", work_item.id, work_item.status):
            return None
        # A successful run can opt out of posting (produces.silent) -- e.g. a watch
        # that found nothing worth surfacing. Mark it handled so it isn't reconsidered,
        # but send no message.
        if work_item.status == "succeeded" and self._latest_attempt_silent(work_item):
            self._emit_event(
                event_type="discord.work_status_posted",
                entity_kind="work_item",
                entity_id=work_item.id,
                work_item_id=work_item.id,
                workflow_run_id=work_item.workflow_run_id,
                summary="Suppressed work output (silent run)",
                payload={"status": work_item.status, "silent": True},
            )
            return None
        if self._is_discord_intake_work(work_item):
            return await self._post_intake_work_response(
                work_item=work_item,
                gateway=gateway,
            )

        thread = await self.ensure_work_thread(
            work_item_id=work_item.id,
            parent_channel_id=parent_channel_id,
            gateway=gateway,
        )
        uploads = self._work_uploads(work_item)
        chunks = self._work_thread_chunks(work_item, uploads)
        sent_messages: list[DiscordSentMessage] = []
        for index, content in enumerate(chunks):
            is_last = index == len(chunks) - 1
            sent = await gateway.send_message(
                channel_id=thread.discord_thread_id,
                content=content,
                view=build_work_controls_view(work_item) if is_last else None,
                attachments=uploads if is_last else None,
            )
            sent_messages.append(sent)
            DiscordService(self.session).record_message(
                discord_message_id=sent.message_id,
                discord_channel_id=sent.channel_id,
                discord_thread_id=thread.discord_thread_id,
                direction="outbound",
                author="tasque",
                content_preview=content,
                work_item_id=work_item.id,
                workflow_run_id=work_item.workflow_run_id,
            )
        sent = sent_messages[-1]
        self._emit_event(
            event_type="discord.work_status_posted",
            entity_kind="work_item",
            entity_id=work_item.id,
            work_item_id=work_item.id,
            workflow_run_id=work_item.workflow_run_id,
            summary=f"Posted work status to Discord: {work_item.status}",
            payload={
                "status": work_item.status,
                "discord_message_id": sent.message_id,
                "discord_message_ids": [message.message_id for message in sent_messages],
                "upload_artifact_ids": [upload.artifact_id for upload in uploads if upload.artifact_id],
            },
        )
        return sent

    async def _post_intake_work_response(
        self,
        *,
        work_item: WorkItem,
        gateway: DiscordOutputGateway,
    ) -> DiscordSentMessage | None:
        channel_id = self._discord_intake_response_channel_id(work_item)
        if channel_id is None:
            return None

        uploads = self._intake_work_uploads(work_item)
        content = self._with_intake_upload_note(
            self._render_intake_work_response(work_item),
            uploads,
        )
        sent = await gateway.send_message(
            channel_id=channel_id,
            content=content,
            attachments=uploads,
        )
        DiscordService(self.session).record_message(
            discord_message_id=sent.message_id,
            discord_channel_id=sent.channel_id,
            discord_thread_id=None,
            direction="outbound",
            author="tasque",
            content_preview=content,
            work_item_id=work_item.id,
            workflow_run_id=work_item.workflow_run_id,
        )
        self._emit_event(
            event_type="discord.work_status_posted",
            entity_kind="work_item",
            entity_id=work_item.id,
            work_item_id=work_item.id,
            workflow_run_id=work_item.workflow_run_id,
            summary=f"Posted intake response to Discord: {work_item.status}",
            payload={
                "mode": "intake_response",
                "status": work_item.status,
                "discord_message_id": sent.message_id,
                "discord_channel_id": sent.channel_id,
                "upload_artifact_ids": [upload.artifact_id for upload in uploads if upload.artifact_id],
            },
        )
        return sent

    async def post_workflow_status(
        self,
        *,
        workflow_run_id: str,
        parent_channel_id: str,
        gateway: DiscordOutputGateway,
    ) -> DiscordSentMessage | None:
        run = self.session.get(WorkflowRun, workflow_run_id)
        if run is None:
            raise KeyError(f"Unknown workflow run: {workflow_run_id}")
        if run.status not in TERMINAL_WORKFLOW_PANEL_STATUSES:
            return None
        if self._has_status_post("discord.workflow_final_posted", run.id, run.status):
            return None

        thread = await self.ensure_workflow_thread(
            workflow_run_id=run.id,
            parent_channel_id=parent_channel_id,
            gateway=gateway,
        )
        uploads = self._workflow_final_uploads(run)
        chunks = self._workflow_thread_chunks(run, uploads)
        sent_messages: list[DiscordSentMessage] = []
        for index, content in enumerate(chunks):
            is_last = index == len(chunks) - 1
            sent = await gateway.send_message(
                channel_id=thread.discord_thread_id,
                content=content,
                view=build_workflow_controls_view(run) if is_last else None,
                attachments=uploads if is_last else None,
            )
            sent_messages.append(sent)
            DiscordService(self.session).record_message(
                discord_message_id=sent.message_id,
                discord_channel_id=sent.channel_id,
                discord_thread_id=thread.discord_thread_id,
                direction="outbound",
                author="tasque",
                content_preview=content,
                workflow_run_id=run.id,
            )
        sent = sent_messages[-1]
        self._emit_event(
            event_type="discord.workflow_final_posted",
            entity_kind="workflow_run",
            entity_id=run.id,
            workflow_run_id=run.id,
            summary=f"Posted workflow final status to Discord: {run.status}",
            payload={
                "status": run.status,
                "discord_message_id": sent.message_id,
                "discord_message_ids": [message.message_id for message in sent_messages],
                "upload_artifact_ids": [upload.artifact_id for upload in uploads if upload.artifact_id],
            },
        )
        return sent

    async def ensure_work_thread(
        self,
        *,
        work_item_id: str,
        parent_channel_id: str,
        gateway: DiscordOutputGateway,
    ) -> DiscordThread:
        work_item = self.session.get(WorkItem, work_item_id)
        if work_item is None:
            raise KeyError(f"Unknown work item: {work_item_id}")
        existing = self.session.scalar(
            select(DiscordThread).where(
                DiscordThread.purpose == "work",
                DiscordThread.work_item_id == work_item.id,
            )
        )
        if existing is not None:
            return existing

        # A reply-processor's own response stays in the thread the user replied in:
        # it inherits that thread via work_item.discord_thread_id. Post into the
        # existing thread instead of opening a new one — but do NOT re-bind it, so the
        # generator stays the bound owner (handle_thread_reply keeps routing replies
        # against its reply_followup_work context) and uq_discord_thread_work holds.
        #
        # This reuse is limited to direct reply responses (source_kind
        # "discord_reply_followup") and scheduled runs explicitly bound to a thread
        # (source_kind "schedule" with a discord_thread_id in the schedule payload --
        # e.g. an agent-owned "watch" that fires into its own thread). Any *new*
        # worker a reply-processor spawns (the next generator, via produces.child_work
        # or the work_enqueue MCP tool) still gets its own fresh thread, even though it
        # may carry an inherited discord_thread_id.
        if work_item.discord_thread_id and work_item.source_kind in {
            "discord_reply_followup",
            "schedule",
        }:
            bound = self.session.scalar(
                select(DiscordThread).where(
                    DiscordThread.discord_thread_id == work_item.discord_thread_id,
                )
            )
            if bound is not None:
                return bound

        initial_message = ""
        initial_embed = self._build_work_status_embed(work_item)
        ref = await gateway.create_thread(
            parent_channel_id=parent_channel_id,
            name=self._thread_name("work", work_item.id, work_item.title),
            initial_message=initial_message,
            initial_embed=initial_embed,
        )
        thread = DiscordService(self.session).bind_thread(
            purpose="work",
            discord_channel_id=parent_channel_id,
            discord_thread_id=ref.thread_id,
            work_item_id=work_item.id,
        )
        if ref.starter_message_id is not None:
            DiscordService(self.session).record_message(
                discord_message_id=ref.starter_message_id,
                discord_channel_id=parent_channel_id,
                discord_thread_id=None,
                direction="outbound",
                author="tasque",
                content_preview=self._render_work_embed_preview(initial_embed),
                work_item_id=work_item.id,
                workflow_run_id=work_item.workflow_run_id,
            )
        self._emit_event(
            event_type="discord.thread_created",
            entity_kind="work_item",
            entity_id=work_item.id,
            work_item_id=work_item.id,
            workflow_run_id=work_item.workflow_run_id,
            summary="Created Discord work thread",
            payload={"discord_thread_id": ref.thread_id},
        )
        return thread

    async def ensure_workflow_thread(
        self,
        *,
        workflow_run_id: str,
        parent_channel_id: str,
        gateway: DiscordOutputGateway,
    ) -> DiscordThread:
        run = self.session.get(WorkflowRun, workflow_run_id)
        if run is None:
            raise KeyError(f"Unknown workflow run: {workflow_run_id}")
        existing = self.session.scalar(
            select(DiscordThread).where(
                DiscordThread.purpose == "workflow",
                DiscordThread.workflow_run_id == run.id,
            )
        )
        if existing is not None:
            run.discord_thread_id = existing.discord_thread_id
            self.session.flush()
            return existing

        # If this run was started from an existing Discord thread (e.g. a stylist work
        # thread that triggered the build), deliver its result back INTO that thread
        # instead of opening a separate workflow thread. We reuse the existing
        # DiscordThread for posting only and do NOT create a second binding -- the global
        # uq_discord_thread_id holds, and the thread's original owner keeps routing
        # replies (handle_thread_reply is unaffected).
        if run.discord_thread_id:
            bound = self.session.scalar(
                select(DiscordThread).where(
                    DiscordThread.discord_thread_id == run.discord_thread_id,
                )
            )
            if bound is not None:
                return bound

        initial_message = ""
        initial_embed = self._build_workflow_status_embed(run)
        ref = await gateway.create_thread(
            parent_channel_id=parent_channel_id,
            name=self._thread_name("workflow", run.id, run.name),
            initial_message=initial_message,
            initial_embed=initial_embed,
        )
        thread = DiscordService(self.session).bind_thread(
            purpose="workflow",
            discord_channel_id=parent_channel_id,
            discord_thread_id=ref.thread_id,
            workflow_run_id=run.id,
        )
        run.discord_thread_id = thread.discord_thread_id
        if ref.starter_message_id is not None:
            DiscordService(self.session).record_message(
                discord_message_id=ref.starter_message_id,
                discord_channel_id=parent_channel_id,
                discord_thread_id=None,
                direction="outbound",
                author="tasque",
                content_preview=self._render_work_embed_preview(initial_embed),
                workflow_run_id=run.id,
            )
        self._emit_event(
            event_type="discord.thread_created",
            entity_kind="workflow_run",
            entity_id=run.id,
            workflow_run_id=run.id,
            summary="Created Discord workflow thread",
            payload={"discord_thread_id": ref.thread_id},
        )
        return thread

    def _pending_workflow_runs(self, limit: int) -> Sequence[WorkflowRun]:
        if limit <= 0:
            return []
        candidates = self.session.scalars(
            select(WorkflowRun)
            .where(
                WorkflowRun.status.in_(TERMINAL_WORKFLOW_PANEL_STATUSES),
            )
            # Newest first: a freshly-finished run must stay inside the candidate
            # window even once thousands of older runs exist, or its final status
            # would never post.
            .order_by(WorkflowRun.updated_at.desc())
            .limit(limit * 4)
        ).all()
        return [
            run
            for run in candidates
            if not self._has_status_post("discord.workflow_final_posted", run.id, run.status)
        ][:limit]

    def _workflow_panel_candidates(self, limit: int) -> Sequence[WorkflowRun]:
        if limit <= 0:
            return []
        statuses = ACTIVE_WORKFLOW_PANEL_STATUSES | TERMINAL_WORKFLOW_PANEL_STATUSES
        # Newest first. With ASC + LIMIT, once more than `limit` runs accumulated
        # the window only ever held the oldest (long-finished) runs, so currently
        # active runs never got a status panel. DESC keeps live runs in-window.
        return self.session.scalars(
            select(WorkflowRun)
            .where(WorkflowRun.status.in_(statuses))
            .order_by(WorkflowRun.updated_at.desc())
            .limit(limit)
        ).all()

    def _workflow_panel_event(self, workflow_run_id: str) -> WorkEvent | None:
        return self.session.scalar(
            select(WorkEvent)
            .where(
                WorkEvent.event_type == "discord.workflow_status_panel_posted",
                WorkEvent.entity_id == workflow_run_id,
            )
            .order_by(WorkEvent.created_at.desc(), WorkEvent.id.desc())
            .limit(1)
        )

    def _pending_work_items(self, limit: int) -> Sequence[WorkItem]:
        if limit <= 0:
            return []
        candidates = self.session.scalars(
            select(WorkItem)
            .where(
                WorkItem.status.in_(WORK_OUTPUT_STATUSES),
                WorkItem.visible.is_(True),
                WorkItem.workflow_run_id.is_(None),
            )
            # Newest first, same reason as the workflow-run candidates: keep
            # just-finished work in-window regardless of history size.
            .order_by(WorkItem.updated_at.desc())
            .limit(limit * 4)
        ).all()
        return [
            work_item
            for work_item in candidates
            if not self._has_status_post("discord.work_status_posted", work_item.id, work_item.status)
        ][:limit]

    def _has_status_post(self, event_type: str, entity_id: str, status: str) -> bool:
        events = self.session.scalars(
            select(WorkEvent).where(
                WorkEvent.event_type == event_type,
                WorkEvent.entity_id == entity_id,
            )
        ).all()
        return any((event.payload or {}).get("status") == status for event in events)

    def _unposted_workflow_events(self, workflow_run_id: str) -> list[WorkEvent]:
        posted_event_ids = self._posted_workflow_event_ids()
        events = self.session.scalars(
            select(WorkEvent)
            .where(
                WorkEvent.workflow_run_id == workflow_run_id,
                or_(
                    WorkEvent.event_type.like("workflow.%"),
                    WorkEvent.event_type.in_(WORKFLOW_WORK_EVENT_TYPES),
                ),
            )
            .order_by(WorkEvent.created_at.asc(), WorkEvent.id.asc())
        ).all()
        return [event for event in events if event.id not in posted_event_ids]

    def _posted_workflow_event_ids(self) -> set[int]:
        events = self.session.scalars(
            select(WorkEvent).where(WorkEvent.event_type == "discord.workflow_status_posted")
        ).all()
        posted: set[int] = set()
        for event in events:
            for event_id in (event.payload or {}).get("workflow_event_ids") or []:
                if isinstance(event_id, int):
                    posted.add(event_id)
                elif isinstance(event_id, str) and event_id.isdigit():
                    posted.add(int(event_id))
        return posted

    def _control_panel_message_id(self, parent_channel_id: str) -> str | None:
        event = self._control_panel_event(parent_channel_id)
        if event is None:
            return None
        payload = event.payload or {}
        message_id = payload.get("discord_message_id")
        return str(message_id) if message_id else None

    def _control_panel_event(self, parent_channel_id: str) -> WorkEvent | None:
        events = self.session.scalars(
            select(WorkEvent).where(
                WorkEvent.event_type == "discord.control_panel_posted",
                WorkEvent.entity_id == CONTROL_PANEL_ENTITY_ID,
            )
        ).all()
        for event in reversed(events):
            payload = event.payload or {}
            if (
                payload.get("discord_channel_id") == parent_channel_id
                and payload.get("panel_version") == CONTROL_PANEL_VERSION
            ):
                return event
        return None

    def _latest_attempt_silent(self, work_item: WorkItem) -> bool:
        attempt = self._latest_attempt(work_item.id)
        if attempt is None:
            return False
        produces = attempt.produces or {}
        return bool(produces.get("silent") or produces.get("suppress_output"))

    def _latest_attempt(self, work_item_id: str) -> WorkAttempt | None:
        return self.session.scalar(
            select(WorkAttempt)
            .where(WorkAttempt.work_item_id == work_item_id)
            .order_by(WorkAttempt.attempt_number.desc())
        )

    def _work_uploads(self, work_item: WorkItem) -> list[DiscordFileUpload]:
        attempt = self._latest_attempt(work_item.id)
        artifact_ids: list[str] = []
        if attempt is not None:
            produces = attempt.produces or {}
            artifact_ids.extend(_string_list(produces.get("discord_upload_artifact_ids")))
            artifact_ids.extend(_string_list(produces.get("upload_artifact_ids")))
            if work_item.status == "dead_letter":
                artifact_ids.extend(
                    artifact.id
                    for artifact in self._provider_log_artifacts(attempt)
                )
        tagged = self.session.scalars(
            select(Artifact).where(
                Artifact.work_item_id == work_item.id,
                Artifact.archived_at.is_(None),
            )
        ).all()
        for artifact in tagged:
            if "discord_upload" in set(artifact.tags or []):
                artifact_ids.append(artifact.id)
        return self._uploads_from_artifact_ids(artifact_ids)

    def _work_thread_chunks(
        self,
        work_item: WorkItem,
        uploads: list[DiscordFileUpload],
    ) -> list[str]:
        content = self._render_work_thread_content(work_item)
        if uploads:
            content = self._with_attachment_note(content, uploads)
        chunks = _split_discord_markdown(content, limit=DISCORD_MESSAGE_LIMIT)
        if chunks:
            return chunks
        return ["Done."]

    def _render_work_thread_content(self, work_item: WorkItem) -> str:
        attempt = self._latest_attempt(work_item.id)
        if work_item.status == "succeeded":
            report = self._attempt_report_markdown(attempt)
            if report:
                return report
            if attempt is not None and attempt.summary:
                return attempt.summary
            return "Done."
        if work_item.status == "canceled":
            return "Canceled."
        return self._render_work_status(work_item)

    def _workflow_thread_chunks(
        self,
        run: WorkflowRun,
        uploads: list[DiscordFileUpload],
    ) -> list[str]:
        content = self._render_workflow_thread_content(run)
        if uploads:
            content = self._with_attachment_note(content, uploads)
        chunks = _split_discord_markdown(content, limit=DISCORD_MESSAGE_LIMIT)
        if chunks:
            return chunks
        return ["Done."]

    def _render_workflow_thread_content(self, run: WorkflowRun) -> str:
        if run.status == "completed":
            return self._render_workflow_success_content(run)
        if run.status == "canceled":
            return "Canceled."
        return self._render_workflow_final_status(run)

    def _render_workflow_success_content(self, run: WorkflowRun) -> str:
        final_attempts = self._workflow_final_attempts(run)
        if not final_attempts:
            return "Done."
        sections: list[str] = []
        for node, attempt in final_attempts:
            report = self._attempt_report_markdown(attempt)
            content = report or attempt.summary or "Done."
            if len(final_attempts) == 1:
                sections.append(content)
            else:
                sections.append(f"## {node.node_key}\n\n{content}")
        return "\n\n".join(section.strip() for section in sections if section.strip()) or "Done."

    def _intake_work_uploads(self, work_item: WorkItem) -> list[DiscordFileUpload]:
        attempt = self._latest_attempt(work_item.id)
        artifact_ids: list[str] = []
        if attempt is not None:
            produces = attempt.produces or {}
            artifact_ids.extend(_string_list(produces.get("discord_upload_artifact_ids")))
            artifact_ids.extend(_string_list(produces.get("upload_artifact_ids")))
        tagged = self.session.scalars(
            select(Artifact).where(
                Artifact.work_item_id == work_item.id,
                Artifact.archived_at.is_(None),
            )
        ).all()
        for artifact in tagged:
            if "discord_upload" in set(artifact.tags or []):
                artifact_ids.append(artifact.id)
        return self._uploads_from_artifact_ids(artifact_ids)

    def _workflow_uploads(self, run: WorkflowRun) -> list[DiscordFileUpload]:
        artifacts = self.session.scalars(
            select(Artifact).where(
                Artifact.workflow_run_id == run.id,
                Artifact.archived_at.is_(None),
            )
        ).all()
        return [
            upload
            for upload in (
                self._upload_from_artifact(artifact)
                for artifact in artifacts
                if "discord_upload" in set(artifact.tags or [])
            )
            if upload is not None
        ][:10]

    def _workflow_final_uploads(self, run: WorkflowRun) -> list[DiscordFileUpload]:
        artifact_ids: list[str] = []
        for _node, attempt in self._workflow_final_attempts(run):
            produces = attempt.produces or {}
            artifact_ids.extend(_string_list(produces.get("discord_upload_artifact_ids")))
            artifact_ids.extend(_string_list(produces.get("upload_artifact_ids")))

        tagged = self.session.scalars(
            select(Artifact).where(
                Artifact.workflow_run_id == run.id,
                Artifact.archived_at.is_(None),
            )
        ).all()
        for artifact in tagged:
            if "discord_upload" in set(artifact.tags or []):
                artifact_ids.append(artifact.id)
        return self._uploads_from_artifact_ids(artifact_ids)

    def _uploads_from_artifact_ids(self, artifact_ids: list[str]) -> list[DiscordFileUpload]:
        uploads: list[DiscordFileUpload] = []
        seen: set[str] = set()
        for artifact_id in artifact_ids:
            if artifact_id in seen:
                continue
            seen.add(artifact_id)
            artifact = self.session.get(Artifact, artifact_id)
            if artifact is None:
                continue
            upload = self._upload_from_artifact(artifact)
            if upload is not None:
                uploads.append(upload)
        return uploads[:10]

    def _upload_from_artifact(self, artifact: Artifact) -> DiscordFileUpload | None:
        path = Path(artifact.local_path)
        if not path.is_file():
            return None
        filename = Path(str(artifact.title)).name if artifact.title else path.name
        return DiscordFileUpload(
            path=str(path),
            filename=filename or path.name,
            artifact_id=artifact.id,
        )

    def _with_attachment_note(self, content: str, uploads: list[DiscordFileUpload]) -> str:
        if not uploads:
            return content
        names = ", ".join(upload.filename or Path(upload.path).name for upload in uploads[:5])
        suffix = "" if len(uploads) <= 5 else f", and {len(uploads) - 5} more"
        note = f"Attached files: {names}{suffix}"
        return f"{content.strip()}\n\n{note}" if content.strip() else note

    def _with_intake_upload_note(self, content: str, uploads: list[DiscordFileUpload]) -> str:
        if not uploads:
            return content
        names = ", ".join(upload.filename or Path(upload.path).name for upload in uploads[:5])
        suffix = "" if len(uploads) <= 5 else f", and {len(uploads) - 5} more"
        return self._truncate(f"{content}\n\nAttached: {names}{suffix}")

    def _render_work_intro(self, work_item: WorkItem) -> str:
        return self._truncate(
            "\n".join(
                [
                    f"Work: {work_item.title}",
                    f"Status: {work_item.status}",
                    f"Worker: {work_item.worker_kind}",
                ]
            )
        )

    def _build_work_status_embed(self, work_item: WorkItem) -> dict[str, Any]:
        attempt = self._latest_attempt(work_item.id)
        description = self._work_embed_description(work_item, attempt)
        fields = [
            {"name": "Status", "value": work_item.status, "inline": True},
            {"name": "Worker", "value": work_item.worker_kind, "inline": True},
        ]
        return {
            "title": self._truncate(f"Work: {work_item.title}", limit=256),
            "description": self._truncate(description, limit=900),
            "color": _work_status_color(work_item.status),
            "fields": fields,
        }

    def _work_embed_description(
        self,
        work_item: WorkItem,
        attempt: WorkAttempt | None,
    ) -> str:
        if work_item.status == "succeeded":
            if attempt is not None and attempt.summary:
                return attempt.summary
            report = self._attempt_report_text(attempt, limit=500)
            if report:
                return _one_line_preview(report, limit=500)
            return "Done."
        if work_item.status == "canceled":
            return "Canceled."
        if attempt is not None and attempt.error_message:
            return attempt.error_message
        if attempt is not None and attempt.summary:
            return attempt.summary
        return f"Status: {work_item.status}"

    def _render_work_embed_preview(self, embed: dict[str, Any]) -> str:
        title = str(embed.get("title") or "").strip()
        description = str(embed.get("description") or "").strip()
        return self._truncate("\n".join(part for part in [title, description] if part))

    def _build_workflow_status_embed(self, run: WorkflowRun) -> dict[str, Any]:
        final_attempts = self._workflow_final_attempts(run)
        description = self._workflow_embed_description(run, final_attempts)
        fields = [
            {"name": "Status", "value": run.status, "inline": True},
            {"name": "Run ID", "value": run.id, "inline": False},
        ]
        return {
            "title": self._truncate(f"Workflow: {run.name}", limit=256),
            "description": self._truncate(description, limit=900),
            "color": _workflow_status_color(run.status),
            "fields": fields,
        }

    def _workflow_embed_description(
        self,
        run: WorkflowRun,
        final_attempts: list[tuple[WorkflowNode, WorkAttempt]],
    ) -> str:
        if run.status == "completed":
            for _node, attempt in final_attempts:
                if attempt.summary:
                    return attempt.summary
            for _node, attempt in final_attempts:
                report = self._attempt_report_text(attempt, limit=500)
                if report:
                    return _one_line_preview(report, limit=500)
            return "Done."
        if run.status == "canceled":
            return "Canceled."
        failed_or_canceled = [
            node
            for node in self._workflow_nodes(run.id)
            if node.status in {"failed", "canceled"}
        ]
        if failed_or_canceled:
            node = failed_or_canceled[0]
            detail = node.failure_reason or node.status
            return f"{node.node_key}: {detail}"
        return f"Status: {run.status}"

    def _render_work_status(self, work_item: WorkItem) -> str:
        attempt = self._latest_attempt(work_item.id)
        lines = [
            f"Work update: {work_item.title}",
            f"Status: {work_item.status}",
            f"Worker: {work_item.worker_kind}",
        ]
        if attempt is not None and attempt.summary:
            lines.append(f"Summary: {attempt.summary}")
        if attempt is not None and attempt.error_message:
            lines.append(f"Error: {attempt.error_message}")
        if attempt is not None and work_item.status == "dead_letter":
            provider_run = self._provider_run(attempt)
            if provider_run is not None:
                lines.extend(self._provider_log_lines(provider_run))
        return self._truncate("\n".join(lines))

    def _render_intake_work_response(self, work_item: WorkItem) -> str:
        attempt = self._latest_attempt(work_item.id)
        if work_item.status == "succeeded":
            report = self._attempt_report_text(attempt, limit=1800)
            if report:
                return self._truncate(report)
            if attempt is not None and attempt.summary:
                return self._truncate(attempt.summary)
            return "Done."
        if work_item.status == "canceled":
            return "Canceled."
        if attempt is not None and attempt.error_message:
            return self._truncate(f"I hit an error while handling that: {attempt.error_message}")
        return self._truncate(f"I could not complete that request. Status: {work_item.status}")

    def _attempt_report_text(self, attempt: WorkAttempt | None, *, limit: int) -> str:
        if attempt is None or not attempt.report_artifact_id:
            return ""
        artifact = self.session.get(Artifact, attempt.report_artifact_id)
        if artifact is None:
            return ""
        return self._artifact_text_preview(artifact, limit=limit)

    def _attempt_report_markdown(self, attempt: WorkAttempt | None) -> str:
        if attempt is None or not attempt.report_artifact_id:
            return ""
        artifact = self.session.get(Artifact, attempt.report_artifact_id)
        if artifact is None:
            return ""
        return self._artifact_text(artifact)

    def _is_discord_intake_work(self, work_item: WorkItem) -> bool:
        return self._discord_intake_response_channel_id(work_item) is not None

    def _discord_intake_response_channel_id(self, work_item: WorkItem) -> str | None:
        if work_item.source_kind != "discord":
            return None
        context = work_item.context or {}
        intake = context.get("discord_intake")
        if not isinstance(intake, dict):
            return None
        channel_id = intake.get("discord_channel_id")
        return str(channel_id).strip() if channel_id else None

    def _provider_run(self, attempt: WorkAttempt) -> ProviderRun | None:
        if not attempt.provider_run_id:
            return None
        return self.session.get(ProviderRun, attempt.provider_run_id)

    def _provider_log_artifacts(self, attempt: WorkAttempt) -> list[Artifact]:
        provider_run = self._provider_run(attempt)
        if provider_run is None:
            return []
        ids = [
            provider_run.stdout_artifact_id,
            provider_run.stderr_artifact_id,
            provider_run.raw_stream_artifact_id,
        ]
        artifacts: list[Artifact] = []
        for artifact_id in ids:
            if not artifact_id:
                continue
            artifact = self.session.get(Artifact, artifact_id)
            if artifact is not None and artifact.archived_at is None:
                artifacts.append(artifact)
        artifacts.extend(self._provider_trace_artifacts(provider_run))
        return artifacts

    def _provider_log_lines(self, provider_run: ProviderRun) -> list[str]:
        lines = [
            "",
            f"Provider run: {provider_run.provider} `{provider_run.id}` ({provider_run.status})",
        ]
        labels = [
            ("stdout", provider_run.stdout_artifact_id),
            ("stderr", provider_run.stderr_artifact_id),
            ("raw", provider_run.raw_stream_artifact_id),
        ]
        log_lines: list[str] = []
        for label, artifact_id in labels:
            if not artifact_id:
                continue
            artifact = self.session.get(Artifact, artifact_id)
            if artifact is None:
                continue
            filename = Path(artifact.local_path).name
            log_lines.append(f"- {label}: {filename} (`{artifact.id}`)")
        for artifact in self._provider_trace_artifacts(provider_run):
            filename = Path(artifact.local_path).name
            log_lines.append(f"- trace: {filename} (`{artifact.id}`)")
        if log_lines:
            lines.append("Provider logs:")
            lines.extend(log_lines)
        return lines

    def _provider_trace_artifacts(self, provider_run: ProviderRun) -> list[Artifact]:
        artifacts = self.session.scalars(
            select(Artifact)
            .where(
                Artifact.source_kind == "provider_run",
                Artifact.source_id == provider_run.id,
                Artifact.archived_at.is_(None),
            )
            .order_by(Artifact.created_at)
        ).all()
        return [artifact for artifact in artifacts if "trace" in set(artifact.tags or [])]

    def _render_workflow_status(self, run: WorkflowRun, events: Sequence[WorkEvent]) -> str:
        nodes = self._workflow_nodes(run.id)
        counts = _count_node_statuses(nodes)
        lines = [
            f"Workflow update: {run.name}",
            f"Status: {run.status}",
            f"Nodes: {_format_counts(counts)}",
        ]

        waiting = [node for node in nodes if node.status == "awaiting_input"]
        if waiting:
            lines.append("")
            for node in waiting[:3]:
                prompt = node.definition.get("prompt") or "Workflow is awaiting input."
                lines.append(f"Waiting: {node.node_key}: {prompt}")

        failed_or_canceled = [
            node for node in nodes if node.status in {"failed", "canceled"}
        ]
        if failed_or_canceled:
            lines.append("")
            for node in failed_or_canceled[:5]:
                detail = node.failure_reason or node.status
                lines.append(f"{node.status.title()}: {node.node_key}: {detail}")

        lines.append("")
        lines.append("Recent changes:")
        shown = events[-8:]
        for event in shown:
            lines.append(f"- {_short_event_type(event.event_type)}: {event.summary or ''}")
        omitted = len(events) - len(shown)
        if omitted > 0:
            lines.append(f"- ... {omitted} earlier change(s)")

        return self._truncate("\n".join(lines))

    def _render_workflow_final_status(self, run: WorkflowRun) -> str:
        nodes = self._workflow_nodes(run.id)
        counts = _count_node_statuses(nodes)
        lines = [
            f"Workflow complete: {run.name}" if run.status == "completed" else f"Workflow ended: {run.name}",
            f"Status: {run.status}",
            f"Run ID: {run.id}",
            f"Nodes: {_format_counts(counts)}",
        ]

        failed_or_canceled = [
            node for node in nodes if node.status in {"failed", "canceled"}
        ]
        if failed_or_canceled:
            lines.append("")
            lines.append("Failed or canceled steps:")
            for node in failed_or_canceled[:5]:
                detail = node.failure_reason or node.status
                lines.append(f"- {node.node_key}: {detail}")

        final_attempts = self._workflow_final_attempts(run)
        if final_attempts:
            lines.append("")
            lines.append("Final output:")
            for node, attempt in final_attempts[:3]:
                if attempt.summary:
                    lines.append(f"- {node.node_key}: {attempt.summary}")
                elif attempt.report_artifact_id:
                    lines.append(f"- {node.node_key}: report artifact `{attempt.report_artifact_id}`")

            preview = self._workflow_final_report_preview(final_attempts)
            if preview:
                lines.append("")
                lines.append(preview)

        return self._truncate("\n".join(lines))

    def _workflow_final_attempts(self, run: WorkflowRun) -> list[tuple[WorkflowNode, WorkAttempt]]:
        leaf_nodes = self._workflow_leaf_work_nodes(run.id)
        attempts: list[tuple[WorkflowNode, WorkAttempt]] = []
        for node in leaf_nodes:
            if not node.work_item_id:
                continue
            attempt = self._latest_attempt(node.work_item_id)
            if attempt is not None:
                attempts.append((node, attempt))
        return attempts

    def _workflow_leaf_work_nodes(self, workflow_run_id: str) -> list[WorkflowNode]:
        nodes = [
            node
            for node in self._workflow_nodes(workflow_run_id)
            if node.work_item_id is not None and node.kind in {"work", "native", "model"}
        ]
        if not nodes:
            return []
        upstream_node_ids = {
            str(edge.from_node_id)
            for edge in self.session.scalars(
                select(WorkflowEdge).where(WorkflowEdge.workflow_run_id == workflow_run_id)
            ).all()
        }
        leaves = [node for node in nodes if node.id not in upstream_node_ids]
        if leaves:
            return leaves
        return nodes[-3:]

    def _workflow_final_report_preview(
        self,
        final_attempts: list[tuple[WorkflowNode, WorkAttempt]],
    ) -> str:
        for _node, attempt in final_attempts:
            if not attempt.report_artifact_id:
                continue
            artifact = self.session.get(Artifact, attempt.report_artifact_id)
            if artifact is None:
                continue
            preview = self._artifact_text_preview(artifact, limit=1200)
            if preview:
                return "Final report preview:\n" + preview
        return ""

    def _artifact_text(self, artifact: Artifact) -> str:
        path = Path(artifact.local_path)
        if not path.is_file() or path.suffix.lower() not in {".md", ".txt", ".json", ".log"}:
            return ""
        try:
            return path.read_text(encoding="utf-8", errors="replace").strip()
        except OSError:
            return ""

    def _artifact_text_preview(self, artifact: Artifact, *, limit: int) -> str:
        content = self._artifact_text(artifact)
        if len(content) <= limit:
            return content
        return content[: limit - 20] + "\n[truncated]"

    def _workflow_nodes(self, workflow_run_id: str) -> list[WorkflowNode]:
        return list(
            self.session.scalars(
                select(WorkflowNode)
                .where(WorkflowNode.workflow_run_id == workflow_run_id)
                .order_by(WorkflowNode.created_at)
            ).all()
        )

    def _thread_name(self, purpose: str, entity_id: str, title: str) -> str:
        safe_title = " ".join(title.split())[:72]
        return f"{purpose}-{entity_id[:8]} {safe_title}"[:100]

    def _truncate(self, content: str, limit: int = 1900) -> str:
        if len(content) <= limit:
            return content
        return content[: limit - 20] + "\n[truncated]"

    def _emit_event(
        self,
        *,
        event_type: str,
        entity_kind: str,
        entity_id: str,
        work_item_id: str | None = None,
        workflow_run_id: str | None = None,
        summary: str | None = None,
        payload: dict[str, object] | None = None,
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


def _string_list(value: object) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return [str(item) for item in value if item]
    return []


def _split_discord_markdown(content: str, *, limit: int) -> list[str]:
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
            for start in range(0, len(line), limit):
                chunk = line[start : start + limit].rstrip()
                if chunk:
                    chunks.append(chunk)
            continue
        if len(current) + len(line) > limit and current.strip():
            chunks.append(current.rstrip())
            current = line
            continue
        current += line
    if current.strip():
        chunks.append(current.rstrip())
    return chunks


def _one_line_preview(content: str, *, limit: int) -> str:
    compact = " ".join(content.split())
    if len(compact) <= limit:
        return compact
    return compact[: limit - 20] + " [truncated]"


def _work_status_color(status: str) -> int:
    if status == "succeeded":
        return 0x57F287
    if status == "dead_letter":
        return 0xED4245
    if status == "canceled":
        return 0x747F8D
    return 0x5865F2


def _workflow_status_color(status: str) -> int:
    if status == "completed":
        return 0x57F287
    if status == "failed":
        return 0xED4245
    if status == "canceled":
        return 0x747F8D
    return 0x5865F2


def _required_channel(name: str, value: str | None) -> str:
    if value is None or not value.strip():
        raise ValueError(f"{name} is required; Discord output no longer falls back to another channel.")
    return value.strip()


def _count_node_statuses(nodes: Sequence[WorkflowNode]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for node in nodes:
        counts[node.status] = counts.get(node.status, 0) + 1
    return counts


def _format_counts(counts: dict[str, int]) -> str:
    if not counts:
        return "(none)"
    return ", ".join(f"{key}={value}" for key, value in sorted(counts.items()))


def _short_event_type(event_type: str) -> str:
    return event_type.removeprefix("workflow.").removeprefix("work.").replace("_", " ")


def _embed_signature(embed: dict[str, Any]) -> str:
    return json.dumps(
        {
            "title": embed.get("title"),
            "description": embed.get("description"),
            "color": embed.get("color"),
            "fields": embed.get("fields"),
        },
        sort_keys=True,
        default=str,
    )
