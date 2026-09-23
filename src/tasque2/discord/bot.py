"""The Discord client: intake and reply routing, button actions, and the output loop."""

from __future__ import annotations

import asyncio
import logging

import discord
from opentelemetry.trace import SpanKind

from tasque2.config import Settings, get_settings
from tasque2.db import session_scope
from tasque2.discord.gateway import DiscordPyGateway
from tasque2.discord.output import DiscordOutputService, OutputChannels
from tasque2.discord.routing import DiscordAttachmentPayload, DiscordRouteResult, DiscordService
from tasque2.discord.ui import DiscordUIService, is_modal_action, parse_custom_id
from tasque2.models import WorkItem
from tasque2.telemetry import span

logger = logging.getLogger(__name__)

WAITING_STATUSES = {"ready", "running", "cancel_requested"}
TYPING_ACTIONS = {"work_queued", "work_reply_recorded", "workflow_reply_followup_recorded"}
TYPING_INTERVAL_SECONDS = 8.0


def output_channels(settings: Settings) -> OutputChannels:
    values = {
        "TASQUE2_DISCORD_OPS_CHANNEL_ID": settings.discord_ops_channel_id,
        "TASQUE2_DISCORD_JOBS_CHANNEL_ID": settings.discord_jobs_channel_id,
        "TASQUE2_DISCORD_CHAINS_CHANNEL_ID": settings.discord_chains_channel_id,
        "TASQUE2_DISCORD_DLQ_CHANNEL_ID": settings.discord_dlq_channel_id,
    }
    missing = [name for name, value in values.items() if not (value or "").strip()]
    if missing:
        raise RuntimeError("Discord output channels are required; missing " + ", ".join(missing) + ".")
    ops, jobs, chains, dlq = (str(value).strip() for value in values.values())
    return OutputChannels(ops=ops, jobs=jobs, chains=chains, dlq=dlq)


def discord_configured(settings: Settings | None = None) -> bool:
    settings = settings or get_settings()
    return bool(settings.discord_token and settings.discord_intake_channel_id)


class TasqueBot(discord.Client):
    def __init__(self, settings: Settings | None = None) -> None:
        # Tasque never joins voice, so discord.py's start-up warnings about missing voice libraries are noise.
        discord.VoiceClient.warn_nacl = discord.VoiceClient.warn_dave = False
        intents = discord.Intents.default()
        intents.message_content = True
        super().__init__(intents=intents)
        self.settings = settings or get_settings()
        self.channels = output_channels(self.settings)
        self.intake_channel_id = (self.settings.discord_intake_channel_id or "").strip()
        self.allowed_users = self.settings.allowed_discord_user_ids
        self._output_task: asyncio.Task | None = None

    async def on_ready(self) -> None:
        logger.info("Discord connected as %s", self.user)
        if self._output_task is None:
            self._output_task = asyncio.create_task(self._output_loop())

    async def on_message(self, message: discord.Message) -> None:
        # System messages (thread renames, pins) are authored by the acting user, so only
        # plain messages and replies count as the user speaking.
        if message.author.bot or message.type not in (discord.MessageType.default, discord.MessageType.reply):
            return
        if not self._allowed(str(message.author.id)):
            return
        channel_id = str(message.channel.id)
        thread_id = channel_id if isinstance(message.channel, discord.Thread) else None
        referenced = _referenced_message_id(message)
        if not self._routable(channel_id, thread_id, referenced):
            return
        try:
            attachments = await self._read_attachments(message)
        except ValueError as exc:
            await message.channel.send(str(exc)[:1900])
            return
        result = await asyncio.to_thread(
            self._route,
            discord_message_id=str(message.id),
            channel_id=channel_id,
            thread_id=thread_id,
            author=str(message.author),
            content=message.content,
            attachments=attachments,
            referenced=referenced,
        )
        if result is not None and result.action in TYPING_ACTIONS and result.entity_id:
            asyncio.create_task(self._type_until_done(message.channel, result.entity_id))

    async def on_interaction(self, interaction: discord.Interaction) -> None:
        data = interaction.data if isinstance(interaction.data, dict) else {}
        custom_id = data.get("custom_id")
        action = parse_custom_id(custom_id) if isinstance(custom_id, str) else None
        if action is None:
            return
        if interaction.user is not None and not self._allowed(str(interaction.user.id)):
            if not interaction.response.is_done():
                await interaction.response.send_message("This bot does not accept controls from you.", ephemeral=True)
            return
        if is_modal_action(action) and action.entity_id:
            await interaction.response.send_modal(_GateAnswerModal(workflow_run_id=action.entity_id))
            return
        try:
            if not interaction.response.is_done():
                await interaction.response.defer(ephemeral=True)
        except discord.HTTPException:
            pass
        try:
            content = await asyncio.to_thread(_run_ui_action, action)
        except Exception as exc:  # noqa: BLE001 - report the failure to the clicker
            content = f"Tasque action failed: {exc}"
        await interaction.followup.send(content=content[:1900], ephemeral=True)

    async def close(self) -> None:
        if self._output_task is not None:
            self._output_task.cancel()
        await super().close()

    def _route(
        self,
        *,
        discord_message_id: str,
        channel_id: str,
        thread_id: str | None,
        author: str,
        content: str,
        attachments: list[DiscordAttachmentPayload],
        referenced: str | None,
    ) -> DiscordRouteResult | None:
        if not self._routable(channel_id, thread_id, referenced):
            return None
        is_intake = channel_id == self.intake_channel_id
        with (
            span(
                "tasque.discord.receive",
                kind=SpanKind.CONSUMER,
                attributes={
                    "tasque.discord.channel_id": channel_id,
                    "tasque.discord.thread_id": thread_id,
                    "tasque.discord.intake": is_intake,
                    "tasque.discord.attachments": len(attachments),
                },
            ) as current,
            session_scope() as session,
        ):
            service = DiscordService(session)
            common = {
                "discord_message_id": discord_message_id,
                "discord_channel_id": channel_id,
                "author": author,
                "content": content,
                "attachments": attachments,
            }
            if thread_id is not None:
                result = service.handle_thread_reply(
                    discord_thread_id=thread_id, referenced_discord_message_id=referenced, **common
                )
            elif referenced:
                result = service.handle_channel_message(referenced_discord_message_id=referenced, **common)
                if result.action == "unbound_channel" and is_intake:
                    result = service.handle_intake_message(**common)
            else:
                result = service.handle_intake_message(**common)
            current.set_attribute("tasque.discord.route", result.action)
            return result

    async def _output_loop(self) -> None:
        await self.wait_until_ready()
        gateway = DiscordPyGateway(self, asyncio.get_running_loop())
        try:
            await asyncio.to_thread(self._ensure_panel, gateway)
        except Exception:  # noqa: BLE001 - the panel is retried on the next start
            logger.exception("Could not post the ops panel")
        while not self.is_closed():
            try:
                await asyncio.to_thread(self._output_pass, gateway)
            except Exception:  # noqa: BLE001 - one bad post must never stop Discord output
                logger.exception("Discord output pass failed; retrying next poll")
            await asyncio.sleep(self.settings.discord_output_poll_seconds)

    def _ensure_panel(self, gateway: DiscordPyGateway) -> None:
        with session_scope() as session:
            DiscordOutputService(session).ensure_control_panel(channel_id=self.channels.ops, gateway=gateway)

    def _output_pass(self, gateway: DiscordPyGateway) -> None:
        with session_scope() as session:
            DiscordOutputService(session).post_pending_updates(gateway=gateway, channels=self.channels)

    async def _type_until_done(self, channel, work_item_id: str) -> None:
        while await asyncio.to_thread(_waiting_on, work_item_id):
            try:
                async with channel.typing():
                    await asyncio.sleep(TYPING_INTERVAL_SECONDS)
            except discord.HTTPException:
                return

    async def _read_attachments(self, message: discord.Message) -> list[DiscordAttachmentPayload]:
        payloads = []
        for attachment in message.attachments:
            size = int(getattr(attachment, "size", 0) or 0)
            if size > self.settings.discord_max_attachment_bytes:
                raise ValueError(
                    f"Attachment {attachment.filename!r} is {size} bytes; the limit is "
                    f"{self.settings.discord_max_attachment_bytes} bytes."
                )
            payloads.append(
                DiscordAttachmentPayload(
                    filename=attachment.filename, content_type=attachment.content_type, data=await attachment.read()
                )
            )
        return payloads

    def _allowed(self, user_id: str) -> bool:
        return not self.allowed_users or user_id in self.allowed_users

    def _routable(self, channel_id: str, thread_id: str | None, referenced: str | None) -> bool:
        """Intake messages, thread messages and replies are routed; other channels are ignored."""
        return thread_id is not None or bool(referenced) or channel_id == self.intake_channel_id


def _run_ui_action(action) -> str:
    with session_scope() as session:
        return DiscordUIService(session).handle_action(action)


def _waiting_on(work_item_id: str) -> bool:
    with session_scope() as session:
        work_item = session.get(WorkItem, work_item_id)
        return work_item is not None and work_item.status in WAITING_STATUSES


def _referenced_message_id(message: discord.Message) -> str | None:
    reference = getattr(message, "reference", None)
    if reference is None:
        return None
    message_id = getattr(reference, "message_id", None) or getattr(getattr(reference, "resolved", None), "id", None)
    return str(message_id) if message_id is not None else None


class _GateAnswerModal(discord.ui.Modal):
    def __init__(self, *, workflow_run_id: str) -> None:
        super().__init__(title="Answer workflow gate")
        self.workflow_run_id = workflow_run_id
        self.node_key = discord.ui.TextInput(
            label="Gate key", required=False, max_length=120, placeholder="Leave blank when only one gate is open"
        )
        self.answer = discord.ui.TextInput(label="Answer", style=discord.TextStyle.paragraph, max_length=4000)
        self.add_item(self.node_key)
        self.add_item(self.answer)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        def submit() -> str:
            with session_scope() as session:
                return DiscordUIService(session).answer_gate(
                    workflow_run_id=self.workflow_run_id,
                    answer=str(self.answer.value),
                    node_key=str(self.node_key.value or "").strip() or None,
                )

        try:
            content = await asyncio.to_thread(submit)
        except Exception as exc:  # noqa: BLE001 - report the failure to the submitter
            content = f"Could not answer the gate: {exc}"
        await interaction.response.send_message(content=content[:1900], ephemeral=True)
