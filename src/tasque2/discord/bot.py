"""The Discord client: intake and reply routing, button actions, the output loop, and the messages the user
sent while Tasque was away.

A gate card's button answers its gate, and the card then shows the choice and loses its buttons. A choice
that asks for a note (``ask_note``) opens a text box first: submitting it answers with the note; closing it
changes nothing.

A message is routed as it arrives. When the database is busy (another writer holds SQLite's lock past its
timeout), the message is tried again after a pause, as the records of sent messages are, rather than dropped.
A voice message is heard first, while voice is on (``TASQUE2_VOICE_MODEL``): its clip is transcribed on this
machine (``tasque2.discord.voice``) and its words go on as typed text, without the clip. One that cannot be
made out gets a one-line reply and is saved as received; nothing is routed.
When the client connects with a new session (at start-up, or after a disconnect Discord could not resume),
it reads back what the user sent meanwhile: the intake channel and the active threads with messages since
Tasque last listened (the newest saved message at start-up, the disconnect after that), a day back at most,
oldest first, routing each message it never saved without holding up the start.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import threading
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import discord
from opentelemetry.trace import SpanKind
from sqlalchemy import func, select
from sqlalchemy.exc import OperationalError

from tasque2.config import Settings, get_settings
from tasque2.db import session_scope
from tasque2.discord import voice
from tasque2.discord.gateway import DiscordPyGateway
from tasque2.discord.output import (
    RECORD_ATTEMPTS,
    RECORD_PAUSE_SECONDS,
    DiscordOutputService,
    OutputChannels,
    database_busy,
)
from tasque2.discord.routing import DiscordAttachmentPayload, DiscordRouteResult, DiscordService
from tasque2.discord.ui import DiscordUIService, is_modal_action, parse_custom_id
from tasque2.models import DiscordMessage, WorkItem, utc_now
from tasque2.telemetry import record_exception, span
from tasque2.workflows import NOTE_LABEL_CHARS

logger = logging.getLogger(__name__)

WAITING_STATUSES = {"ready", "running", "cancel_requested"}
TYPING_ACTIONS = {"work_queued", "work_reply_recorded", "workflow_reply_followup_recorded"}
TYPING_INTERVAL_SECONDS = 8.0
# How far back the messages sent while Tasque was away are read, and how many per channel.
CATCH_UP_WINDOW = timedelta(days=1)
CATCH_UP_LIMIT = 100
# A message that meets a busy database is tried again, as the records of sent messages are: the attempts,
# and the pause before the n-th retry (n times this many seconds: about a minute in all).
ROUTE_ATTEMPTS = RECORD_ATTEMPTS
ROUTE_PAUSE_SECONDS = RECORD_PAUSE_SECONDS
# Discord's limits: a message, a text box, and a modal's title; and how much of a note an answered card shows.
MESSAGE_CHARS = 2000
NOTE_CHARS = 4000
MODAL_TITLE_CHARS = 45
NOTE_SHOWN_CHARS = 100
NOTE_PLACEHOLDER = "Anything you typed in the thread goes too"
# A voice message that could not be made out: the reply, and the message as it is saved.
UNHEARD_REPLY = "Couldn't make out that voice message, so nothing was done with it. Please type it instead."
UNHEARD_PREVIEW = "(a voice message that could not be made out; nothing was done with it)"


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
        self.voice_model = (self.settings.voice_model or "").strip()
        self.voice_language = (self.settings.voice_language or "").strip() or None
        self._output_task: asyncio.Task | None = None
        self._catch_up_task: asyncio.Task | None = None
        self._away_since: datetime | None = None

    async def setup_hook(self) -> None:
        """Once per process, before connecting: while voice is on, the speech model downloads in the background.
        A daemon thread rather than ``asyncio.to_thread``, so a first download never holds up shutdown."""
        if self.voice_model:
            threading.Thread(target=voice.fetch, args=(self.voice_model,), name="voice-fetch", daemon=True).start()

    async def on_ready(self) -> None:
        """A new session: the first, or one Discord could not resume. What the user sent meanwhile is read
        back without holding up the start."""
        logger.info("Discord connected as %s", self.user)
        since = self._away_since or await asyncio.to_thread(_newest_saved_message_at)
        self._away_since = None
        self._catch_up_task = asyncio.create_task(self._catch_up(since))
        if self._output_task is None:
            self._output_task = asyncio.create_task(self._output_loop())

    async def on_disconnect(self) -> None:
        if self._away_since is None:
            self._away_since = utc_now()

    async def on_resumed(self) -> None:
        self._away_since = None  # the gateway replayed what was missed

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
        content = message.content
        clips = [item for item in attachments if item.voice] if self.voice_model else []
        if clips:
            words = await self._hear(message.channel, clips)
            if not words:
                await message.channel.send(UNHEARD_REPLY)
                await asyncio.to_thread(
                    _save_unheard,
                    discord_message_id=str(message.id),
                    channel_id=channel_id,
                    thread_id=thread_id,
                    author=str(message.author),
                    content=content,
                )
                return
            content = "\n\n".join(part for part in (content.strip(), words) if part)
            attachments = [item for item in attachments if not item.voice]
        result = await asyncio.to_thread(
            self._route,
            discord_message_id=str(message.id),
            channel_id=channel_id,
            thread_id=thread_id,
            author=str(message.author),
            content=content,
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
        if action.scope == "gate" and action.entity_id:
            try:  # a read (well inside the three seconds Discord gives a reply), so the box opens at once
                prompt = await asyncio.to_thread(_gate_note_prompt, action)
            except Exception as exc:  # noqa: BLE001 - report the failure to the clicker
                await interaction.response.send_message(f"Tasque action failed: {exc}"[:1900], ephemeral=True)
                return
            if prompt is not None:
                choice, label = prompt
                await interaction.response.send_modal(
                    _GateNoteModal(action, choice=choice, label=label, card=interaction.message)
                )
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
        if action.scope == "gate":
            await _mark_card(interaction.message, content)
        await interaction.followup.send(content=content[:1900], ephemeral=True)

    async def close(self) -> None:
        for task in (self._output_task, self._catch_up_task):
            if task is not None:
                task.cancel()
        await super().close()

    async def _catch_up(self, since: datetime | None) -> None:
        """Route the messages the user sent while Tasque was away: the intake channel's and each active
        thread's with messages since ``since``, a day back at most, oldest first; one already saved is skipped."""
        after = max(moment for moment in (since, utc_now() - CATCH_UP_WINDOW) if moment is not None)
        routed = 0
        try:
            for channel in await self._channels_with_messages_since(after):
                async for message in channel.history(after=after, limit=CATCH_UP_LIMIT, oldest_first=True):
                    if await asyncio.to_thread(_saved, str(message.id)):
                        continue
                    await self.on_message(message)
                    routed += 1
        except Exception:  # noqa: BLE001 - nothing else waits on it; what failed is logged
            logger.exception("Could not read back the messages sent while Tasque was away")
        if routed:
            logger.info("Routed %d message(s) sent while Tasque was away", routed)

    async def _channels_with_messages_since(self, after: datetime) -> list:
        """The intake channel and the active threads whose last message is newer than ``after``."""
        intake = self.get_channel(int(self.intake_channel_id))
        if intake is None:
            intake = await self.fetch_channel(int(self.intake_channel_id))
        channels = [intake]
        for guild in self.guilds:
            channels.extend(await guild.active_threads())
        return [channel for channel in channels if _newer_than(getattr(channel, "last_message_id", None), after)]

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
        """Route one message; when the database is busy, try again after a pause rather than drop it."""
        if not self._routable(channel_id, thread_id, referenced):
            return None
        fields = {
            "discord_message_id": discord_message_id,
            "channel_id": channel_id,
            "thread_id": thread_id,
            "author": author,
            "content": content,
            "attachments": attachments,
            "referenced": referenced,
        }
        return _when_free(lambda: self._route_once(**fields), "routing a Discord message")

    def _route_once(
        self,
        *,
        discord_message_id: str,
        channel_id: str,
        thread_id: str | None,
        author: str,
        content: str,
        attachments: list[DiscordAttachmentPayload],
        referenced: str | None,
    ) -> DiscordRouteResult:
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
                    filename=attachment.filename,
                    content_type=attachment.content_type,
                    data=await attachment.read(),
                    voice=bool(getattr(attachment, "is_voice_message", lambda: False)()),
                )
            )
        return payloads

    async def _hear(self, channel, clips: list[DiscordAttachmentPayload]) -> str:
        """The words spoken in the voice clips, typing meanwhile: empty when one could not be made out."""
        heard = []
        async with contextlib.AsyncExitStack() as typing:
            try:
                await typing.enter_async_context(channel.typing())
            except discord.HTTPException:
                pass  # typing is a courtesy; the message goes on without it
            for clip in clips:
                words = await asyncio.to_thread(self._transcribe, clip)
                if not words:
                    return ""
                heard.append(words)
        return " ".join(heard)

    def _transcribe(self, clip: DiscordAttachmentPayload) -> str:
        """One clip's words, in a span saying whether it was heard: empty when nothing was said or it failed
        (a failure is logged)."""
        with span("tasque.discord.transcribe") as current:
            try:
                words = voice.transcribe(clip.data, model=self.voice_model, language=self.voice_language)
            except voice.VoiceError as exc:
                record_exception(current, exc)
                logger.warning("Could not transcribe a Discord voice message: %s", exc)
                words = ""
            current.set_attribute("tasque.discord.heard", bool(words))
        return words

    def _allowed(self, user_id: str) -> bool:
        return not self.allowed_users or user_id in self.allowed_users

    def _routable(self, channel_id: str, thread_id: str | None, referenced: str | None) -> bool:
        """Intake messages, thread messages and replies are routed; other channels are ignored."""
        return thread_id is not None or bool(referenced) or channel_id == self.intake_channel_id


def _run_ui_action(action, note: str | None = None, author: str | None = None) -> str:
    with session_scope() as session:
        return DiscordUIService(session).handle_action(action, note=note, author=author)


def _gate_note_prompt(action) -> tuple[str, str] | None:
    """The choice and its text box's label, when a gate button's choice asks for a note."""
    with session_scope() as session:
        return DiscordUIService(session).gate_note_prompt(str(action.action), str(action.entity_id))


async def _mark_card(card: discord.Message | None, content: str, note: str = "") -> None:
    """A gate card once answered: its text kept, the choice and the start of its note added, its buttons
    gone. When the card is long its own text gives way, never the choice."""
    if card is None or not content.startswith("Chosen: "):
        return
    flat = " ".join(note.split())
    if len(flat) > NOTE_SHOWN_CHARS:
        flat = flat[: NOTE_SHOWN_CHARS - 1] + "…"
    added = f"\n\n**{content}**" + (f"\n> {flat}" if flat else "")
    try:
        await card.edit(content=card.content[: MESSAGE_CHARS - len(added)] + added, view=None)
    except discord.HTTPException:
        pass


def _waiting_on(work_item_id: str) -> bool:
    with session_scope() as session:
        work_item = session.get(WorkItem, work_item_id)
        return work_item is not None and work_item.status in WAITING_STATUSES


def _saved(discord_message_id: str) -> bool:
    with session_scope() as session:
        found = session.scalar(select(DiscordMessage.id).where(DiscordMessage.discord_message_id == discord_message_id))
        return found is not None


def _when_free(action: Callable[[], Any], doing: str) -> Any:
    """``action()``; when the database is busy, it is tried again after a pause rather than dropped."""
    for attempt in range(1, ROUTE_ATTEMPTS):
        try:
            return action()
        except OperationalError as exc:
            if not database_busy(exc):
                raise
            logger.warning("The database was busy %s; trying again (%d)", doing, attempt)
            time.sleep(ROUTE_PAUSE_SECONDS * attempt)
    return action()


def _save_unheard(
    *, discord_message_id: str, channel_id: str, thread_id: str | None, author: str, content: str
) -> None:
    """Save a voice message that could not be made out as received, so the catch-up never routes it again."""

    def save() -> None:
        with session_scope() as session:
            DiscordService(session).record_message(
                discord_message_id=discord_message_id,
                discord_channel_id=channel_id,
                discord_thread_id=thread_id,
                direction="inbound",
                author=author,
                content_preview="\n\n".join(part for part in (content.strip(), UNHEARD_PREVIEW) if part),
            )

    _when_free(save, "saving a Discord message")


def _newest_saved_message_at() -> datetime | None:
    """When the newest message on record (sent or received) was saved: where a start-up's catch-up begins."""
    with session_scope() as session:
        moment = session.scalar(select(func.max(DiscordMessage.created_at)))
    if moment is None:
        return None
    return moment if moment.tzinfo else moment.replace(tzinfo=UTC)


def _newer_than(message_id: int | str | None, after: datetime) -> bool:
    return message_id is not None and discord.utils.snowflake_time(int(message_id)) > after


def _referenced_message_id(message: discord.Message) -> str | None:
    reference = getattr(message, "reference", None)
    if reference is None:
        return None
    message_id = getattr(reference, "message_id", None) or getattr(getattr(reference, "resolved", None), "id", None)
    return str(message_id) if message_id is not None else None


class _GateNoteModal(discord.ui.Modal):
    """The text box a gate choice that asks for a note opens: submitting answers the gate with the note and
    marks the card as a tap does. discord.py names it, so ``on_interaction`` leaves its submit to it."""

    def __init__(self, action, *, choice: str, label: str, card: discord.Message | None) -> None:
        super().__init__(title=choice[:MODAL_TITLE_CHARS])
        self.action, self.card = action, card
        self.note = discord.ui.TextInput(
            label=label[:NOTE_LABEL_CHARS],
            style=discord.TextStyle.paragraph,
            max_length=NOTE_CHARS,
            placeholder=NOTE_PLACEHOLDER,
        )
        self.add_item(self.note)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        note = str(self.note.value or "").strip()
        author = str(interaction.user) if interaction.user is not None else None
        try:
            await interaction.response.defer(ephemeral=True)
        except discord.HTTPException:
            pass
        try:
            content = await asyncio.to_thread(_run_ui_action, self.action, note, author)
        except Exception as exc:  # noqa: BLE001 - report the failure to the submitter
            content = f"Tasque action failed: {exc}"
        await _mark_card(self.card, content, note)
        await interaction.followup.send(content=content[:1900], ephemeral=True)


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
