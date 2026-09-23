"""The narrow interface Tasque uses to post to Discord.

Output passes run in a worker thread, so the gateway is synchronous: the discord.py
implementation hands each call to the bot's event loop and waits for it.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from tasque2.discord.uploads import DiscordFileUpload, batch_uploads
from tasque2.telemetry import instruments

MESSAGE_LIMIT = 1900
CALL_TIMEOUT_SECONDS = 180


@dataclass(frozen=True)
class DiscordThreadRef:
    thread_id: str
    starter_message_id: str | None = None


@dataclass(frozen=True)
class DiscordSentMessage:
    message_id: str
    channel_id: str


class DiscordGateway(Protocol):
    def create_thread(
        self,
        *,
        parent_channel_id: str,
        name: str,
        initial_message: str,
        initial_embed: dict[str, Any] | None = None,
    ) -> DiscordThreadRef: ...

    def send_message(
        self,
        *,
        channel_id: str,
        content: str,
        view: object | None = None,
        attachments: Sequence[DiscordFileUpload] | None = None,
    ) -> DiscordSentMessage: ...

    def send_embed(
        self, *, channel_id: str, embed: dict[str, Any], view: object | None = None
    ) -> DiscordSentMessage: ...

    def edit_message(
        self,
        *,
        channel_id: str,
        message_id: str,
        content: str | None = None,
        embed: dict[str, Any] | None = None,
        view: object | None = None,
    ) -> None: ...


class FakeDiscordGateway:
    """Records everything it is asked to post; used by tests and the output simulator."""

    def __init__(self) -> None:
        self.created_threads: list[tuple[str, str, str]] = []
        self.created_thread_embeds: list[dict[str, Any] | None] = []
        self.sent_messages: list[tuple[str, str]] = []
        self.sent_embeds: list[tuple[str, dict[str, Any], object | None]] = []
        self.edited_messages: list[tuple[str, str, str | None, dict[str, Any] | None, object | None]] = []
        self.sent_views: list[object | None] = []
        self.sent_attachments: list[list[DiscordFileUpload]] = []
        self._threads = 0
        self._messages = 0

    def create_thread(self, *, parent_channel_id, name, initial_message, initial_embed=None) -> DiscordThreadRef:
        self._threads += 1
        self._messages += 1
        self.created_threads.append((parent_channel_id, name, initial_message))
        self.created_thread_embeds.append(initial_embed)
        return DiscordThreadRef(
            thread_id=f"fake-thread-{self._threads}", starter_message_id=f"fake-message-{self._messages}"
        )

    def send_message(self, *, channel_id, content, view=None, attachments=None) -> DiscordSentMessage:
        self._messages += 1
        self.sent_messages.append((channel_id, content))
        self.sent_views.append(view)
        self.sent_attachments.append(list(attachments or []))
        return DiscordSentMessage(message_id=f"fake-message-{self._messages}", channel_id=channel_id)

    def send_embed(self, *, channel_id, embed, view=None) -> DiscordSentMessage:
        self._messages += 1
        self.sent_embeds.append((channel_id, embed, view))
        self.sent_views.append(view)
        self.sent_attachments.append([])
        return DiscordSentMessage(message_id=f"fake-message-{self._messages}", channel_id=channel_id)

    def edit_message(self, *, channel_id, message_id, content=None, embed=None, view=None) -> None:
        self.edited_messages.append((channel_id, message_id, content, embed, view))


class DiscordPyGateway:
    """Runs each call on the discord.py client's event loop from a worker thread."""

    def __init__(self, client, loop: asyncio.AbstractEventLoop) -> None:
        self.client = client
        self.loop = loop

    def create_thread(self, *, parent_channel_id, name, initial_message, initial_embed=None) -> DiscordThreadRef:
        return self._call(self._create_thread(parent_channel_id, name, initial_message, initial_embed))

    def send_message(self, *, channel_id, content, view=None, attachments=None) -> DiscordSentMessage:
        return self._call(self._send_message(channel_id, content, view, list(attachments or [])))

    def send_embed(self, *, channel_id, embed, view=None) -> DiscordSentMessage:
        return self._call(self._send_embed(channel_id, embed, view))

    def edit_message(self, *, channel_id, message_id, content=None, embed=None, view=None) -> None:
        self._call(self._edit_message(channel_id, message_id, content, embed, view))

    def _call(self, coroutine):
        return asyncio.run_coroutine_threadsafe(coroutine, self.loop).result(timeout=CALL_TIMEOUT_SECONDS)

    async def _channel(self, channel_id: str):
        channel = self.client.get_channel(int(channel_id))
        if channel is None:
            channel = await self.client.fetch_channel(int(channel_id))
        return channel

    async def _create_thread(self, parent_channel_id, name, initial_message, initial_embed) -> DiscordThreadRef:
        import discord

        channel = await self._channel(parent_channel_id)
        if not isinstance(channel, discord.TextChannel):
            raise TypeError("The Discord output channel must be a text channel.")
        kwargs: dict[str, Any] = {}
        if initial_message.strip():
            kwargs["content"] = initial_message[:MESSAGE_LIMIT]
        if initial_embed is not None:
            kwargs["embed"] = discord.Embed.from_dict(initial_embed)
        message = await channel.send(**(kwargs or {"content": "Started."}))
        thread = await message.create_thread(name=name[:100], auto_archive_duration=10080)
        _count_outbound()
        return DiscordThreadRef(thread_id=str(thread.id), starter_message_id=str(message.id))

    async def _send_message(self, channel_id, content, view, attachments) -> DiscordSentMessage:
        import discord

        channel = await self._channel(channel_id)
        first = None
        for index, (uploads, notes, temps) in enumerate(batch_uploads(attachments) or [([], [], [])]):
            if index == 0:
                body = content
            else:
                names = ", ".join(upload.display_name for upload in uploads)
                body = f"(continued — {len(uploads)} more attachment{'s' if len(uploads) != 1 else ''}: {names})"
            if notes:
                body = f"{body}\n" + " · ".join(notes)
            body = body[:MESSAGE_LIMIT]
            message_view = view if index == 0 else None
            try:
                files = [discord.File(upload.path, filename=upload.filename) for upload in uploads]
                try:
                    message = await channel.send(body, view=message_view, files=files or None)
                except discord.HTTPException as exc:
                    too_large = getattr(exc, "status", None) == 413 or getattr(exc, "code", None) == 40005
                    if not files or not too_large:
                        raise
                    kept = ", ".join(upload.artifact_id or upload.path for upload in uploads)
                    fallback = f"{body}\n(attachments exceeded Discord's upload limit — kept as artifacts: {kept})"
                    message = await channel.send(fallback[:MESSAGE_LIMIT], view=message_view)
            finally:
                for temp in temps:
                    Path(temp).unlink(missing_ok=True)
            _count_outbound()
            first = first or message
        return DiscordSentMessage(message_id=str(first.id), channel_id=str(channel.id))

    async def _send_embed(self, channel_id, embed, view) -> DiscordSentMessage:
        import discord

        channel = await self._channel(channel_id)
        message = await channel.send(embed=discord.Embed.from_dict(embed), view=view)
        _count_outbound()
        return DiscordSentMessage(message_id=str(message.id), channel_id=str(channel.id))

    async def _edit_message(self, channel_id, message_id, content, embed, view) -> None:
        import discord

        channel = await self._channel(channel_id)
        message = await channel.fetch_message(int(message_id))
        kwargs: dict[str, Any] = {"view": view}
        if content is not None:
            kwargs["content"] = content
        if embed is not None:
            kwargs["embed"] = discord.Embed.from_dict(embed)
        await message.edit(**kwargs)


def _count_outbound() -> None:
    instruments().discord_messages.add(1, {"tasque.discord.direction": "outbound"})
