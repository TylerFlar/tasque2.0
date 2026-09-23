from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import discord
import pytest
from opentelemetry.trace import SpanKind
from sqlalchemy import select

from tasque2.config import Settings, reset_settings
from tasque2.db import session_scope
from tasque2.discord import bot as bot_module
from tasque2.discord.bot import TasqueBot, _referenced_message_id, _waiting_on, discord_configured, output_channels
from tasque2.discord.output import OutputChannels
from tasque2.discord.routing import DiscordRouteResult, DiscordService
from tasque2.models import DiscordMessage, WorkItem
from tasque2.work.repository import WorkRepository

CHANNEL_IDS = {
    "discord_intake_channel_id": "100",
    "discord_ops_channel_id": "1",
    "discord_jobs_channel_id": "2",
    "discord_chains_channel_id": "3",
    "discord_dlq_channel_id": "4",
}


def _settings(**overrides: Any) -> Settings:
    return Settings(**{**CHANNEL_IDS, **overrides})


def _route(bot: TasqueBot, **fields: Any) -> DiscordRouteResult | None:
    message = {
        "discord_message_id": "m1",
        "channel_id": "100",
        "thread_id": None,
        "author": "user",
        "content": "Plan my week.",
        "attachments": [],
        "referenced": None,
        **fields,
    }
    return bot._route(**message)


def _receive_spans(spans) -> list:
    return [span for span in spans.get_finished_spans() if span.name == "tasque.discord.receive"]


class _Typing:
    def __init__(self, channel: _Channel) -> None:
        self.channel = channel

    async def __aenter__(self) -> None:
        self.channel.typing_count += 1

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class _Channel:
    def __init__(self, channel_id: int = 100) -> None:
        self.id = channel_id
        self.typing_count = 0
        self.sent: list[str] = []

    def typing(self) -> _Typing:
        return _Typing(self)

    async def send(self, content: str) -> None:
        self.sent.append(content)


class _Thread(discord.Thread):
    def __init__(self, thread_id: int) -> None:
        self.id = thread_id


class _Author:
    def __init__(self, user_id: int = 7, *, bot: bool = False) -> None:
        self.id = user_id
        self.bot = bot

    def __str__(self) -> str:
        return "user#0001"


class _Attachment:
    def __init__(self, filename: str, data: bytes) -> None:
        self.filename = filename
        self.content_type = "text/plain"
        self.size = len(data)
        self._data = data

    async def read(self) -> bytes:
        return self._data


def _message(**fields: Any) -> SimpleNamespace:
    values = {
        "id": 900,
        "author": _Author(),
        "type": discord.MessageType.default,
        "channel": _Channel(),
        "content": "hello",
        "attachments": [],
        "reference": None,
        **fields,
    }
    return SimpleNamespace(**values)


def _deliver(bot: TasqueBot, message: SimpleNamespace, result: DiscordRouteResult | None = None) -> dict[str, Any]:
    """Run ``on_message`` with routing and typing stubbed; returns what they were called with."""
    seen: dict[str, Any] = {"routes": [], "typing": []}

    def route(**fields: Any) -> DiscordRouteResult | None:
        seen["routes"].append(fields)
        return result

    async def type_until_done(channel, work_item_id: str) -> None:
        seen["typing"].append(work_item_id)

    bot._route = route
    bot._type_until_done = type_until_done

    async def main() -> None:
        await bot.on_message(message)
        await asyncio.sleep(0)

    asyncio.run(main())
    return seen


def test_output_channels_reads_all_four_channel_ids() -> None:
    settings = _settings(discord_jobs_channel_id=" 2 ")

    assert output_channels(settings) == OutputChannels(ops="1", jobs="2", chains="3", dlq="4")


def test_output_channels_names_every_missing_channel() -> None:
    settings = _settings(discord_ops_channel_id=None, discord_dlq_channel_id="  ")

    with pytest.raises(RuntimeError) as error:
        output_channels(settings)

    assert "TASQUE2_DISCORD_OPS_CHANNEL_ID" in str(error.value)
    assert "TASQUE2_DISCORD_DLQ_CHANNEL_ID" in str(error.value)
    assert "TASQUE2_DISCORD_JOBS_CHANNEL_ID" not in str(error.value)


def test_discord_is_configured_by_a_token_and_an_intake_channel(monkeypatch: pytest.MonkeyPatch) -> None:
    assert discord_configured(_settings(discord_token="token"))
    assert not discord_configured(_settings())
    assert not discord_configured(_settings(discord_token="token", discord_intake_channel_id=None))

    monkeypatch.setenv("TASQUE2_DISCORD_TOKEN", "token")
    monkeypatch.setenv("TASQUE2_DISCORD_INTAKE_CHANNEL_ID", "100")
    reset_settings()
    assert discord_configured()


def test_bot_reads_its_channels_and_allowlist_from_settings() -> None:
    bot = TasqueBot(_settings(discord_allowed_user_ids="7, 8"))

    assert bot.channels == OutputChannels(ops="1", jobs="2", chains="3", dlq="4")
    assert bot.intake_channel_id == "100"
    assert bot._allowed("7")
    assert not bot._allowed("9")
    assert TasqueBot(_settings())._allowed("9")


def test_bot_needs_every_output_channel() -> None:
    with pytest.raises(RuntimeError):
        TasqueBot(_settings(discord_chains_channel_id=None))


def test_intake_message_is_received_in_a_span_that_its_work_joins(fresh_db: Path, spans) -> None:
    bot = TasqueBot(_settings())

    result = _route(bot)

    assert result is not None
    assert result.action == "work_queued"
    [receive] = _receive_spans(spans)
    assert receive.kind is SpanKind.CONSUMER
    assert dict(receive.attributes) == {
        "tasque.discord.channel_id": "100",
        "tasque.discord.intake": True,
        "tasque.discord.attachments": 0,
        "tasque.discord.route": "work_queued",
    }
    assert "discord.intake_queued" in [event.name for event in receive.events]
    with session_scope() as session:
        work = session.get(WorkItem, result.entity_id)
        assert work is not None
        assert work.lane == "discord-intake"
        assert work.traceparent.split("-")[1:3] == [
            f"{receive.context.trace_id:032x}",
            f"{receive.context.span_id:016x}",
        ]


def test_thread_reply_is_received_in_a_span_naming_the_thread(fresh_db: Path, spans) -> None:
    with session_scope() as session:
        work = WorkRepository(session).create_work_item(
            title="Kitchen", task_instruction="Cook.", worker_kind="provider.fake", lane="kitchen"
        )
        DiscordService(session).bind_thread(
            purpose="work", discord_channel_id="2", discord_thread_id="300", work_item_id=work.id
        )
    bot = TasqueBot(_settings())

    result = _route(bot, discord_message_id="m2", channel_id="300", thread_id="300", content="No mushrooms.")

    assert result is not None
    assert result.action == "work_reply_recorded"
    [receive] = _receive_spans(spans)
    assert receive.attributes["tasque.discord.thread_id"] == "300"
    assert receive.attributes["tasque.discord.intake"] is False
    assert receive.attributes["tasque.discord.route"] == "work_reply_recorded"
    with session_scope() as session:
        followup = session.get(WorkItem, result.entity_id)
        assert followup is not None
        assert followup.lane == "kitchen"
        assert followup.traceparent.split("-")[1:3] == [
            f"{receive.context.trace_id:032x}",
            f"{receive.context.span_id:016x}",
        ]


def test_reply_in_another_channel_routes_to_the_referenced_work(fresh_db: Path) -> None:
    with session_scope() as session:
        work = WorkRepository(session).create_work_item(title="Digest", task_instruction="Post.", worker_kind="manual")
        DiscordService(session).record_message(
            discord_message_id="out-1",
            discord_channel_id="2",
            discord_thread_id=None,
            direction="outbound",
            author="tasque",
            content_preview="Digest posted.",
            work_item_id=work.id,
        )
    bot = TasqueBot(_settings())

    result = _route(bot, discord_message_id="m3", channel_id="2", content="More detail please.", referenced="out-1")

    assert result is not None
    assert result.action == "work_reply_recorded"
    with session_scope() as session:
        followup = session.get(WorkItem, result.entity_id)
        assert followup is not None
        assert followup.context["parent_work_item_id"] == work.id


def test_intake_reply_to_a_message_tasque_does_not_know_is_queued_as_intake(fresh_db: Path) -> None:
    bot = TasqueBot(_settings())

    result = _route(bot, discord_message_id="m4", content="Also add eggs.", referenced="someone-elses-message")

    assert result is not None
    assert result.action == "work_queued"
    with session_scope() as session:
        work = session.get(WorkItem, result.entity_id)
        assert work is not None
        assert work.lane == "discord-intake"
        message = session.scalar(select(DiscordMessage).where(DiscordMessage.discord_message_id == "m4"))
        assert message is not None
        assert message.work_item_id == work.id


def test_plain_message_outside_intake_and_threads_is_ignored(fresh_db: Path, spans) -> None:
    bot = TasqueBot(_settings())

    assert _route(bot, channel_id="2") is None
    assert _receive_spans(spans) == []
    with session_scope() as session:
        assert session.scalar(select(DiscordMessage)) is None


def test_typing_runs_until_the_work_is_done(fresh_db: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(bot_module, "TYPING_INTERVAL_SECONDS", 0.01)
    with session_scope() as session:
        work_id = (
            WorkRepository(session)
            .create_work_item(title="Typing work", task_instruction="Run slowly.", worker_kind="manual")
            .id
        )
    bot = TasqueBot(_settings())
    channel = _Channel()

    async def finish_work() -> None:
        await asyncio.sleep(0.05)
        with session_scope() as session:
            session.get(WorkItem, work_id).status = "succeeded"

    async def main() -> None:
        await asyncio.gather(bot._type_until_done(channel, work_id), finish_work())

    assert _waiting_on(work_id)
    asyncio.run(main())
    assert channel.typing_count >= 1
    assert not _waiting_on(work_id)
    assert not _waiting_on("missing-work")


@pytest.mark.parametrize("action", ["work_queued", "work_reply_recorded", "workflow_reply_followup_recorded"])
def test_typing_starts_for_every_route_that_queues_work_the_user_waits_on(action: str) -> None:
    seen = _deliver(TasqueBot(_settings()), _message(), DiscordRouteResult(action=action, entity_id="w1"))

    assert seen["typing"] == ["w1"]


@pytest.mark.parametrize(
    "result",
    [
        DiscordRouteResult(action="workflow_reply_recorded", entity_id="run-1"),
        DiscordRouteResult(action="workflow_gate_answered", entity_id="node-1"),
        DiscordRouteResult(action="intake_already_recorded", entity_id="w1"),
        DiscordRouteResult(action="message_recorded"),
        DiscordRouteResult(action="unbound_thread"),
        DiscordRouteResult(action="work_queued"),
        None,
    ],
)
def test_typing_does_not_start_without_work_to_wait_on(result: DiscordRouteResult | None) -> None:
    seen = _deliver(TasqueBot(_settings()), _message(), result)

    assert len(seen["routes"]) == 1
    assert seen["typing"] == []


def test_on_message_routes_the_message_fields() -> None:
    reference = SimpleNamespace(message_id=321, resolved=None)
    message = _message(
        id=901,
        channel=_Channel(100),
        content="Book the dentist.",
        attachments=[_Attachment("notes.txt", b"call before noon")],
        reference=reference,
    )

    seen = _deliver(TasqueBot(_settings()), message)

    [route] = seen["routes"]
    assert route["discord_message_id"] == "901"
    assert route["channel_id"] == "100"
    assert route["thread_id"] is None
    assert route["author"] == "user#0001"
    assert route["content"] == "Book the dentist."
    assert route["referenced"] == "321"
    assert [(item.filename, item.data) for item in route["attachments"]] == [("notes.txt", b"call before noon")]


def test_on_message_routes_a_thread_message_with_its_thread_id() -> None:
    seen = _deliver(TasqueBot(_settings()), _message(channel=_Thread(300)))

    [route] = seen["routes"]
    assert route["channel_id"] == "300"
    assert route["thread_id"] == "300"


def test_on_message_ignores_plain_messages_outside_intake_threads_and_replies() -> None:
    channel = _Channel(555)
    message = _message(channel=channel, attachments=[_Attachment("video.mp4", b"x" * 20)])

    seen = _deliver(TasqueBot(_settings(discord_max_attachment_bytes=10)), message)

    assert seen["routes"] == []
    assert channel.sent == []


@pytest.mark.parametrize(
    "fields",
    [
        {"author": _Author(bot=True)},
        {"type": discord.MessageType.thread_created},
        {"type": discord.MessageType.pins_add},
        {"author": _Author(9)},
    ],
)
def test_on_message_ignores_bots_system_messages_and_users_outside_the_allowlist(fields: dict[str, Any]) -> None:
    seen = _deliver(TasqueBot(_settings(discord_allowed_user_ids="7")), _message(**fields))

    assert seen["routes"] == []


def test_on_message_refuses_an_oversized_attachment_in_the_channel() -> None:
    channel = _Channel()
    message = _message(channel=channel, attachments=[_Attachment("scan.pdf", b"x" * 20)])

    seen = _deliver(TasqueBot(_settings(discord_max_attachment_bytes=10)), message)

    assert seen["routes"] == []
    assert channel.sent == ["Attachment 'scan.pdf' is 20 bytes; the limit is 10 bytes."]


def test_referenced_message_id_reads_the_reply_reference() -> None:
    assert _referenced_message_id(SimpleNamespace(reference=None)) is None
    assert _referenced_message_id(SimpleNamespace()) is None
    assert _referenced_message_id(SimpleNamespace(reference=SimpleNamespace(message_id=12, resolved=None))) == "12"
    resolved = SimpleNamespace(reference=SimpleNamespace(message_id=None, resolved=SimpleNamespace(id=34)))
    assert _referenced_message_id(resolved) == "34"
