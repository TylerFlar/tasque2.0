from __future__ import annotations

import asyncio
from pathlib import Path

from tasque2.db import session_scope
from tasque2.discord_adapter import DiscordRouteResult
from tasque2.discord_bot import (
    _start_typing_for_result,
    _typing_until_work_done,
    _work_is_waiting_for_response,
)
from tasque2.models import WorkItem
from tasque2.repo import WorkRepository


class _FakeTypingContext:
    def __init__(self, channel: _FakeChannel) -> None:
        self.channel = channel

    async def __aenter__(self) -> None:
        self.channel.typing_count += 1

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class _FakeChannel:
    def __init__(self) -> None:
        self.typing_count = 0

    def typing(self) -> _FakeTypingContext:
        return _FakeTypingContext(self)


def test_discord_typing_indicator_runs_until_work_terminal(fresh_db: Path) -> None:
    with session_scope() as session:
        work = WorkRepository(session).create_work_item(
            title="Typing work",
            task_instruction="Run slowly.",
            worker_kind="manual",
        )
        work_id = work.id

    async def complete_work() -> None:
        await asyncio.sleep(0.02)
        with session_scope() as session:
            work_item = session.get(WorkItem, work_id)
            assert work_item is not None
            work_item.status = "succeeded"

    async def run_check() -> _FakeChannel:
        channel = _FakeChannel()
        await asyncio.gather(
            _typing_until_work_done(channel, work_id, interval_seconds=0.01),
            complete_work(),
        )
        return channel

    assert _work_is_waiting_for_response(work_id)
    channel = asyncio.run(run_check())
    assert channel.typing_count >= 1
    assert not _work_is_waiting_for_response(work_id)


class _FakeClient:
    def __init__(self) -> None:
        self.loop = self
        self.started: list[str] = []

    def create_task(self, coro) -> None:  # loop stand-in
        coro.close()
        self.started.append("task")


def test_typing_starts_for_every_route_that_queues_work_the_user_waits_on() -> None:
    # A thread bound to a workflow run answers with its own action; it queues a follow-up
    # work item exactly like a work-bound thread, so it must drive the typing indicator too.
    waiting = [
        DiscordRouteResult(action="work_queued", entity_id="w1"),
        DiscordRouteResult(action="work_reply_recorded", entity_id="w2"),
        DiscordRouteResult(action="workflow_reply_followup_recorded", entity_id="w3"),
    ]
    for result in waiting:
        client = _FakeClient()
        _start_typing_for_result(client, _FakeChannel(), result)
        assert client.started, f"{result.action} should start the typing indicator"


def test_typing_does_not_start_when_there_is_no_work_item_to_wait_on() -> None:
    # entity_id here is a workflow run / node / nothing — polling it as a WorkItem is futile.
    for result in [
        DiscordRouteResult(action="workflow_reply_recorded", entity_id="run-1"),
        DiscordRouteResult(action="workflow_gate_answered", entity_id="node-1"),
        DiscordRouteResult(action="message_recorded", entity_id=None),
        DiscordRouteResult(action="unbound_thread", entity_id=None),
        DiscordRouteResult(action="work_queued", entity_id=None),
    ]:
        client = _FakeClient()
        _start_typing_for_result(client, _FakeChannel(), result)
        assert not client.started, f"{result.action} should not start the typing indicator"
