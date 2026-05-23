from __future__ import annotations

import asyncio
from pathlib import Path

from tasque2.db import session_scope
from tasque2.discord_bot import _typing_until_work_done, _work_is_waiting_for_response
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
