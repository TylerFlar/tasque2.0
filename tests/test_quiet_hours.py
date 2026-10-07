from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import select

from tasque2.db import session_scope
from tasque2.discord.gateway import FakeDiscordGateway
from tasque2.discord.output import DiscordOutputService, OutputChannels
from tasque2.models import DiscordMessage, WorkAttempt, WorkItem, utc_now
from tasque2.work.repository import WorkRepository
from tasque2.work.runner import WorkRunner

CHANNELS = OutputChannels(ops="ops", jobs="jobs", chains="chains", dlq="dlq")


@pytest.fixture()
def quiet(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("tasque2.discord.output.in_quiet_hours", lambda *args, **kwargs: True)


def _finished(session, title: str, *, source_kind: str, worker_kind: str = "function.echo", **produces: Any) -> str:
    work = WorkRepository(session).create_work_item(
        title=title, task_instruction=title, worker_kind=worker_kind, source_kind=source_kind
    )
    WorkRunner(session).run_next()
    if produces:
        attempt = session.scalar(select(WorkAttempt).where(WorkAttempt.work_item_id == work.id))
        attempt.produces = {**(attempt.produces or {}), **produces}
        session.flush()
    return work.id


def _posted_titles(gateway: FakeDiscordGateway) -> list[str]:
    return [content for _channel, content in gateway.sent_messages]


def test_quiet_hours_hold_unprompted_posts_and_let_replies_reminders_and_urgent_ones_through(
    fresh_db: Path, quiet: None
) -> None:
    gateway = FakeDiscordGateway()
    with session_scope() as session:
        _finished(session, "Nightly career report", source_kind="schedule")
        _finished(session, "Answer to your question", source_kind="discord_reply_followup")
        _finished(session, "Reminder: take the bins out", source_kind="schedule", worker_kind="function.notify")
        _finished(session, "Site is down", source_kind="schedule", urgent=True)

        DiscordOutputService(session).post_pending_updates(gateway=gateway, channels=CHANNELS)

    posted = _posted_titles(gateway)
    assert "Answer to your question" in posted
    assert "Reminder: take the bins out" in posted
    assert "Site is down" in posted
    assert "Nightly career report" not in posted


def test_held_posts_go_out_once_quiet_hours_end(fresh_db: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    gateway = FakeDiscordGateway()
    monkeypatch.setattr("tasque2.discord.output.in_quiet_hours", lambda *args, **kwargs: True)
    with session_scope() as session:
        work_id = _finished(session, "Nightly career report", source_kind="schedule")
        service = DiscordOutputService(session)
        service.post_pending_updates(gateway=gateway, channels=CHANNELS)
        assert _posted_titles(gateway) == []

        monkeypatch.setattr("tasque2.discord.output.in_quiet_hours", lambda *args, **kwargs: False)
        service.post_pending_updates(gateway=gateway, channels=CHANNELS)
        service.post_pending_updates(gateway=gateway, channels=CHANNELS)

        assert _posted_titles(gateway) == ["Nightly career report"]
        assert session.get(WorkItem, work_id).status == "succeeded"


def test_a_user_who_is_up_gets_everything_at_once(fresh_db: Path, quiet: None) -> None:
    gateway = FakeDiscordGateway()
    with session_scope() as session:
        session.add(
            DiscordMessage(
                discord_message_id="in-1",
                discord_channel_id="intake",
                direction="inbound",
                author="user",
                content_preview="still up, what's next?",
                created_at=utc_now() - timedelta(minutes=10),
            )
        )
        _finished(session, "Nightly career report", source_kind="schedule")

        DiscordOutputService(session).post_pending_updates(gateway=gateway, channels=CHANNELS)

    assert "Nightly career report" in _posted_titles(gateway)


def test_activity_more_than_an_hour_ago_does_not_count_as_awake(fresh_db: Path, quiet: None) -> None:
    gateway = FakeDiscordGateway()
    with session_scope() as session:
        session.add(
            DiscordMessage(
                discord_message_id="in-old",
                discord_channel_id="intake",
                direction="inbound",
                author="user",
                content_preview="good night",
                created_at=utc_now() - timedelta(hours=3),
            )
        )
        _finished(session, "Nightly career report", source_kind="schedule")

        DiscordOutputService(session).post_pending_updates(gateway=gateway, channels=CHANNELS)

    assert _posted_titles(gateway) == []
