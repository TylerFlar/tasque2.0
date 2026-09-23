from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import select

from tasque2.db import session_scope
from tasque2.discord.gateway import FakeDiscordGateway
from tasque2.discord.output import DiscordOutputService, OutputChannels
from tasque2.discord.routing import DiscordService
from tasque2.mcp import tools
from tasque2.models import Schedule, WorkItem
from tasque2.reminders import ReminderService, parse_when
from tasque2.schedules import ScheduleService
from tasque2.work.repository import WorkRepository
from tasque2.work.runner import WorkRunner

NOW = datetime(2026, 9, 23, 16, 0, tzinfo=UTC)
CHANNELS = OutputChannels(ops="ops", jobs="jobs", chains="chains", dlq="dlq")


def _thread(session, thread_id: str = "thread-finance") -> WorkItem:
    owner = WorkRepository(session).create_work_item(
        title="Finance opener", task_instruction="Open.", worker_kind="manual"
    )
    owner.status, owner.visible = "succeeded", False
    DiscordService(session).bind_thread(
        purpose="work", discord_channel_id="jobs", discord_thread_id=thread_id, work_item_id=owner.id
    )
    return owner


def test_a_reminder_posts_its_text_in_the_thread_at_its_time_without_a_model(fresh_db: Path) -> None:
    gateway = FakeDiscordGateway()
    with session_scope() as session:
        _thread(session)
        reminder = ReminderService(session).set(
            "Pay the phone bill", "2026-09-26T09:00", thread_id="thread-finance", now=NOW
        )
        schedules = ScheduleService(session)

        assert schedules.poll_due_schedules(now=datetime(2026, 9, 26, 15, 59, tzinfo=UTC)) == 0
        assert schedules.poll_due_schedules(now=datetime(2026, 9, 26, 16, 0, tzinfo=UTC)) == 1
        WorkRunner(session).run_next()
        DiscordOutputService(session).post_pending_updates(gateway=gateway, channels=CHANNELS)

        assert reminder.at == datetime(2026, 9, 26, 9, 0, tzinfo=reminder.at.tzinfo)
        assert gateway.created_threads == []
        assert gateway.sent_messages == [("thread-finance", "Reminder: Pay the phone bill")]
        [listed] = ReminderService(session).list(days=7, now=datetime(2026, 9, 26, 17, 0, tzinfo=UTC))
        assert listed.sent is True


def test_a_date_alone_posts_late_morning_and_past_times_are_refused(fresh_db: Path) -> None:
    assert parse_when("2026-09-26").hour == 11
    with session_scope() as session, pytest.raises(ValueError, match="already passed"):
        ReminderService(session).set("Too late", "2026-09-20", thread_id="t", now=NOW)
    with pytest.raises(ValueError, match="YYYY-MM-DD"):
        parse_when("next friday")


def test_reminders_list_cancel_and_prune(fresh_db: Path) -> None:
    with session_scope() as session:
        service = ReminderService(session)
        soon = service.set("Email MS Advising about CSE 210", "2026-09-30", thread_id="t", now=NOW)
        later = service.set("Winter tuition", "2026-12-10", thread_id="t", now=NOW)

        assert [r.text for r in service.list(days=10, now=NOW)] == ["Email MS Advising about CSE 210"]
        service.cancel(soon.id)
        assert [r.text for r in service.list(days=100, now=NOW)] == ["Winter tuition"]
        assert service.prune(now=NOW + timedelta(days=200)) == 1
        assert session.get(Schedule, later.id) is None


def test_reminder_tools_default_to_the_thread_the_work_answers_in(
    fresh_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with session_scope() as session:
        _thread(session, "thread-social")
        reply = WorkRepository(session).create_work_item(
            title="Social reply", task_instruction="Reply.", worker_kind="manual", discord_thread_id="thread-social"
        )
        orphan = WorkRepository(session).create_work_item(title="Loose", task_instruction="x", worker_kind="manual")
        reply_id, orphan_id = reply.id, orphan.id

    monkeypatch.setenv("TASQUE2_WORK_ITEM_ID", reply_id)
    created = json.loads(tools.reminder_set(text="Invite Sam to the Saturday hike", at="2099-01-02T18:00"))
    listed = json.loads(tools.reminder_list(days=365 * 80))
    monkeypatch.setenv("TASQUE2_WORK_ITEM_ID", orphan_id)
    refused = json.loads(tools.reminder_set(text="Nowhere to post", at="2099-01-02"))

    assert created["reminder"]["thread_id"] == "thread-social"
    assert [item["text"] for item in listed["items"]] == ["Invite Sam to the Saturday hike"]
    assert refused["ok"] is False and "thread_id" in refused["error"]
    assert json.loads(tools.reminder_cancel(reminder_id=created["reminder"]["id"]))["ok"] is True
    with session_scope() as session:
        assert session.scalar(select(Schedule).where(Schedule.worker_kind == "function.notify")) is None
