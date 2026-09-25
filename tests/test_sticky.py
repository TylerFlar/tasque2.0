from __future__ import annotations

import json
import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from tasque2.db import session_scope
from tasque2.discord.gateway import FakeDiscordGateway
from tasque2.discord.output import DiscordOutputService, OutputChannels
from tasque2.discord.routing import DiscordService
from tasque2.discord.ui import build_sticky_embed
from tasque2.mcp import tools
from tasque2.models import Schedule, WorkEvent
from tasque2.reminders import ReminderService
from tasque2.schedules import ScheduleService
from tasque2.sticky import STICKY_NOTES_MAX_CHARS, StickyService, StickyView
from tasque2.work.repository import WorkRepository

NOW = datetime(2026, 9, 25, 3, 0, tzinfo=UTC)  # Thursday 2026-09-24 20:00 in Los Angeles
CHANNELS = OutputChannels(ops="ops", jobs="jobs", chains="chains", dlq="dlq")


def _thread(session: Session, thread_id: str, *, status: str = "active") -> None:
    owner = WorkRepository(session).create_work_item(
        title="Lane opener", task_instruction="Open.", worker_kind="manual"
    )
    owner.status, owner.visible = "succeeded", False
    DiscordService(session).bind_thread(
        purpose="work", discord_channel_id="jobs", discord_thread_id=thread_id, work_item_id=owner.id, status=status
    )


def _schedule(
    session: Session, name: str, expression: str, *, thread: str | None, schedule_type: str = "cron", **payload
) -> Schedule:
    if thread is not None:
        payload["discord_thread_id"] = thread
    return ScheduleService(session).create_schedule(
        name=name,
        schedule_type=schedule_type,
        expression=expression,
        worker_kind="provider.default",
        payload={"task_instruction": f"Run {name}.", **payload},
    )


def test_a_threads_coming_up_lists_the_next_run_of_each_schedule_posting_into_it(fresh_db: Path) -> None:
    with session_scope() as session:
        _thread(session, "t-career")
        _schedule(session, "career-review", "0 12 * * SUN", thread="t-career")
        _schedule(session, "career-outcomes", "0 8 * * *", thread="t-career", gate="career_outcomes")
        _schedule(session, "new-year check", "2027-01-05T09:00", thread="t-career", schedule_type="date")
        _schedule(session, "already ran", "2026-09-20T09:00", thread="t-career", schedule_type="date")
        _schedule(session, "switched off", "0 9 * * *", thread="t-career").enabled = False
        _schedule(session, "finance-daily", "30 7 * * *", thread="t-finance")
        _schedule(session, "no thread", "0 9 * * *", thread=None)
        ReminderService(session).set(
            "Eight weeks are up on the Tasque project", "2026-11-18T11:00", thread_id="t-career", now=NOW
        )

        view = StickyService(session).view("t-career", now=NOW)
        upcoming = StickyService(session).upcoming(now=NOW)

    assert view.lines() == [
        "daily 08:00 · career-outcomes (if needed)",
        "Sun 9/27 12:00 · career-review",
        "Wed 11/18 11:00 · Reminder: Eight weeks are up on the Tasque project",
        "Tue 1/5/27 09:00 · new-year check",
    ]
    assert view.notes == "" and view.shown is True
    assert set(upcoming) == {"t-career", "t-finance"}


def test_a_long_coming_up_list_is_capped_and_counts_the_rest(fresh_db: Path) -> None:
    with session_scope() as session:
        _thread(session, "t-busy")
        for hour in range(10):
            _schedule(session, f"job {hour}", f"0 {hour + 9} * * *", thread="t-busy")

        view = StickyService(session).view("t-busy", now=NOW)

    assert len(view.coming_up) == 8
    assert view.lines()[0] == "daily 09:00 · job 0"
    assert view.lines()[-1] == "+2 more"


def test_sticky_notes_show_in_active_tasque_threads_with_notes_or_an_upcoming_run(fresh_db: Path) -> None:
    with session_scope() as session:
        stickies = StickyService(session)
        for thread_id in ("t-live", "t-quiet", "t-notes"):
            _thread(session, thread_id)
        _thread(session, "t-old", status="archived")
        _schedule(session, "live", "0 9 * * *", thread="t-live")
        _schedule(session, "old", "0 9 * * *", thread="t-old")
        _schedule(session, "unbound", "0 9 * * *", thread="t-unbound")
        stickies.set_notes("t-notes", "- Call the landlord about the lease", now=NOW)

        assert [view.thread_id for view in stickies.showable(now=NOW)] == ["t-live", "t-notes"]
        with pytest.raises(ValueError, match="not an active Tasque thread"):
            stickies.set_notes("t-old", "- Anything")
        with pytest.raises(ValueError, match="not an active Tasque thread"):
            stickies.view("t-unbound")


def test_notes_are_cleaned_capped_and_each_change_is_recorded(fresh_db: Path) -> None:
    with session_scope() as session:
        _thread(session, "t")
        stickies = StickyService(session)

        view = stickies.set_notes("t", "  - one  \r\n\r\n\r\n\r\n- two\t \n", now=NOW)
        stickies.set_notes("t", "- one\n\n- two", now=NOW + timedelta(hours=1))
        with pytest.raises(ValueError, match=f"{STICKY_NOTES_MAX_CHARS} characters"):
            stickies.set_notes("t", "x" * (STICKY_NOTES_MAX_CHARS + 1))
        stickies.set_notes("t", "", now=NOW)
        summaries = session.scalars(
            select(WorkEvent.summary).where(WorkEvent.event_type == "sticky.notes_updated").order_by(WorkEvent.id)
        ).all()

    assert view.notes == "- one\n\n- two"
    assert view.notes_updated_at == NOW
    assert summaries == ["Sticky notes updated (12 chars)", "Sticky notes cleared"]


def test_a_sticky_note_posts_silently_pins_once_and_is_edited_in_place(fresh_db: Path) -> None:
    gateway = FakeDiscordGateway()
    with session_scope() as session:
        _thread(session, "t-career")
        _schedule(session, "career-review", "0 12 * * SUN", thread="t-career")
        output = DiscordOutputService(session)

        assert output.refresh_stickies(gateway=gateway, now=NOW) == 1
        assert output.refresh_stickies(gateway=gateway, now=NOW) == 0
        StickyService(session).set_notes("t-career", "- Reply to Anthony (Leidos) on LinkedIn", now=NOW)
        assert output.refresh_stickies(gateway=gateway, now=NOW) == 1
        assert output.refresh_stickies(gateway=gateway, now=NOW + timedelta(days=3)) == 1

    [(channel, posted, _view)] = gateway.sent_embeds
    assert channel == "t-career"
    assert posted["title"] == "Sticky note"
    assert posted["fields"] == [{"name": "Coming up", "value": "Sun 9/27 12:00 · career-review", "inline": False}]
    assert "description" not in posted and "footer" not in posted
    assert gateway.silent_message_ids == ["fake-message-1"]
    assert gateway.pinned_messages == [("t-career", "fake-message-1")]
    first, second = gateway.edited_messages
    assert first[:2] == second[:2] == ("t-career", "fake-message-1")
    assert first[3]["description"] == "- Reply to Anthony (Leidos) on LinkedIn"
    assert first[3]["footer"] == {"text": "notes updated"}
    assert first[3]["timestamp"] == NOW.isoformat()
    assert second[3]["fields"][0]["value"] == "Sun 10/4 12:00 · career-review"


def test_deleting_a_sticky_notes_message_turns_it_off_until_a_worker_shows_it_again(fresh_db: Path) -> None:
    gateway = FakeDiscordGateway()
    with session_scope() as session:
        stickies = StickyService(session)
        output = DiscordOutputService(session)
        _thread(session, "t-social")
        _schedule(session, "social-weekly", "0 13 * * 0", thread="t-social")
        output.refresh_stickies(gateway=gateway, now=NOW)
        gateway.gone_message_ids.add("fake-message-1")
        stickies.set_notes("t-social", "- Send the board games invite", now=NOW)

        assert output.refresh_stickies(gateway=gateway, now=NOW) == 0
        assert output.refresh_stickies(gateway=gateway, now=NOW) == 0
        hidden = stickies.set_notes("t-social", "- Send the board games invite", now=NOW)
        shown = stickies.set_notes("t-social", "- Send the board games invite", show=True, now=NOW)
        assert output.refresh_stickies(gateway=gateway, now=NOW) == 1
        events = session.scalars(
            select(WorkEvent.event_type).where(WorkEvent.entity_id == "t-social").order_by(WorkEvent.id)
        ).all()

    assert hidden.shown is False and shown.shown is True
    assert len(gateway.sent_embeds) == 2
    assert gateway.sent_embeds[-1][1]["description"] == "- Send the board games invite"
    assert events == ["sticky.posted", "sticky.notes_updated", "sticky.removed", "sticky.shown", "sticky.posted"]


class _PinsFail(FakeDiscordGateway):
    def pin_message(self, *, channel_id, message_id) -> None:
        raise RuntimeError("Missing Permissions")


def test_a_sticky_note_that_cannot_be_pinned_is_still_kept_current(fresh_db: Path) -> None:
    gateway = _PinsFail()
    with session_scope() as session:
        _thread(session, "t-home")
        _schedule(session, "daybook-morning", "0 9 * * *", thread="t-home")
        output = DiscordOutputService(session)

        assert output.refresh_stickies(gateway=gateway, now=NOW) == 1
        StickyService(session).set_notes("t-home", "- Renew the parking permit", now=NOW)
        assert output.refresh_stickies(gateway=gateway, now=NOW) == 1

    assert len(gateway.sent_embeds) == 1
    assert gateway.edited_messages[-1][3]["description"] == "- Renew the parking permit"


class _PinsRefusedUntilAllowed(FakeDiscordGateway):
    def __init__(self) -> None:
        super().__init__()
        self.allowed = False
        self.pin_attempts = 0

    def pin_message(self, *, channel_id, message_id) -> None:
        self.pin_attempts += 1
        if not self.allowed:
            raise RuntimeError("403 Forbidden (error code: 50013): Missing Permissions")
        super().pin_message(channel_id=channel_id, message_id=message_id)


def test_a_refused_pin_is_tried_again_every_ten_minutes_until_it_holds(
    fresh_db: Path, caplog: pytest.LogCaptureFixture
) -> None:
    gateway = _PinsRefusedUntilAllowed()
    with session_scope() as session:
        for thread_id in ("t-career", "t-home"):
            _thread(session, thread_id)
            _schedule(session, f"{thread_id} review", "0 12 * * SUN", thread=thread_id)
        output = DiscordOutputService(session)

        with caplog.at_level(logging.WARNING, logger="tasque2.discord.output"):
            output.refresh_stickies(gateway=gateway, now=NOW)
            output.refresh_stickies(gateway=gateway, now=NOW + timedelta(minutes=5))
            output.refresh_stickies(gateway=gateway, now=NOW + timedelta(minutes=10))
        assert gateway.pin_attempts == 4
        gateway.allowed = True
        output.refresh_stickies(gateway=gateway, now=NOW + timedelta(minutes=20))
        output.refresh_stickies(gateway=gateway, now=NOW + timedelta(hours=2))

    assert gateway.pin_attempts == 6
    assert gateway.pinned_messages == [("t-career", "fake-message-1"), ("t-home", "fake-message-2")]
    [warning] = [record.getMessage() for record in caplog.records if "Could not pin" in record.getMessage()]
    assert "2 sticky note(s)" in warning and "Missing Permissions" in warning and "every 10 minutes" in warning


def test_a_note_deleted_before_its_pin_holds_is_turned_off(fresh_db: Path) -> None:
    gateway = _PinsRefusedUntilAllowed()
    with session_scope() as session:
        _thread(session, "t-home")
        _schedule(session, "daybook-morning", "0 9 * * *", thread="t-home")
        output = DiscordOutputService(session)
        output.refresh_stickies(gateway=gateway, now=NOW)
        gateway.gone_message_ids.add("fake-message-1")
        gateway.allowed = True

        output.refresh_stickies(gateway=gateway, now=NOW + timedelta(minutes=10))
        sticky = StickyService(session).sticky("t-home")

    assert sticky is not None and sticky.status == "off" and sticky.discord_message_id is None
    assert gateway.pinned_messages == []


def test_the_pending_output_pass_keeps_sticky_notes_current(fresh_db: Path) -> None:
    gateway = FakeDiscordGateway()
    with session_scope() as session:
        _thread(session, "t-home")
        StickyService(session).set_notes("t-home", "- Book the dentist", now=NOW)
        DiscordOutputService(session).post_pending_updates(gateway=gateway, channels=CHANNELS)

    assert [(channel, embed.get("description")) for channel, embed, _view in gateway.sent_embeds[-1:]] == [
        ("t-home", "- Book the dentist")
    ]


def test_an_empty_sticky_note_says_so() -> None:
    embed = build_sticky_embed(StickyView(thread_id="t", notes="", notes_updated_at=None, shown=True, coming_up=()))

    assert embed["title"] == "Sticky note"
    assert embed["description"] == "_(nothing here)_"
    assert "fields" not in embed and "footer" not in embed


def test_sticky_tools_default_to_the_thread_the_work_answers_in(
    fresh_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with session_scope() as session:
        _thread(session, "t-career")
        _schedule(session, "career-review", "0 12 * * SUN", thread="t-career")
        repository = WorkRepository(session)
        reply = repository.create_work_item(
            title="Career reply", task_instruction="Reply.", worker_kind="manual", discord_thread_id="t-career"
        )
        queued = repository.create_work_item(
            title="Queued follow-up",
            task_instruction="Follow up.",
            worker_kind="manual",
            context={"parent_work_item_id": reply.id},
        )
        loose = repository.create_work_item(title="Loose", task_instruction="x", worker_kind="manual")
        queued_id, loose_id = queued.id, loose.id

    monkeypatch.setenv("TASQUE2_WORK_ITEM_ID", queued_id)
    written = json.loads(tools.sticky_set(notes="- Email Alex the buyer sketch"))
    sticky = json.loads(tools.sticky_get())["sticky"]
    too_long = json.loads(tools.sticky_set(notes="x" * (STICKY_NOTES_MAX_CHARS + 1)))
    monkeypatch.setenv("TASQUE2_WORK_ITEM_ID", loose_id)
    no_thread = json.loads(tools.sticky_set(notes="- Nowhere to show this"))
    elsewhere = json.loads(tools.sticky_get(thread_id="t-career"))
    unknown = json.loads(tools.sticky_get(thread_id="t-unknown"))

    assert written == {"ok": True, "thread_id": "t-career", "shown": True, "chars": 29}
    assert sticky["notes"] == "- Email Alex the buyer sketch"
    assert [line.split(" · ")[1] for line in sticky["coming_up"]] == ["career-review"]
    assert too_long["ok"] is False and str(STICKY_NOTES_MAX_CHARS) in too_long["error"]
    assert no_thread["ok"] is False and "thread_id" in no_thread["error"]
    assert elsewhere["sticky"]["notes"] == "- Email Alex the buyer sketch"
    assert unknown["ok"] is False
    with session_scope() as session:
        event = session.scalar(select(WorkEvent).where(WorkEvent.event_type == "sticky.notes_updated"))
        assert event is not None and event.work_item_id == queued_id


def test_sticky_set_says_when_the_user_removed_the_sticky_note(fresh_db: Path) -> None:
    with session_scope() as session:
        _thread(session, "t-kitchen")
        stickies = StickyService(session)
        stickies.record_posted("t-kitchen", message_id="m-1", signature="s")
        stickies.turn_off("t-kitchen")

    hidden = json.loads(tools.sticky_set(notes="- Thaw the chili", thread_id="t-kitchen"))
    shown = json.loads(tools.sticky_set(notes="- Thaw the chili", thread_id="t-kitchen", show=True))

    assert hidden["shown"] is False and "removed" in hidden["note"]
    assert shown["shown"] is True and "note" not in shown


def test_sticky_set_states_the_notes_limit() -> None:
    assert f"{STICKY_NOTES_MAX_CHARS} characters" in (tools.sticky_set.__doc__ or "")
