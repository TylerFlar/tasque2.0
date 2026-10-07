from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from typer.testing import CliRunner

from tasque2.cli import app
from tasque2.db import session_scope
from tasque2.localtime import in_quiet_hours, quiet_window
from tasque2.models import DiscordMessage, DiscordThread, WorkItem, utc_now
from tasque2.ops.effort import effort_report

_IDS = iter(range(1, 1_000_000))


def _message(session, *, thread: str, direction: str, text: str, at: datetime) -> None:
    session.add(
        DiscordMessage(
            discord_message_id=f"m-{next(_IDS)}",
            discord_channel_id="jobs",
            discord_thread_id=thread,
            direction=direction,
            author="tasque" if direction == "outbound" else "user",
            content_preview=text,
            created_at=at,
        )
    )


def _thread(session, thread_id: str, lane: str) -> None:
    work = WorkItem(title=f"{lane} work", task_instruction="x", worker_kind="function.echo", lane=lane)
    session.add(work)
    session.flush()
    session.add(
        DiscordThread(purpose="work", discord_channel_id="jobs", discord_thread_id=thread_id, work_item_id=work.id)
    )


def test_effort_report_counts_posts_asks_quiet_hours_and_pushback_per_thread(fresh_db: Path, monkeypatch) -> None:
    monkeypatch.setenv("TASQUE2_QUIET_HOURS", "22:00-08:00")
    monkeypatch.setenv("TASQUE2_TIMEZONE", "UTC")
    from tasque2.config import reset_settings

    reset_settings()
    day = utc_now().replace(hour=12, minute=0, second=0, microsecond=0) - timedelta(days=1)
    with session_scope() as session:
        _thread(session, "t-career", "career")
        _thread(session, "t-kitchen", "kitchen")
        _message(session, thread="t-career", direction="outbound", text="Applied to X.", at=day)
        _message(session, thread="t-career", direction="outbound", text="Your call: apply?", at=day.replace(hour=3))
        _message(session, thread="t-career", direction="outbound", text="Say 'sent' when it's out", at=day)
        _message(session, thread="t-career", direction="inbound", text="why didn't you just apply", at=day)
        _message(session, thread="t-career", direction="inbound", text="ok thanks", at=day)
        _message(session, thread="t-kitchen", direction="outbound", text="Logged: bowl.", at=day)

    with session_scope() as session:
        report = effort_report(session, days=7)

    rows = {row["label"]: row for row in report["threads"]}
    assert rows["career"]["posts"] == 3
    assert rows["career"]["ask_share"] == 0.67
    assert rows["career"]["quiet_hours_share"] == 0.33
    assert rows["career"]["inbound"] == 2
    assert rows["career"]["pushback"] == 1
    assert rows["kitchen"]["ask_share"] == 0.0
    assert report["threads"][0]["label"] == "career"
    assert report["totals"]["posts"] == 4
    assert report["totals"]["ask_share"] == 0.5
    assert report["totals"]["posts_per_week"] == 4.0


def test_effort_command_prints_the_totals_and_a_table(fresh_db: Path) -> None:
    with session_scope() as session:
        _thread(session, "t-1", "daybook")
        _message(session, thread="t-1", direction="outbound", text="Morning page", at=utc_now())

    result = CliRunner().invoke(app, ["effort", "--days", "7"])

    assert result.exit_code == 0, result.output
    assert "1 posts" in result.output
    assert "daybook" in result.output


def test_quiet_window_parses_crosses_midnight_and_can_be_turned_off(isolated: Path, monkeypatch) -> None:
    from tasque2.config import reset_settings

    monkeypatch.setenv("TASQUE2_TIMEZONE", "UTC")
    monkeypatch.setenv("TASQUE2_QUIET_HOURS", "22:00-08:00")
    reset_settings()
    base = datetime(2026, 10, 6, tzinfo=utc_now().tzinfo)
    assert in_quiet_hours(base.replace(hour=23))
    assert in_quiet_hours(base.replace(hour=7, minute=59))
    assert not in_quiet_hours(base.replace(hour=8))
    assert not in_quiet_hours(base.replace(hour=15))

    monkeypatch.setenv("TASQUE2_QUIET_HOURS", "13:00-14:00")
    reset_settings()
    assert in_quiet_hours(base.replace(hour=13, minute=30))
    assert not in_quiet_hours(base.replace(hour=14, minute=1))

    for off in ("", "nonsense", "09:00-09:00"):
        monkeypatch.setenv("TASQUE2_QUIET_HOURS", off)
        reset_settings()
        assert quiet_window() is None
        assert not in_quiet_hours(base.replace(hour=23))
