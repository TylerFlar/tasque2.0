from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from typer.testing import CliRunner

from tasque2.cli import app
from tasque2.db import session_scope
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


def test_effort_report_counts_posts_asks_and_pushback_per_thread(fresh_db: Path) -> None:
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
    assert rows["career"]["inbound"] == 2
    assert rows["career"]["pushback"] == 1
    assert rows["kitchen"]["ask_share"] == 0.0
    assert report["threads"][0]["label"] == "career"
    assert report["totals"]["posts"] == 4
    assert report["totals"]["ask_share"] == 0.5
    assert report["totals"]["posts_per_week"] == 4.0


def test_an_ask_is_answered_when_the_user_writes_before_the_next_post(fresh_db: Path) -> None:
    start = utc_now().replace(microsecond=0) - timedelta(days=2)
    with session_scope() as session:
        _thread(session, "t-money", "money")
        _message(session, thread="t-money", direction="outbound", text="Pay the card in full?", at=start)
        _message(session, thread="t-money", direction="inbound", text="yes", at=start + timedelta(minutes=5))
        _message(session, thread="t-money", direction="inbound", text="and thanks", at=start + timedelta(minutes=6))
        _message(session, thread="t-money", direction="outbound", text="Which account?", at=start + timedelta(hours=1))
        _message(session, thread="t-money", direction="outbound", text="Paid.", at=start + timedelta(hours=2))
        _message(session, thread="t-money", direction="inbound", text="ok", at=start + timedelta(hours=3))
        _message(
            session, thread="t-money", direction="outbound", text="Confirm the move?", at=start + timedelta(hours=4)
        )

    with session_scope() as session:
        report = effort_report(session, days=7)
        earlier = effort_report(session, days=1, now=start + timedelta(minutes=30))

    row = report["threads"][0]
    # the first ask was answered; the second met Tasque's next post first; the last still waits
    assert (row["asks"], row["answered"]) == (3, 1)
    assert (report["totals"]["asks"], report["totals"]["answered"]) == (3, 1)
    # a window ends at its own now: what came after it is not counted
    assert (earlier["threads"][0]["posts"], earlier["threads"][0]["inbound"]) == (1, 2)
    assert earlier["threads"][0]["answered"] == 1


def test_effort_command_prints_the_totals_and_a_table(fresh_db: Path) -> None:
    with session_scope() as session:
        _thread(session, "t-1", "daybook")
        _message(session, thread="t-1", direction="outbound", text="Morning page", at=utc_now())
        _thread(session, "t-2", "money")
        _message(session, thread="t-2", direction="outbound", text="Pay it now?", at=utc_now() - timedelta(hours=2))
        _message(session, thread="t-2", direction="inbound", text="yes", at=utc_now() - timedelta(hours=1))

    result = CliRunner().invoke(app, ["effort", "--days", "7"])

    assert result.exit_code == 0, result.output
    assert "2 posts" in result.output and "1 of 1 asks answered" in result.output
    assert "daybook" in result.output
    assert "Answered" in result.output and "1/1" in result.output
