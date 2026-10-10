"""The Workshop's sweep: each source it reads, what it makes of it, and the pass that keeps the ledger."""

from __future__ import annotations

import json
import os
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import select

from tasque2.config import get_settings
from tasque2.db import session_scope
from tasque2.extensions import registry
from tasque2.memory import MemoryService
from tasque2.models import (
    DiscordMessage,
    DiscordThread,
    FailedWork,
    ProviderRun,
    Schedule,
    ScheduleOccurrence,
    WorkAttempt,
    WorkflowRun,
    WorkItem,
    utc_now,
)
from tasque2.ops.faults import append_entry, faults_path, tool_errors_path, warnings_path
from tasque2.schedules import ScheduleService
from tasque2.workshop import ledger, sweep, triage

NOW = utc_now()


def _keys(signals: list[Any], prefix: str) -> list[str]:
    return sorted(signal.key for signal in signals if signal.key.startswith(prefix))


def _by_key(signals: list[Any]) -> dict[str, Any]:
    return {signal.key: signal for signal in signals}


def _item(session, *, lane: str = "career", title: str = "Career run", source_kind: str | None = None) -> WorkItem:
    item = WorkItem(
        title=title, task_instruction="x", worker_kind="provider.default", lane=lane, source_kind=source_kind
    )
    session.add(item)
    session.flush()
    return item


def _attempt(session, item: WorkItem, *, minutes: float = 2, ended_days_ago: float = 1, status: str = "succeeded"):
    ended = NOW - timedelta(days=ended_days_ago)
    attempt = WorkAttempt(
        work_item_id=item.id,
        attempt_number=item.attempt_count + 1,
        status=status,
        worker_kind=item.worker_kind,
        started_at=ended - timedelta(minutes=minutes),
        ended_at=ended,
        created_at=ended - timedelta(minutes=minutes),
    )
    item.attempt_count += 1
    session.add(attempt)
    session.flush()
    return attempt


# --- the ledgers on disk ---------------------------------------------------------------------------------


def test_recurring_faults_repeated_warnings_and_tool_errors_alike(fresh_db: Path) -> None:
    def fault(signature: str, days_ago: float, message: str = "'lane'") -> dict[str, Any]:
        return {
            "at": (NOW - timedelta(days=days_ago)).isoformat(),
            "logger": "tasque2.digest",
            "signature": signature,
            "exc_type": "KeyError",
            "exc_message": message,
            "frame": {"file": "tasque2/digest.py", "line": 3, "function": "build"},
        }

    for entry in (fault("aaa", 2), fault("aaa", 1), fault("bbb", 1), fault("ccc", 1, "401 Unauthorized")):
        append_entry(faults_path(), entry)
    append_entry(faults_path(), fault("ccc", 0.5, "401 Unauthorized"))
    for days, message in ((2, "Could not pin 2 notes"), (1, "Could not pin 5 notes"), (1, "Once only")):
        signature = "w-pin" if "pin" in message else "w-once"
        append_entry(
            warnings_path(),
            {
                "at": (NOW - timedelta(days=days)).isoformat(),
                "logger": "tasque2.sticky",
                "message": message,
                "signature": signature,
                "frame": {"file": "tasque2/sticky.py", "function": "pin"},
            },
        )
    for days, error in ((2, "No memory 1f2e3d4c5b6a7980"), (1, "No memory 0a9b8c7d6e5f4321"), (1, "x is required.")):
        append_entry(
            tool_errors_path(),
            {
                "at": (NOW - timedelta(days=days)).isoformat(),
                "tool": "memory_get",
                "error_type": "KeyError",
                "error": error,
                "work_item_id": None,
            },
        )
    with session_scope() as session:
        signals = sweep.gather(session, now=NOW)
        ledger.record(session, signals, now=NOW)
        statuses = {key: ledger.row(session, key).status for key in _by_key(signals)}
        found = _by_key(signals)
    assert statuses["fault:aaa"] == "new" and statuses["fault:bbb"] == "watching"  # once is not yet a pattern
    assert found["fault:aaa"].title == "KeyError in tasque2/digest.py:build"
    assert "reads as environmental" in " ".join(found["fault:ccc"].evidence)
    assert statuses["warning:w-pin"] == "new" and statuses["warning:w-once"] == "watching"
    errors = _keys(signals, "tool_error:")
    assert len(errors) == 2  # the two unknown ids are one kind of error
    assert sorted(statuses[key] for key in errors) == ["new", "watching"]
    assert any(found[key].title == "memory_get answered KeyError: No memory <id>" for key in errors)


def test_the_journal_notes_count_only_when_they_changed_this_past_week(fresh_db: Path) -> None:
    digest = get_settings().resolved_data_dir / "journal" / "digest.md"
    digest.parent.mkdir(parents=True)
    digest.write_text("## Week of 2026-10-05\n- more quiet mornings\n", encoding="utf-8")
    context = {"journal_digest": "journal/digest.md"}
    with session_scope() as session:
        notes = _keys(sweep.gather(session, now=NOW, context=context), "journal:")
        assert len(notes) == 1
        old = (NOW - timedelta(days=20)).timestamp()
        os.utime(digest, (old, old))
        assert _keys(sweep.gather(session, now=NOW, context=context), "journal:") == []
        assert _keys(sweep.gather(session, now=NOW), "journal:") == []  # no notes named, none read


# --- runs ----------------------------------------------------------------------------------------------------


def test_runs_that_never_recovered_and_runs_far_slower_than_usual(fresh_db: Path) -> None:
    with session_scope() as session:
        for title in ("Scout one", "Scout two"):
            item = _item(session, lane="career-scout", title=title)
            attempt = _attempt(session, item, status="failed")
            item.status = "dead_letter"
            session.add(
                FailedWork(work_item_id=item.id, attempt_id=attempt.id, error_type="KeyError", error_message="'url'")
            )
        resolved = _item(session, lane="career-scout", title="Retried")
        session.add(FailedWork(work_item_id=resolved.id, status="retrying", error_type="KeyError"))
        steady = _item(session, lane="kitchen")
        for days in (20, 15, 12, 9, 6, 4):
            _attempt(session, steady, minutes=2, ended_days_ago=days)
        _attempt(session, steady, minutes=20, ended_days_ago=1)  # ten times its usual
        _attempt(session, steady, minutes=4, ended_days_ago=2)  # twice: not far slower
        quick = _item(session, lane="health-sync")
        for days in (20, 15, 12, 9, 6):
            _attempt(session, quick, minutes=0.1, ended_days_ago=days)
        _attempt(session, quick, minutes=1, ended_days_ago=1)  # 10x, but a minute is not slow
        young = _item(session, lane="new-lane")
        _attempt(session, young, minutes=1, ended_days_ago=3)
        _attempt(session, young, minutes=30, ended_days_ago=1)  # too few runs to know its usual
        found = _by_key(sweep.gather(session, now=NOW))
    failed = found["failed:career-scout:KeyError"]
    assert len(failed.occurrences) == 2 and failed.title == "career-scout: runs failed and never recovered (KeyError)"
    assert _keys(list(found.values()), "slow:") == ["slow:kitchen"]
    assert found["slow:kitchen"].evidence[0] == "usually 2.0 min (8 runs in 30 days)"
    assert "took 20 min" in found["slow:kitchen"].evidence[1]


def test_packets_over_their_budget_a_reply_held_to_less(fresh_db: Path) -> None:
    with session_scope() as session:
        for chars, source_kind in ((70_000, None), (50_000, None), (45_000, "discord_reply_followup"), (39_000, None)):
            item = _item(session, source_kind=source_kind, title=f"run {chars}")
            attempt = _attempt(session, item)
            session.add(ProviderRun(attempt_id=attempt.id, provider="claude", usage={"prompt_chars": chars}))
        session.flush()
        found = _by_key(sweep.gather(session, now=NOW))
    assert _keys(list(found.values()), "packet:") == ["packet:career", "packet:career:reply"]
    assert found["packet:career"].evidence == ["1 run(s) this week; the largest 70,000 characters (run 70000)"]
    assert found["packet:career:reply"].title == "career: reply packets over 40k characters"


# --- the user's pushback ---------------------------------------------------------------------------------------


def _thread(session, thread_id: str, *, lane: str, context: dict[str, Any] | None = None) -> None:
    owner = WorkItem(title=f"{lane} opener", task_instruction="x", worker_kind="function.notify", lane=lane)
    owner.context = context or {}
    session.add(owner)
    session.flush()
    session.add(
        DiscordThread(purpose="work", discord_channel_id="jobs", discord_thread_id=thread_id, work_item_id=owner.id)
    )


def _message(session, thread_id: str, text: str, *, days_ago: float, inbound: bool = True) -> DiscordMessage:
    message = DiscordMessage(
        discord_message_id=f"m-{thread_id}-{days_ago}-{inbound}",
        discord_channel_id="jobs",
        discord_thread_id=thread_id,
        direction="inbound" if inbound else "outbound",
        author="owner" if inbound else "tasque",
        content_preview=text,
        created_at=NOW - timedelta(days=days_ago),
    )
    session.add(message)
    session.flush()
    return message


def test_pushback_anywhere_but_a_private_thread_and_the_same_correction_twice(fresh_db: Path) -> None:
    with session_scope() as session:
        _thread(session, "t-errands", lane="errands")
        _thread(session, "t-journal", lane="journal", context={"private": True})
        _thread(session, "t-workshop", lane="workshop")
        _message(session, "t-errands", "Should I book the earlier dentist slot on Friday?", days_ago=6.1, inbound=False)
        first = _message(session, "t-errands", "Just book earlier dentist slots, stop asking me about them", days_ago=6)
        second = _message(
            session, "t-errands", "I told you: earlier dentist slots, book them and don't ask", days_ago=5
        )
        _message(session, "t-errands", "Thanks, that looks good", days_ago=4)
        _message(session, "t-errands", "Why did you book the early class?", days_ago=40)  # outside the window
        _message(session, "t-journal", "Stop asking me how I feel", days_ago=3)
        _message(session, "t-workshop", "No, that's wrong", days_ago=2)
        found = _by_key(sweep.gather(session, now=NOW, skip_threads={"t-workshop"}))
    assert _keys(list(found.values()), "pushback:") == [f"pushback:{first.discord_message_id}"]
    pushed = found[f"pushback:{first.discord_message_id}"]
    assert pushed.title.startswith("Pushback in the errands thread: Just book earlier dentist slots")
    assert pushed.evidence[1] == "answering: Should I book the earlier dentist slot on Friday?"
    repeat = found[f"correction:t-errands:{first.discord_message_id}"]
    assert repeat.source == "repeat_correction" and len(repeat.occurrences) == 1
    assert repeat.evidence[1].endswith(second.content_preview)


# --- schedules, tools, doctrine, decisions -------------------------------------------------------------------


def _schedule(session, name: str, expression: str, *, days_old: float = 40, **extra: Any) -> Schedule:
    schedule = ScheduleService(session).create_schedule(
        name=name,
        schedule_type=extra.pop("schedule_type", "cron"),
        expression=expression,
        worker_kind="function.noop",
        payload=extra.pop("payload", {}),
        enabled=extra.pop("enabled", True),
    )
    schedule.created_at = NOW - timedelta(days=days_old)
    schedule.updated_at = NOW - timedelta(days=days_old)
    session.flush()
    return schedule


def _occurrence(session, schedule: Schedule, *, days_ago: float, status: str) -> None:
    moment = NOW - timedelta(days=days_ago)
    session.add(
        ScheduleOccurrence(
            schedule_id=schedule.id, scheduled_for=moment, status=status, dedupe_key=f"{schedule.id}:{moment}"
        )
    )


def test_gates_that_always_skip_and_schedules_nothing_ran_for_30_days(fresh_db: Path) -> None:
    with session_scope() as session:
        gated = _schedule(session, "card-writer", "0 9 * * *", payload={"gate": "due"})
        for days in range(1, 13):
            _occurrence(session, gated, days_ago=days, status="skipped")
        idle = _schedule(session, "stale-scan", "0 9 * * *")
        _schedule(session, "new-year", "0 9 1 1 *")  # its cadence is longer than 30 days
        _schedule(session, "paused-lane", "0 9 * * *", enabled=False)
        busy = _schedule(session, "daily", "0 9 * * *")
        _occurrence(session, busy, days_ago=3, status="enqueued")
        _schedule(session, "one-off", "2026-01-01T09:00:00", schedule_type="date", days_old=400)
        _schedule(session, "fresh", "0 9 * * *", days_old=10)
        found = _by_key(sweep.gather(session, now=NOW))
    gate = found[f"gate_skip:{gated.id}"]
    assert gate.count == 12 and gate.title == "card-writer: its gate skipped every run for 30 days"
    titles = sorted(signal.title for signal in found.values() if signal.source == "unused_schedule")
    assert titles == [
        "one-off: its one time passed over 30 days ago and it is still on",
        "paused-lane: switched off for 30 days or more",
        "stale-scan: no run in 30 days",
    ]
    assert f"unused_schedule:{idle.id}" in found


def test_tools_nothing_called_for_30_days_once_runs_have_recorded_that_long(fresh_db: Path) -> None:
    with session_scope() as session:
        recent = _attempt(session, _item(session), ended_days_ago=5)
        session.add(
            ProviderRun(
                attempt_id=recent.id,
                provider="claude",
                usage={"tool_calls": {"mcp__tasque2__work_list": 1, "Bash": 3}},
                created_at=NOW - timedelta(days=5),
            )
        )
        session.flush()
        assert _keys(sweep.gather(session, now=NOW), "unused_tools:") == []  # five days of runs say too little
        early = _attempt(session, _item(session), ended_days_ago=29)
        session.add(
            ProviderRun(
                attempt_id=early.id,
                provider="claude",
                usage={"tool_calls": {"mcp__tasque2__memory_recall": 2}},
                created_at=NOW - timedelta(days=29),
            )
        )
        session.flush()
        found = _by_key(sweep.gather(session, now=NOW))
    idle = found["unused_tools:core"]
    assert _keys(list(found.values()), "unused_tools:") == ["unused_tools:core"]  # one row for the core's tools
    prefix = "2 runs in 30 days called none of: "
    assert idle.evidence[0].startswith(prefix)
    named = [name for line in [idle.evidence[0][len(prefix) :], *idle.evidence[1:]] for name in line.split(", ")]
    assert "reminder_cancel" in named and not {"work_list", "memory_recall", "submit_worker_result"} & set(named)
    assert idle.count == len(named) and idle.title == f"{len(named)} core tool(s): no run called them in 30 days"


def test_doctrine_near_its_cap_and_questions_nobody_answered(fresh_db: Path) -> None:
    with session_scope() as session:
        service = MemoryService(session)
        marker = "<!-- tasque:max_chars=1000 -->\n"
        service.upsert_canonical(
            namespace="cooking", canonical_key="cooking_direction", kind="doctrine", content=marker + "x" * 920
        )
        service.upsert_canonical(namespace="cooking", canonical_key="cooking_state", kind="state", content=marker + "y")
        asked = service.upsert_canonical(
            namespace="finance",
            canonical_key="finance_direction",
            kind="doctrine",
            content="Rules.\n- Open for the user's decision: keep the old card open? (asked)\n"
            "- Open for the user's decision: a new bank? \n",
        )
        asked.updated_at = NOW - timedelta(days=10)
        session.flush()
        signals = sweep.gather(session, now=NOW)
        ledger.record(session, signals, now=NOW)
        decisions = _keys(signals, "open_decision:")
        assert len(decisions) == 1 and ledger.row(session, decisions[0]).status == "new"
        found = _by_key(signals)
    assert _keys(signals, "doctrine_cap:") == ["doctrine_cap:cooking/cooking_direction"]
    assert found["doctrine_cap:cooking/cooking_direction"].title == "cooking/cooking_direction is at 95% of its cap"
    assert found[decisions[0]].title == "Unanswered in finance/finance_direction: keep the old card open?"


def test_an_extension_reports_its_own_signals_and_one_that_breaks_is_left_out(fresh_db: Path, monkeypatch) -> None:
    def good(session, now) -> list[dict[str, Any]]:
        return [{"key": "decision:car", "title": "A decision took its default", "evidence": ["asked 10-01"]}, {}]

    def broken(session, now) -> list[dict[str, Any]]:
        raise RuntimeError("the board moved")

    monkeypatch.setattr(registry(), "signal_sources", [("money", good), ("studio", broken)])
    with session_scope() as session:
        found = _by_key(sweep.gather(session, now=NOW))
        assert found["money:decision:car"].evidence == ["asked 10-01"]
        with pytest.raises(RuntimeError):
            sweep.gather(session, now=NOW, strict=True)


# --- the pass --------------------------------------------------------------------------------------------------------


def test_the_sweep_keeps_the_ledger_and_starts_one_triage_for_what_is_new(fresh_db: Path) -> None:
    for days in (2, 1):
        append_entry(
            faults_path(),
            {
                "at": (NOW - timedelta(days=days)).isoformat(),
                "logger": "tasque2.x",
                "signature": "aaa",
                "exc_type": "KeyError",
                "message": "boom",
                "frame": {"file": "tasque2/x.py", "function": "f"},
            },
        )
    with session_scope() as session:
        work = WorkItem(
            title="Workshop sweep",
            task_instruction="x",
            worker_kind=sweep.SWEEP_WORKER,
            discord_thread_id="thread-workshop",
            context={"journal_digest": "journal/digest.md"},
        )
        session.add(work)
        session.flush()
        result = sweep.sweep_worker(work)
        assert result["produces"]["silent"] and result["produces"]["new"] == 1
        run = session.get(WorkflowRun, result["produces"]["triage_run_id"])
        assert run.name == triage.WORKFLOW_NAME and run.discord_thread_id == "thread-workshop"
        assert [item["key"] for item in run.input["items"]] == ["fault:aaa"]
        assert run.input["journal_digest"] == str(get_settings().resolved_data_dir / "journal" / "digest.md")
        again = sweep.sweep_worker(work)
        assert again["produces"]["triage_run_id"] is None  # one triage at a time
        triage_node = run.definition.definition["nodes"][0]
        denied = triage_node["runtime_contract"]["disallowed_tools"]
        assert {"Bash", "Write", "Edit", "mcp__tasque2__memory_create"} <= set(denied)
        assert "mcp__tasque2__memory_recall" not in denied
    assert json.loads(json.dumps(result["produces"]))  # plain data
    with session_scope() as session:
        assert session.scalars(select(WorkflowRun).where(WorkflowRun.name == triage.WORKFLOW_NAME)).all()
