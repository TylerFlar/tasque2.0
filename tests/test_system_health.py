from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path

from sqlalchemy import update

from tasque2.daemon.control import state_path, write_state
from tasque2.daemon.restart import request_path, result_path
from tasque2.db import session_scope
from tasque2.mcp import tools
from tasque2.models import FailedWork, Schedule, WorkAttempt, WorkflowNode, utc_now
from tasque2.ops.health import build_system_health
from tasque2.work.repository import WorkRepository
from tasque2.workflows import WorkflowService


def _item(session, title: str, lane: str, status: str):
    item = WorkRepository(session).create_work_item(
        title=title, task_instruction="Do it.", worker_kind="manual", lane=lane
    )
    item.status = status
    session.flush()
    return item


def _attempt(session, item, number: int, status: str, *, minutes_ago: int, minutes_long: int = 2, **fields):
    now = utc_now()
    started = now - timedelta(minutes=minutes_ago)
    attempt = WorkAttempt(
        work_item_id=item.id,
        attempt_number=number,
        status=status,
        worker_kind="manual",
        started_at=started,
        heartbeat_at=fields.pop("heartbeat_at", started),
        ended_at=None if status == "running" else started + timedelta(minutes=minutes_long),
        **fields,
    )
    session.add(attempt)
    session.flush()
    attempt.created_at = started
    session.flush()
    return attempt


def test_a_quiet_week_needs_nothing(fresh_db: Path) -> None:
    write_state(started_at=utc_now(), in_flight_attempt_ids=[], draining=False, version="t")
    with session_scope() as session:
        item = _item(session, "Morning page", "daybook", "succeeded")
        _attempt(session, item, 1, "succeeded", minutes_ago=60)
        health = build_system_health(session)
    assert health["daemon"]["alive"] is True
    assert health["attention"] == []
    assert health["totals"]["succeeded_attempts"] == 1 and health["totals"]["failed_attempts"] == 0


def test_failures_dead_letters_stuck_and_long_runs_are_named(fresh_db: Path) -> None:
    write_state(started_at=utc_now(), in_flight_attempt_ids=[], draining=False, version="t")
    with session_scope() as session:
        recovered = _item(session, "Coach reply", "health-coach", "succeeded")
        _attempt(
            session,
            recovered,
            1,
            "failed",
            minutes_ago=120,
            error_type="TransientProviderError",
            error_message="The worker did not call submit_worker_result.",
        )
        _attempt(session, recovered, 2, "succeeded", minutes_ago=110)
        dead = _item(session, "Weekly review", "money-review", "dead_letter")
        failed = _attempt(
            session, dead, 1, "failed", minutes_ago=90, error_type="LeaseExpired", error_message="lease expired"
        )
        session.add(
            FailedWork(
                work_item_id=dead.id,
                attempt_id=failed.id,
                status="unresolved",
                error_type="LeaseExpired",
                error_message="lease expired",
            )
        )
        hung = _item(session, "Inbox sweep", "daily-gmail-cleanup", "running")
        _attempt(session, hung, 1, "running", minutes_ago=50)
        slow = _item(session, "Apply: Example", "career-apply-weekly", "succeeded")
        _attempt(session, slow, 1, "succeeded", minutes_ago=300, minutes_long=70)
        old = _item(session, "Old failure", "career", "dead_letter")
        stale = _attempt(session, old, 1, "failed", minutes_ago=60 * 24 * 10, error_type="TransientProviderError")
        assert stale.created_at < utc_now() - timedelta(days=7)
        health = build_system_health(session, days=7)
    failures = health["failures_by_lane"]
    assert failures["health-coach"]["recovered"] == 1 and failures["health-coach"]["still_failing"] == 0
    assert failures["money-review"]["still_failing"] == 1
    assert "career" not in failures  # outside the window
    assert health["dead_letters"][0]["lane"] == "money-review"
    assert health["stuck"][0]["lane"] == "daily-gmail-cleanup" and health["stuck"][0]["minutes_silent"] >= 50
    assert health["long_runs"][0]["minutes"] == 70
    attention = " | ".join(health["attention"])
    assert "dead letter in money-review: Weekly review" in attention
    assert "money-review: 1 item(s) failed and did not recover" in attention
    assert "stuck in daily-gmail-cleanup" in attention
    assert "health-coach" not in attention


def test_a_silent_daemon_and_stale_schedules(fresh_db: Path) -> None:
    with session_scope() as session:
        health = build_system_health(session)
    assert health["daemon"]["alive"] is False and "the daemon is not ticking" in health["attention"]
    write_state(started_at=utc_now(), in_flight_attempt_ids=[], draining=False, version="t")
    with session_scope() as session:
        session.add(
            Schedule(
                name="nightly",
                enabled=True,
                schedule_type="cron",
                expression="0 3 * * *",
                timezone="UTC",
                payload={},
                worker_kind="manual",
                last_evaluated_at=utc_now() - timedelta(hours=2),
            )
        )
        session.flush()
        health = build_system_health(session)
    assert health["stale_schedules"][0]["name"] == "nightly"
    assert "1 enabled schedule(s) not evaluated in 15 min" in health["attention"]


def test_the_tool_reports_the_same(fresh_db: Path) -> None:
    write_state(started_at=utc_now(), in_flight_attempt_ids=[], draining=False, version="t")
    data = json.loads(tools.system_health(days=3, intent="weekly check"))
    assert data["ok"] is True and data["window"]["days"] == 3 and data["attention"] == []
    assert state_path().exists()


def test_a_rolled_back_restart_a_stalled_one_and_a_long_waiting_choice_are_named(fresh_db: Path) -> None:
    write_state(started_at=utc_now(), in_flight_attempt_ids=[], draining=False, version="t")
    now = utc_now()
    result_path().write_text(
        json.dumps(
            {
                "reason": "repair 1",
                "ok": False,
                "rolled_back": ["core"],
                "pushed": [],
                "ended_at": (now - timedelta(days=1)).isoformat(),
            }
        ),
        encoding="utf-8",
    )
    request_path().write_text(
        json.dumps(
            {"requested_at": (now - timedelta(days=3)).isoformat(), "reason": "repair 2", "window": "now", "switch": []}
        ),
        encoding="utf-8",
    )
    with session_scope() as session:
        _item(session, "Queued", "daybook", "ready")
        service = WorkflowService(session)
        definition = service.create_definition(
            name="repair",
            version="1",
            definition={"nodes": [{"key": "approve", "kind": "gate", "prompt": "Merge?", "choices": ["Yes", "No"]}]},
        )
        service.start_run(workflow_definition_id=definition.id)
        service.tick_runs()
        session.execute(update(WorkflowNode).values(updated_at=now - timedelta(days=8)))
        health = build_system_health(session, now=now)

    attention = health["attention"]
    assert any("did not come up and was rolled back" in line for line in attention)
    assert any("a restart (repair 2) has waited since" in line and "work is waiting" in line for line in attention)
    assert any("repair has waited 8 days for a choice: Merge?" in line for line in attention)
    assert health["restarts"]["pending"]["waiting"] == "work is waiting"
    assert health["waiting_choices"][0]["gate"] == "approve"


def test_an_old_restart_outcome_is_not_raised_again(fresh_db: Path) -> None:
    write_state(started_at=utc_now(), in_flight_attempt_ids=[], draining=False, version="t")
    old = (utc_now() - timedelta(days=10)).isoformat()
    result_path().write_text(json.dumps({"reason": "x", "ok": False, "ended_at": old}), encoding="utf-8")
    with session_scope() as session:
        health = build_system_health(session)
    assert health["attention"] == [] and health["restarts"]["last"] is None
