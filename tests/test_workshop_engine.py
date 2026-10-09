"""What the Workshop needs from the engine: nodes that skip themselves, take contract values and lanes,
outlive a tolerated failure; workers that wait without failing; restarts never replaced; the tier policy;
the Undo button."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import select

from tasque2.daemon.restart import RestartBusy, read_request, request_restart, waiting_reason
from tasque2.db import session_scope
from tasque2.discord.ui import DiscordUIAction, DiscordUIService, build_undo_view, parse_custom_id, result_view
from tasque2.models import WorkAttempt, WorkflowNode, WorkflowRun, WorkItem, utc_now
from tasque2.work.queue import WorkQueue
from tasque2.work.runner import FunctionWorkerRegistry, WorkDeferred, WorkRunner, default_function_registry
from tasque2.workflows import WorkflowService
from tasque2.workshop import policy


def _node(session, run_id: str, key: str) -> WorkflowNode:
    return session.scalar(
        select(WorkflowNode).where(WorkflowNode.workflow_run_id == run_id, WorkflowNode.node_key == key)
    )


def _start(session, nodes: list[dict[str, Any]], **run: Any) -> tuple[WorkflowService, WorkflowRun]:
    service = WorkflowService(session)
    definition = service.create_definition(name="engine", version="1", definition={"nodes": nodes})
    return service, service.start_run(workflow_definition_id=definition.id, **run)


def _echo(key: str, **extra: Any) -> dict[str, Any]:
    return {"key": key, "title": key, "task_instruction": key, "worker_kind": "function.echo", **extra}


# --- the workflow engine -----------------------------------------------------------------------------------


def test_a_work_node_skips_itself_when_an_upstream_output_says_so(fresh_db: Path) -> None:
    registry = FunctionWorkerRegistry()
    registry.register("function.decide", lambda item: {"summary": "decided", "produces": {"stop": True}})
    nodes = [
        {"key": "decide", "title": "decide", "task_instruction": "x", "worker_kind": "function.decide"},
        _echo("build", depends_on=["decide"], skip_when="decide.stop"),
        _echo("report", depends_on=["build"]),
    ]
    with session_scope() as session:
        service, run = _start(session, nodes)
        service.tick_runs()
        WorkRunner(session, registry=registry).run_next()
        service.tick_runs()
        build = _node(session, run.id, "build")
        assert build.status == "succeeded" and build.output == {"skipped": True} and build.work_item_id is None
        assert _node(session, run.id, "report").status == "enqueued"


def test_a_node_takes_its_lane_and_contract_values_from_upstream(fresh_db: Path) -> None:
    registry = FunctionWorkerRegistry()
    registry.register("function.pick", lambda item: {"summary": "picked", "produces": {"profile": "ultra"}})
    nodes = [
        {"key": "pick", "title": "pick", "task_instruction": "x", "worker_kind": "function.pick"},
        _echo(
            "build",
            depends_on=["pick"],
            lane="workshop-build",
            runtime_contract={"model_profile": "high", "guard": {"mode": "build"}},
            contract_from={"model_profile": "pick.profile", "effort": "pick.missing"},
        ),
    ]
    with session_scope() as session:
        service, run = _start(session, nodes, input={"lane": "workshop"})
        service.tick_runs()
        assert session.get(WorkItem, _node(session, run.id, "pick").work_item_id).lane == "workshop"
        WorkRunner(session, registry=registry).run_next()
        service.tick_runs()
        build = session.get(WorkItem, _node(session, run.id, "build").work_item_id)
        assert build.lane == "workshop-build"
        assert build.runtime_contract == {"model_profile": "ultra", "guard": {"mode": "build"}}


def test_a_tolerated_failure_lets_the_next_node_start(fresh_db: Path) -> None:
    registry = default_function_registry()

    def boom(item: WorkItem) -> None:
        raise RuntimeError("the model run crashed")

    registry.register("function.boom", boom)
    nodes = [
        {
            "key": "plan",
            "title": "plan",
            "task_instruction": "x",
            "worker_kind": "function.boom",
            "tolerate_failure": True,
        },
        _echo("classify", depends_on=["plan"]),
    ]
    with session_scope() as session:
        service, run = _start(session, nodes)
        service.tick_runs()
        WorkRunner(session, registry=registry).run_next()
        service.tick_runs()
        assert _node(session, run.id, "plan").status == "failed_tolerated"
        assert _node(session, run.id, "classify").status == "enqueued"
        WorkRunner(session, registry=registry).run_next()
        service.tick_runs()
        assert session.get(WorkflowRun, run.id).status == "completed"


# --- a worker that waits ---------------------------------------------------------------------------------------


def test_a_deferred_worker_waits_without_failing_and_runs_again(fresh_db: Path) -> None:
    calls: list[str] = []

    def waits_once(item: WorkItem) -> dict[str, Any]:
        calls.append(item.id)
        if len(calls) == 1:
            raise WorkDeferred(utc_now() + timedelta(hours=2), "waiting for another change")
        return {"summary": "done"}

    registry = FunctionWorkerRegistry()
    registry.register("function.waits", waits_once)
    with session_scope() as session:
        item = WorkItem(title="w", task_instruction="x", worker_kind="function.waits", max_attempts=1)
        session.add(item)
        session.flush()
        outcome = WorkRunner(session, registry=registry).run_next()
        assert outcome.status == "ready" and outcome.summary == "waiting for another change"
        assert item.not_before is not None and item.max_attempts == 2
        assert session.scalar(select(WorkAttempt).where(WorkAttempt.work_item_id == item.id)).status == "deferred"
        assert WorkRunner(session, registry=registry).run_next() is None  # not before its time
        assert WorkQueue(session).ready_count(now=utc_now() + timedelta(hours=3)) == 1
    with session_scope() as session:
        item = session.scalars(select(WorkItem)).one()
        item.not_before = None
        session.flush()
        assert WorkRunner(session, registry=registry).run_next().status == "succeeded"


def test_a_deferred_item_does_not_hold_a_restart(fresh_db: Path) -> None:
    request = {"requested_at": utc_now().isoformat(), "reason": "t", "window": "now", "switch": []}
    with session_scope() as session:
        later = WorkItem(
            title="w", task_instruction="x", worker_kind="function.echo", not_before=utc_now() + timedelta(hours=1)
        )
        session.add(later)
        session.flush()
        assert waiting_reason(session, request) is None
        later.not_before = None
        session.flush()
        assert waiting_reason(session, request) == "work is waiting"


def test_a_pending_switch_is_never_replaced(isolated: Path, tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    request_restart(reason="release one", window="now", switch=[{"repo": str(repo), "ref": "a"}])
    with pytest.raises(RestartBusy, match="release one"):
        request_restart(reason="release two", window="now", switch=[{"repo": str(repo), "ref": "b"}])
    assert read_request()["reason"] == "release one"


# --- the tiers --------------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("origin", "kind", "touches", "questions", "tier"),
    [
        ("user", "tweak", ["templates"], 0, policy.AUTO),
        ("user", "tweak", ["doctrine", "extension"], 0, policy.AUTO),
        ("fault", "fix", ["extension"], 0, policy.AUTO),
        ("workshop", "fix", ["templates"], 0, policy.AUTO),
        ("user", "tweak", ["templates"], 1, policy.PLAN),
        ("user", "feature", ["extension"], 0, policy.TAP),
        ("workshop", "feature", ["schedules"], 0, policy.TAP),
        ("user", "fix", ["core"], 0, policy.TAP),
        ("user", "tweak", ["migrations"], 0, policy.TAP),
        ("user", "redesign", ["templates"], 0, policy.PLAN),
        ("user", "tweak", ["goals"], 0, policy.PLAN),
        ("user", "tweak", ["lanes"], 0, policy.PLAN),
        ("user", "fix", ["self"], 0, policy.PLAN),
        ("model", "fix", ["templates"], 0, policy.PLAN),
    ],
)
def test_the_tier_comes_from_who_asked_and_what_it_touches(origin, kind, touches, questions, tier) -> None:
    assert policy.classify(origin=origin, kind=kind, touches=touches, questions=questions)[0] == tier


def test_the_real_diff_only_raises_the_tier() -> None:
    assert policy.touches_from_diff({"data": ["work-templates/cooking/reply.template.md"]}) == {"data"}
    assert policy.touches_from_diff({"data": ["lanes.json"]}) == {"data", "schedules"}
    assert policy.touches_from_diff({"core": ["src/tasque2/workshop/policy.py"]}) == {"core", "self"}
    assert policy.touches_from_diff({"personal": ["migrations/0009.py"]}) == {"extension", "migrations"}
    assert policy.final_tier(policy.AUTO, origin="user", kind="tweak", files={"core": ["src/app.py"]})[0] == policy.TAP
    planned = policy.final_tier(policy.PLAN, origin="user", kind="tweak", files={"data": ["workflows/x.json"]})
    assert planned == (policy.PLAN, "as planned")


# --- the Undo button ----------------------------------------------------------------------------------------


def test_a_report_with_a_release_gets_an_undo_button_that_reverses_it(fresh_db: Path, monkeypatch) -> None:
    pytest.importorskip("discord")
    view = build_undo_view("20261009-101500-ab12")
    assert [child.label for child in view.children] == ["Undo"]
    action = parse_custom_id(view.children[0].custom_id)
    assert action == DiscordUIAction("workshop", "undo", "20261009-101500-ab12")
    assert result_view({"undo_release": "x"}, "controls") is not None
    assert result_view({}, "controls") == "controls"
    seen: list[str] = []
    monkeypatch.setattr(
        "tasque2.workshop.pipeline.undo_by_id", lambda session, release_id: seen.append(release_id) or "Undone."
    )
    with session_scope() as session:
        assert DiscordUIService(session).handle_action(action) == "Undone."
    assert seen == ["20261009-101500-ab12"]
