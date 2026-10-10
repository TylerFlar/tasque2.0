"""The Workshop end to end, on fixture repositories with scripted plan and build runs: a tweak ships on
its own and undoes, a feature waits for one tap, a plan with questions waits for its OK, ideas end at the
plan, a code change holds the code until it is live, and a cold release is announced and undone."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import select
from workshop_shop import (
    TEMPLATE,
    THREAD,
    _commit,
    _extension_build,
    _gate,
    _git,
    _other_build,
    _out,
    _plan,
    _template_build,
    drive,
    open_shop,
)

from tasque2.daemon.restart import clear_request, read_request
from tasque2.daemon.service import Daemon
from tasque2.db import session_scope
from tasque2.models import WorkflowNode, WorkflowRun, WorkItem, utc_now
from tasque2.ops import datarepo
from tasque2.ops.release import find_plan, history, save_plan, state
from tasque2.workflows import WorkflowService
from tasque2.workshop import pipeline, reply


@pytest.fixture()
def shop(fresh_db: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    return open_shop(tmp_path, monkeypatch)


# --- the paths ------------------------------------------------------------------------------------------------


def test_a_tweak_ships_on_its_own_and_undoes(shop: dict[str, Any]) -> None:
    with session_scope() as session:
        run = pipeline.start_change(session, request="make cooking replies shorter", thread_id=THREAD)
        change_id, root = run.input["change_id"], Path(run.input["cwd"])
        assert (root / "data" / TEMPLATE).is_file() and (root / "extensions" / "ext1" / "tool.py").is_file()
        drive(session, {"plan": _plan("Shorter cooking replies", "tweak", ["templates"]), "build": _template_build})
        run = session.get(WorkflowRun, run.id)
        assert run.status == "completed"
        assert _out(session, run.id, "plan_gate")["skipped"] and _out(session, run.id, "ship_gate")["skipped"]
        assert _out(session, run.id, "report")["undo_release"] == change_id
        text, _ = pipeline.report_text(session, run)
        assert text.startswith("**Live now: Shorter cooking replies**") and "shorter replies" in text
        assert shop["suites"] == []  # no code, no suites
        assert (shop["data"] / TEMPLATE).read_text(encoding="utf-8") == "Reply, briefly.\n"
        assert not root.exists() and state(find_plan(change_id)) == "live" and find_plan(change_id).tier == "auto"
        assert pipeline.undo_by_id(session, change_id) == "Undone: Shorter cooking replies is reversed."
    assert (shop["data"] / TEMPLATE).read_text(encoding="utf-8") == "Reply.\n"
    assert state(find_plan(change_id)) == "undone"


def test_a_config_change_ships_when_the_live_config_moved_on_while_it_was_built(shop: dict[str, Any]) -> None:
    def build_while_others_move(item: WorkItem) -> dict[str, Any]:
        produced = _template_build(item)
        (shop["data"] / "work-templates" / "daybook").mkdir(parents=True, exist_ok=True)
        (shop["data"] / "work-templates" / "daybook" / "brief.template.md").write_text("Brief.\n", encoding="utf-8")
        datarepo.snapshot("before another change")  # another change began meanwhile
        return produced

    script = {"plan": _plan("Shorter cooking replies", "tweak", ["templates"]), "build": build_while_others_move}
    with session_scope() as session:
        run = pipeline.start_change(session, request="make cooking replies shorter", thread_id=THREAD)
        drive(session, script)
        text, release_id = pipeline.report_text(session, session.get(WorkflowRun, run.id))
        assert text.startswith("**Live now: Shorter cooking replies**") and release_id
    assert (shop["data"] / TEMPLATE).read_text(encoding="utf-8") == "Reply, briefly.\n"
    assert (shop["data"] / "work-templates" / "daybook" / "brief.template.md").is_file()
    assert _git(shop["data"], "branch", "--list", "workshop/*") == ""


def test_a_feature_waits_for_one_tap_and_a_code_change_waits_for_a_restart(shop: dict[str, Any]) -> None:
    with session_scope() as session:
        run = pipeline.start_change(session, request="let the tool count to two", thread_id=THREAD)
        change_id = run.input["change_id"]
        drive(session, {"plan": _plan("Count to two", "feature", ["extension"]), "build": _extension_build})
        gate = _gate(session, run.id)
        assert gate.node_key == "ship_gate" and _out(session, run.id, "plan_gate")["skipped"]
        card = gate.input["card"]
        assert card.startswith("**Workshop: Count to two** (feature; needs your tap")
        assert "ext1 2 files" in card and "Checks: core suite: 3 passed" in card and "rehearsal 2/2" in card
        assert pipeline.code_holder(session) is None  # a card waiting for a tap holds up no other code change
        assert reply.handle(session, text="nice", thread_id=THREAD, author="owner", referenced=None).startswith(
            "Noted for Count to two"
        )
        assert reply.handle(session, text="Ship", thread_id=THREAD, author="owner", referenced=None) == (
            "Ship: Count to two."
        )
        drive(session, {})
        run = session.get(WorkflowRun, run.id)
        assert run.status == "completed" and _out(session, run.id, "ship_gate")["notes"][0]["text"] == "nice"
        text, undo = pipeline.report_text(session, run)
        assert text.startswith("**Ready: Count to two**") and "at the next idle moment" in text and undo is None
        request = read_request()
        assert request["window"] == "now" and request["switch"][0]["ref"] == f"workshop/{change_id}"
        assert request["release"].endswith(f"{change_id}.json")
        assert pipeline.code_holder(session) == change_id  # taken again on Ship, until it is live
        plan = find_plan(change_id)
        assert plan.cold and plan.tier == "tap" and plan.thread_id == THREAD and plan.repos[0]["push"] is False


def test_a_plan_with_questions_waits_for_its_ok_and_the_answers_reach_the_build(shop: dict[str, Any]) -> None:
    seen: dict[str, Any] = {}

    def build(item: WorkItem) -> dict[str, Any]:
        seen.update(session.get(WorkflowNode, item.workflow_node_id).definition)
        seen["brief"] = WorkflowService(session)._output_reference(
            session.get(WorkflowRun, item.workflow_run_id), "prepare.brief"
        )
        seen["profile"] = item.runtime_contract["model_profile"]
        return _template_build(item)

    questions = [{"question": "How short?", "default": "two sentences"}]
    with session_scope() as session:
        run = pipeline.start_change(session, request="rethink the cooking replies", thread_id=THREAD)
        drive(session, {"plan": _plan("Cooking replies, rethought", "redesign", ["templates"], questions=questions)})
        gate = _gate(session, run.id)
        assert gate.node_key == "plan_gate"
        assert (
            "**Workshop plan: Cooking replies, rethought** (redesign; needs your OK: it has open questions)"
            in (gate.input["card"])
        )
        assert "1. How short? _(default: two sentences)_" in gate.input["card"]
        reply.handle(session, text="one sentence", thread_id=THREAD, author="owner", referenced=None)
        reply.handle(session, text="approve", thread_id=THREAD, author="owner", referenced=None)
        drive(session, {"build": build})
        assert seen["brief"]["answers"] == ["one sentence"] and seen["profile"] == "ultra"
        assert _gate(session, run.id).node_key == "ship_gate"  # a plan still gets its one tap
        reply.handle(session, text="discard", thread_id=THREAD, author="owner", referenced=None)
        drive(session, {})
        assert pipeline.report_text(session, session.get(WorkflowRun, run.id))[0] == (
            "Dropped: Cooking replies, rethought. Nothing changed."
        )
    assert (shop["data"] / TEMPLATE).read_text(encoding="utf-8") == "Reply.\n"
    assert _git(shop["data"], "branch", "--list", "workshop/*") == ""


def test_revise_plans_again_with_the_notes(shop: dict[str, Any]) -> None:
    questions = [{"question": "Which thread?", "default": "Kitchen"}]
    with session_scope() as session:
        first = pipeline.start_change(session, request="add a pantry view", thread_id=THREAD)
        drive(session, {"plan": _plan("A pantry view", "feature", ["extension"], questions=questions)})
        reply.handle(session, text="the Daybook instead", thread_id=THREAD, author="owner", referenced=None)
        reply.handle(session, text="revise", thread_id=THREAD, author="owner", referenced=None)
        drive(session, {"plan": _plan("A pantry view in the Daybook", "feature", ["extension"], questions=questions)})
        runs = pipeline.latest_runs(session, thread_id=THREAD)
        again = next(run for run in runs if run.id != first.id)
        assert again.input["revision_of"] == first.id and again.input["notes"] == ["the Daybook instead"]
        assert again.input["previous_plan"] == "1. A pantry view."
        assert pipeline.report_text(session, session.get(WorkflowRun, first.id))[0] == (
            "Planning A pantry view again with your notes."
        )


def test_ideas_and_questions_end_at_the_plan_with_its_answer(shop: dict[str, Any]) -> None:
    ideas = "1. A weekly money check-in.\n2. Birthday reminders from your contacts."
    with session_scope() as session:
        assert reply.handle(session, text="give me ideas", thread_id=THREAD, author="owner", referenced=None) == (
            "On it: thinking up ideas."
        )
        run = pipeline.latest_runs(session, thread_id=THREAD)[0]
        drive(session, {"plan": lambda item: {"no_change": True, "answer": ideas}})
        run = session.get(WorkflowRun, run.id)
        assert run.status == "completed" and pipeline.report_text(session, run) == (ideas, None)
        assert not Path(run.input["cwd"]).exists()


def test_a_crashed_plan_or_an_empty_build_changes_nothing(shop: dict[str, Any]) -> None:
    with session_scope() as session:
        crashed = pipeline.start_change(session, request="something", thread_id=THREAD)
        drive(session, {"plan": lambda item: None})
        assert pipeline.report_text(session, session.get(WorkflowRun, crashed.id))[0].startswith(
            "I couldn't plan this one (the planning run did not finish)"
        )
        empty = pipeline.start_change(session, request="shorter replies", thread_id=THREAD)
        drive(session, {"plan": _plan("Shorter", "tweak", ["templates"]), "build": lambda item: {"done": True}})
        text = pipeline.report_text(session, session.get(WorkflowRun, empty.id))[0]
        assert text.startswith("Not shipped: Shorter.\n- the build committed nothing")
        assert history() == []


def _go_live(ext: Path, change_id: str) -> None:
    """What the respawn does for a queued release: switch the code, record the outcome."""
    before = _git(ext, "rev-parse", "HEAD")
    _git(ext, "merge", "--ff-only", "-q", f"workshop/{change_id}")
    clear_request()
    plan = find_plan(change_id)
    plan.outcome = {"ok": True, "at": utc_now().isoformat(), "code": {str(ext): {"before": before, "after": "x"}}}
    plan.released_at = utc_now().isoformat()
    save_plan(plan)


def test_a_code_change_waits_while_another_holds_the_code(shop: dict[str, Any]) -> None:
    with session_scope() as session:
        first = pipeline.start_change(session, request="fix the other value", thread_id=THREAD)
        drive(session, {"plan": _plan("The other value", "fix", ["extension"]), "build": _other_build})
        assert pipeline.code_holder(session) == first.input["change_id"]  # shipped on its own: queued to go live
        second = pipeline.start_change(session, request="count to three", thread_id=THREAD)
        drive(session, {"plan": _plan("Count to three", "feature", ["extension"])})
        prepare = session.scalar(
            select(WorkItem).where(
                WorkItem.workflow_run_id == second.id, WorkItem.worker_kind == pipeline.PREPARE_WORKER
            )
        )
        assert prepare.status == "ready" and prepare.not_before is not None
        assert "waiting for another code change" in pipeline.status_text(session)
        assert reply.handle(session, text="undo", thread_id=THREAD, author="owner", referenced=None).startswith(
            "Withdrawn: The other value"
        )
        drive(session, {}, steps=1)  # the withdrawn release freed the code: the waiting change takes it
        session.refresh(prepare)
        assert prepare.status == "succeeded" and pipeline.code_holder(session) == second.input["change_id"]


def test_a_card_waiting_for_its_tap_holds_up_no_other_code_change(shop: dict[str, Any]) -> None:
    ext = shop["project"] / "extensions" / "ext1"
    with session_scope() as session:
        first = pipeline.start_change(session, request="count to two", thread_id=THREAD)
        drive(session, {"plan": _plan("Count to two", "feature", ["extension"]), "build": _extension_build})
        assert _gate(session, first.id).node_key == "ship_gate" and pipeline.code_holder(session) is None
        second = pipeline.start_change(session, request="fix the other value", thread_id=THREAD)
        drive(session, {"plan": _plan("The other value", "fix", ["extension"]), "build": _other_build})
        second_id = second.input["change_id"]
        assert session.get(WorkflowRun, second.id).status == "completed"  # built and queued while the card waited
        assert pipeline.code_holder(session) == second_id
        assert reply.handle(session, text="ship", thread_id=THREAD, author="owner", referenced=None) == (
            "Ship: Count to two."
        )
        drive(session, {})
        release = session.scalar(
            select(WorkItem).where(
                WorkItem.workflow_run_id == first.id, WorkItem.worker_kind == pipeline.RELEASE_WORKER
            )
        )
        assert release.status == "ready" and release.not_before is not None  # waiting for the second to go live
        checks = len(shop["suites"])
    _go_live(ext, second_id)
    with session_scope() as session:
        pipeline.announce_releases(session)
        drive(session, {})
        assert session.get(WorkflowRun, first.id).status == "completed"
        assert len(shop["suites"]) > checks  # checked again on the live code
        assert pipeline.code_holder(session) == first.input["change_id"]
        assert pipeline.report_text(session, session.get(WorkflowRun, first.id))[0].startswith(
            "**Ready: Count to two**"
        )
    branch = f"workshop/{first.input['change_id']}"
    assert _git(ext, "show", f"{branch}:other.py") == "OTHER = 1"  # moved onto the live code
    assert _git(ext, "show", f"{branch}:tool.py") == "VALUE = 2"
    assert _git(ext, "merge-base", "--is-ancestor", "HEAD", branch) == ""


def test_a_change_that_no_longer_applies_on_the_live_code_is_not_released(shop: dict[str, Any]) -> None:
    ext = shop["project"] / "extensions" / "ext1"
    with session_scope() as session:
        run = pipeline.start_change(session, request="count to two", thread_id=THREAD)
        drive(session, {"plan": _plan("Count to two", "feature", ["extension"]), "build": _extension_build})
        _commit(ext, "tool.py", "VALUE = 3\n", "changed live meanwhile")
        reply.handle(session, text="ship", thread_id=THREAD, author="owner", referenced=None)
        drive(session, {})
        text = pipeline.report_text(session, session.get(WorkflowRun, run.id))[0]
        assert text.startswith("Not shipped: Count to two.\n- it no longer applies on the live code")
        assert pipeline.code_holder(session) is None and read_request() is None


def test_a_cold_release_is_announced_once_it_is_live_and_undone_by_a_revert_release(shop: dict[str, Any]) -> None:
    with session_scope() as session:
        run = pipeline.start_change(session, request="fix the tool's value", thread_id=THREAD)
        drive(session, {"plan": _plan("The tool's value", "fix", ["extension"]), "build": _extension_build})
        change_id = run.input["change_id"]
        assert session.get(WorkflowRun, run.id).status == "completed"  # a fix with a test ships on its own
        plan = find_plan(change_id)
        assert plan.window == "quiet" and plan.tier == "auto"
    # what the respawn does: switch, record the outcome
    ext = shop["project"] / "extensions" / "ext1"
    before = _git(ext, "rev-parse", "HEAD")
    _git(ext, "merge", "--ff-only", "-q", f"workshop/{change_id}")
    clear_request()
    plan = find_plan(change_id)
    plan.outcome = {
        "ok": True,
        "at": utc_now().isoformat(),
        "code": {str(ext): {"before": before, "after": _git(ext, "rev-parse", "HEAD")}},
    }
    plan.released_at = utc_now().isoformat()
    save_plan(plan)
    with session_scope() as session:
        assert pipeline.announce_releases(session) == 1
        assert pipeline.announce_releases(session) == 0  # once
        item = session.scalar(select(WorkItem).where(WorkItem.worker_kind == pipeline.ANNOUNCE_WORKER))
        assert item.discord_thread_id == THREAD and item.task_instruction.startswith("**Live now: The tool's value**")
        assert pipeline.announce_worker(item)["produces"]["undo_release"] == change_id
        assert pipeline.code_holder(session) is None
        assert reply.handle(session, text="undo", thread_id=THREAD, author="owner", referenced=None) == (
            "Undoing The tool's value: the reverse goes live when Tasque restarts at the next idle moment."
        )
    undo = find_plan(f"undo-{change_id}")
    assert undo.undo_of == change_id and undo.repos[0]["branch"] == f"undo/{change_id}"
    _git(ext, "merge", "--ff-only", "-q", f"undo/{change_id}")
    assert (ext / "tool.py").read_text(encoding="utf-8") == "VALUE = 1\n"
    assert read_request()["release"].endswith(f"undo-{change_id}.json")


def _daemon_turn(daemon: Daemon, monkeypatch: pytest.MonkeyPatch, *, at: datetime) -> None:
    """One turn of the daemon's loop at ``at``: a tick, the release pass when one is due, then a stop."""

    async def stop(draining: bool) -> None:
        daemon._stop.set()

    monkeypatch.setattr("tasque2.daemon.service.utc_now", lambda: at)
    monkeypatch.setattr(daemon, "_sleep", stop)
    daemon._stop = asyncio.Event()
    asyncio.run(daemon._tick_loop())


def test_a_release_recorded_after_the_daemon_started_is_announced_by_its_loop(
    shop: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    ext, lock = shop["project"] / "extensions" / "ext1", pipeline._lock_path()

    def posts(session) -> list[WorkItem]:
        return list(session.scalars(select(WorkItem).where(WorkItem.worker_kind == pipeline.ANNOUNCE_WORKER)))

    def holder() -> str:
        return json.loads(lock.read_text(encoding="utf-8"))["change_id"]

    with session_scope() as session:
        first = pipeline.start_change(session, request="fix the other value", thread_id=THREAD)
        drive(session, {"plan": _plan("The other value", "fix", ["extension"]), "build": _other_build})
        second = pipeline.start_change(session, request="count to three", thread_id=THREAD)
        drive(session, {"plan": _plan("Count to three", "feature", ["extension"])})
        first_id, second_id = first.input["change_id"], second.input["change_id"]
        prepare_id = session.scalar(
            select(WorkItem.id).where(
                WorkItem.workflow_run_id == second.id, WorkItem.worker_kind == pipeline.PREPARE_WORKER
            )
        )
    daemon = Daemon(discord=False, max_claims=0)
    daemon._announce_releases()  # as it starts, the release still reads as queued
    with session_scope() as session:
        assert posts(session) == [] and session.get(WorkItem, prepare_id).not_before is not None
    assert holder() == first_id
    _go_live(ext, first_id)  # the respawn records the outcome once the new daemon is up and healthy
    later = utc_now() + timedelta(minutes=1)
    _daemon_turn(daemon, monkeypatch, at=later)
    with session_scope() as session:
        [post] = posts(session)
        assert post.discord_thread_id == THREAD and post.task_instruction.startswith("**Live now: The other value**")
        assert not lock.exists()
        prepare = session.get(WorkItem, prepare_id)
        assert prepare.status == "ready" and prepare.not_before is None  # woken
        drive(session, {}, steps=1)
        session.refresh(prepare)
        assert prepare.status == "succeeded"
    assert holder() == second_id
    _daemon_turn(daemon, monkeypatch, at=later + timedelta(minutes=1))
    with session_scope() as session:
        assert len(posts(session)) == 1
    assert holder() == second_id


def test_two_rollbacks_in_a_week_pause_the_workshop(shop: dict[str, Any]) -> None:
    from tasque2.ops.release import ReleasePlan

    def roll_back(number: int) -> None:
        save_plan(
            ReleasePlan(
                id=f"r{number}",
                change_id=f"r{number}",
                title=f"Release {number}",
                repos=[{"name": "ext1", "live": "x", "branch": "b", "base": "c"}],
                outcome={"ok": False, "at": utc_now().isoformat(), "error": "release-apply failed"},
                thread_id=THREAD,
            )
        )

    for number in (1, 2):
        roll_back(number)
    with session_scope() as session:
        assert pipeline.announce_releases(session) == 2
        texts = [item.task_instruction for item in session.scalars(select(WorkItem)).all()]
        assert all(text.startswith("**Not live: Release") and "release-apply failed" in text for text in texts)
    assert pipeline.paused() == "2 releases rolled back within a week"
    with session_scope() as session:
        with pytest.raises(pipeline.WorkshopPaused):
            pipeline.start_change(session, request="sweep", origin="workshop")
        run = pipeline.start_change(session, request="the owner's own ask still starts", thread_id=THREAD)
        assert run.input["paused"] is True
    assert pipeline.resume()
    with session_scope() as session:
        assert pipeline.announce_releases(session) == 0
    assert pipeline.paused() is None  # the owner's resume holds until another release is rolled back
    roll_back(3)
    with session_scope() as session:
        assert pipeline.announce_releases(session) == 1
    assert pipeline.paused() == "3 releases rolled back within a week"


# --- the thread ----------------------------------------------------------------------------------------------


def test_the_thread_takes_commands_and_gathers_quick_messages(shop: dict[str, Any]) -> None:
    with session_scope() as session:
        assert reply.handle(session, text="Pause.", thread_id=THREAD, author="owner", referenced=None).startswith(
            "Paused"
        )
        assert pipeline.paused() == "paused by owner"
        assert reply.handle(session, text="resume", thread_id=THREAD, author="owner", referenced=None) == "Resumed."
        assert reply.handle(session, text="undo", thread_id=THREAD, author="owner", referenced=None).startswith(
            "Nothing to undo"
        )
        assert reply.handle(
            session, text="make replies shorter", thread_id=THREAD, author="owner", referenced=None
        ).startswith("On it")
        WorkflowService(session).tick_runs()
        assert reply.handle(session, text="and friendlier", thread_id=THREAD, author="owner", referenced=None) == (
            "Added to the change I just started."
        )
        run = pipeline.latest_runs(session, thread_id=THREAD)[0]
        assert run.input["request"] == "make replies shorter\nand friendlier"
        plan_item = session.scalar(select(WorkItem).where(WorkItem.workflow_run_id == run.id))
        assert plan_item.context["request"] == "make replies shorter\nand friendlier"
        status = reply.handle(session, text="status", thread_id=THREAD, author="owner", referenced=None)
        assert "make replies shorter" in status and "planning" in status
        run.created_at = utc_now() - timedelta(minutes=5)
        session.flush()
        reply.handle(session, text="new: a pantry view", thread_id=THREAD, author="owner", referenced=None)
        assert pipeline.latest_runs(session, thread_id=THREAD)[0].input["request"] == "a pantry view"


def test_the_workshop_runs_read_their_checkout_and_build_inside_it(shop: dict[str, Any]) -> None:
    nodes = {node["key"]: node for node in pipeline.definition()["nodes"]}
    assert nodes["plan"]["runtime_contract"]["guard"] == {"mode": "plan"}
    assert nodes["build"]["runtime_contract"]["guard"] == {"mode": "build"}
    assert nodes["plan"]["lane"] == "workshop-plan" and nodes["build"]["lane"] == "workshop-build"
    assert nodes["build"]["contract_from"] == {"model_profile": "classify.profile"}
    assert nodes["plan_gate"]["skip_when"] == "classify.skip_plan_gate"
    assert nodes["ship_gate"]["skip_when"] == "verify.skip_ship_gate"
    assert "mcp__tasque2__memory_save" in nodes["build"]["runtime_contract"]["disallowed_tools"]
