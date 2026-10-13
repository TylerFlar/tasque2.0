"""The weekly refactor pass, end to end on fixture repositories: it reads at ultra and never writes, runs one
step at a time, files its step into a free slot as the Workshop's own change with its files and evidence (never
among triage's new items), records nothing for a pass that failed or found no step, leaves a step alone whose
row is open or resting, and files nothing while the Workshop is paused."""

from __future__ import annotations

from collections.abc import Callable
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import or_, select
from workshop_shop import THREAD, _out, _plan, _template_build, drive, open_shop

from tasque2.db import session_scope
from tasque2.models import WorkflowRun, WorkItem, utc_now
from tasque2.work.queue import WorkQueue
from tasque2.workflows import WorkflowService
from tasque2.workshop import ledger, pipeline, refactor, triage
from tasque2.workshop.ledger import Signal

STEP = {
    "title": "Weather out of the core",
    "request": "Weather sits in the core with a home location as its default. Move it to an optional part; the "
    "same tool keeps working; update the map.",
    "files": ["src/tasque2/weather.py", "docs/architecture.md"],
    "evidence": ["weather.py is in the core", "its default location is one person's"],
}
KEY = "refactor:weather-out-of-the-core"


@pytest.fixture()
def shop(fresh_db: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    return open_shop(tmp_path, monkeypatch)


def _pass(session) -> WorkflowRun | None:
    """The pass's schedule fires: the run it started, or None."""
    work = WorkItem(
        title="Workshop refactor pass",
        task_instruction="x",
        worker_kind=refactor.LAUNCH_WORKER,
        discord_thread_id=THREAD,
    )
    session.add(work)
    session.flush()
    produced = refactor.launch_worker(work)["produces"]
    assert produced["silent"] is True
    return session.get(WorkflowRun, produced["refactor_run_id"]) if produced.get("refactor_run_id") else None


def _answer(session, run: WorkflowRun, model: Callable[[WorkItem], dict[str, Any] | None]) -> dict[str, Any]:
    """The pass's model run answers (``model``: its produces, or None when it crashed) and its step is filed.
    Other work, such as a change waiting for its plan, is held meanwhile and stays where it was."""
    WorkflowService(session).tick_runs()
    held = list(
        session.scalars(
            select(WorkItem).where(
                WorkItem.status == "ready",
                or_(WorkItem.workflow_run_id.is_(None), WorkItem.workflow_run_id != run.id),
            )
        ).all()
    )
    queue = WorkQueue(session)
    for item in held:
        queue.pause_work(item.id)
    drive(session, {"refactor": model}, steps=2)
    for item in held:
        queue.resume_work(item.id)
    return _out(session, run.id, "file")


def _step(step: dict[str, Any] | None, why: str | None = None) -> Callable[[WorkItem], dict[str, Any]]:
    return lambda item: {"step": step, "why": why, "silent": True}


def _fixes(session, *names: str) -> None:
    """Fixes triage proposed, one file each, filed into the Workshop's slots."""
    ledger.record(session, [Signal(key=f"warning:{name}", source="warning", title=f"Fix {name}") for name in names])
    for name in names:
        proposal = {"id": name, "title": f"Fix {name}", "request": f"Fix {name}.", "files": [f"src/{name}.py"]}
        ledger.propose(session, [f"warning:{name}"], proposal)
    triage.file_waiting(session, thread_id=THREAD)


def test_the_pass_reads_at_ultra_never_writes_and_runs_one_at_a_time(shop: dict[str, Any]) -> None:
    with session_scope() as session:
        _fixes(session, "a")
        first = _pass(session)
        node = {node["key"]: node for node in first.definition.definition["nodes"]}["refactor"]
        assert node["runtime_contract"]["model_profile"] == "ultra" and node["lane"] == "workshop-refactor"
        denied = set(node["runtime_contract"]["disallowed_tools"])
        assert {"Write", "Edit", "Bash", "Agent", "mcp__tasque2__memory_save"} <= denied
        assert "WebSearch" not in denied  # it may read how similar tools are organized
        assert first.input["history"] == [] and first.input["waiting"] == []
        [open_change] = first.input["open_changes"]  # titled by its request until its plan names it
        assert (open_change["title"], open_change["origin"]) == ("Fix a.", "workshop")
        assert (open_change["files"], open_change["stage"]) == (["src/a.py"], "planning")
        assert first.input["memory_canonical_keys"] == pipeline.DOCTRINE_KEYS
        assert _pass(session) is None  # a pass is still open
        assert _answer(session, first, _step(STEP))["state"] == "filed"
        assert _pass(session) is None  # its step is still open


def test_the_step_is_filed_as_the_workshops_own_change_never_triaged_and_ships_quietly(
    shop: dict[str, Any],
) -> None:
    with session_scope() as session:
        run = _pass(session)
        filed = _answer(session, run, _step(STEP))
        assert filed["silent"] is True
        assert (filed["step"], filed["key"], filed["state"]) == (STEP["title"], KEY, "filed")
        assert [change["title"] for change in filed["filed"]] == [STEP["title"]]
        change = session.get(WorkflowRun, filed["filed"][0]["run_id"])
        assert (change.input["origin"], change.input["author"]) == ("workshop", "workshop")
        assert change.input["request"] == STEP["request"] and change.input["files"] == sorted(STEP["files"])
        assert change.input["evidence"] == STEP["evidence"] and change.input["issues"] == [KEY]
        row = ledger.row(session, KEY)
        assert (row.kind, row.source, row.status) == ("issue", "refactor", "filed")
        assert row.change_id == change.input["change_id"]
        assert ledger.new_items(session) == [] and triage.start(session, thread_id=THREAD) is None
        drive(session, {"plan": _plan(STEP["title"], "tweak", ["templates"]), "build": _template_build})
        assert _out(session, change.id, "report")["silent"] is True  # the Workshop's own: shipped quietly
        row = ledger.row(session, KEY)
        assert row.status == "done" and row.change_id == change.input["change_id"]
        again = _pass(session)  # the step is done: the next pass may run, and sees it shipped
        today = utc_now().date().isoformat()
        assert again.input["history"] == [{"title": STEP["title"], "outcome": "shipped", "on": today}]
        left = _answer(session, again, _step(STEP))
        assert left["filed"] == [] and left["left"] == "shipped" and "state" not in left
        assert ledger.row(session, KEY).status == "done" and pipeline.open_changes(session) == []


def test_a_pass_that_failed_or_found_no_step_records_nothing(shop: dict[str, Any]) -> None:
    with session_scope() as session:
        crashed = _pass(session)
        assert _answer(session, crashed, lambda item: None) == {"silent": True, "step": None, "filed": []}
        assert ledger.counts(session) == {} and pipeline.open_changes(session) == []
        empty = _pass(session)  # a pass that failed holds nothing: the next may start at once
        found = _answer(session, empty, _step(None, "nothing stands in the way this week"))
        assert found == {"silent": True, "step": None, "filed": []}
        half = _pass(session)
        assert _answer(session, half, _step({"title": "Half a step"}))["step"] is None  # no request: no step
        assert ledger.counts(session) == {} and pipeline.open_changes(session) == []
        assert _pass(session) is not None


def test_a_step_waits_for_a_free_slot_and_a_paused_workshop_files_nothing(shop: dict[str, Any]) -> None:
    with session_scope() as session:
        _fixes(session, "a", "b", "c")  # the Workshop's three slots are taken
        run = _pass(session)
        assert [change["title"] for change in run.input["open_changes"]] == ["Fix a.", "Fix b.", "Fix c."]
        waiting = _answer(session, run, _step(STEP))
        assert waiting["state"] == "waiting for a slot" and waiting["filed"] == []
        row = ledger.row(session, KEY)
        assert row.status == "waiting" and row.proposal["files"] == STEP["files"]
        assert row.proposal["request"] == STEP["request"] and row.proposal["evidence"] == STEP["evidence"]
        assert _pass(session) is None  # the step still waits: one at a time
        assert "Waiting for a free slot:" in pipeline.status_text(session)
        drive(session, {"plan": lambda item: {"no_change": True, "answer": "fine as it is"}})  # the fixes end
        pipeline.pause("paused by hand")
        assert triage.file_waiting(session, thread_id=THREAD) == []  # paused: nothing is filed
        assert pipeline.resume()
        assert [change["title"] for change in triage.file_waiting(session, thread_id=THREAD)] == [STEP["title"]]
        assert ledger.row(session, KEY).status == "filed"


def test_the_file_step_files_nothing_while_the_workshop_is_paused(shop: dict[str, Any]) -> None:
    with session_scope() as session:
        run = _pass(session)
        pipeline.pause("paused by hand")
        waiting = _answer(session, run, _step(STEP))
        assert waiting["state"] == "waiting for a slot" and waiting["filed"] == []
        assert ledger.row(session, KEY).status == "waiting" and pipeline.open_changes(session) == []
        assert _pass(session) is None  # the step waits for the Workshop to resume


def test_a_resting_step_is_not_proposed_again_until_its_rest_ends(shop: dict[str, Any]) -> None:
    until = utc_now() + ledger.SET_ASIDE
    with session_scope() as session:
        ledger.record(session, [Signal(key=KEY, source="refactor", title=STEP["title"])])
        ledger.set_aside(session, [KEY], until=until)  # its change was discarded
        run = _pass(session)  # a resting step holds no slot
        assert run.input["history"][0]["outcome"] == f"set aside until {until.date().isoformat()}"
        left = _answer(session, run, _step(STEP))
        assert left["filed"] == [] and left["left"] == f"set aside until {until.date().isoformat()}"
        row = ledger.row(session, KEY)
        assert row.status == "resting" and row.proposal is None and pipeline.open_changes(session) == []
        row.rest_until = utc_now() - timedelta(days=1)  # its rest over, the same step may be proposed again
        session.flush()
        again = _pass(session)
        assert again.input["history"][0]["outcome"] == "set aside"
        assert _answer(session, again, _step(STEP))["state"] == "filed"
        assert ledger.row(session, KEY).status == "filed"
