"""Goal reviews and the monthly look: a goal off plan by its own count, or one the look names, gets a deep
review of its own (one at a time, planned at ultra, always a pitch, silent when nothing should change), the
fixes keep their slots beside it, and the look runs at most every two weeks, reads how each thread is
answered, and keeps what it found in the ledger without posting."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from workshop_shop import THREAD, _gate, _out, _plan, _template_build, drive, open_shop

from tasque2.db import session_scope
from tasque2.extensions import registry
from tasque2.models import DiscordMessage, DiscordThread, ProviderRun, WorkAttempt, WorkflowRun, WorkItem, utc_now
from tasque2.workflows import WorkflowService
from tasque2.workshop import ledger, look, pipeline, reviews, sweep, triage
from tasque2.workshop.ledger import Signal, aware


@pytest.fixture()
def shop(fresh_db: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    return open_shop(tmp_path, monkeypatch)


def _count(session, goal: str, *, hours_ago: float = 0, off_plan_days: float | None = None) -> None:
    """What a goal's own count says on a sweep: it is off plan (since ``off_plan_days`` ago, when given)."""
    signal = Signal(
        key=ledger.review_key(goal),
        source="goal_off_plan",
        title=f"The {goal} goal is off plan",
        evidence=[f"{goal}: 3 weeks off plan"],
        since=utc_now() - timedelta(days=off_plan_days) if off_plan_days is not None else None,
        kind=ledger.REVIEW,
    )
    ledger.record(session, [signal], now=utc_now() - timedelta(hours=hours_ago))


def _no_change(item: WorkItem) -> dict[str, Any]:
    return {"no_change": True, "answer": "The method is right; the weeks off were a trip."}


# --- goal reviews -------------------------------------------------------------------------------------------


def test_an_extension_count_that_names_a_goal_is_its_review_row_and_never_triages(
    fresh_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def counts(session, now) -> list[dict[str, Any]]:
        return [
            {
                "key": "garden",
                "goal": "garden",
                "source": "goal_off_plan",
                "title": "The garden goal is off plan: 4 weeks",
                "evidence": ["no bed planted since 09-01"],
                "since": "2026-09-01",
            },
            {"key": "decision:fence", "title": "A decision took its default", "evidence": ["asked 10-01"]},
        ]

    monkeypatch.setattr(registry(), "signal_sources", [("goals", counts)])
    now = utc_now()
    with session_scope() as session:
        ledger.record(session, sweep.gather(session, now=now), now=now)
        review = ledger.row(session, "goal_review:garden")
        assert (review.kind, review.status, review.source) == ("review", "new", "goal_off_plan")
        assert review.evidence == ["no bed planted since 09-01"]
        assert ledger.row(session, "goals:decision:fence").kind == "issue"
        assert [found.key for found in ledger.new_items(session)] == ["goals:decision:fence"]
        assert ledger.counts(session) == {"new": 1}
        run = triage.start(session, thread_id=THREAD)
        assert [item["key"] for item in run.input["items"]] == ["goals:decision:fence"]  # triage never sees it
        lines = ledger.status_lines(session, now=now)
    assert lines[0] == "Ledger: 1 new."
    assert lines[-2:] == ["Goal reviews:", "- The garden goal is off plan: 4 weeks: waiting its turn"]


def test_one_goal_review_at_a_time_the_oldest_first_none_while_paused_and_never_a_stale_count(
    shop: dict[str, Any],
) -> None:
    with session_scope() as session:
        _count(session, "chess", hours_ago=72)  # its count stopped three days ago: back on plan
        _count(session, "garden", hours_ago=2)
        _count(session, "piano", hours_ago=1, off_plan_days=60)  # off plan longer, but it joined the queue last
        pipeline.pause("paused by hand")
        assert reviews.file_next(session, thread_id=THREAD) is None
        assert pipeline.resume()
        started = reviews.file_next(session, thread_id=THREAD)
        assert started["goal"] == "garden"
        run = session.get(WorkflowRun, started["run_id"])
        assert run.input["review"] == {"goal": "garden"} and run.input["origin"] == "workshop"
        assert run.input["request"] == "Goal review: The garden goal is off plan"
        assert run.input["issues"] == ["goal_review:garden"] and run.input["evidence"] == ["garden: 3 weeks off plan"]
        assert ledger.row(session, "goal_review:garden").status == "filed"
        assert reviews.file_next(session, thread_id=THREAD) is None  # one at a time
        assert "- The garden goal is off plan: under review" in pipeline.status_text(session)
        drive(session, {"plan": _no_change})
        assert session.get(WorkflowRun, run.id).status == "completed"
        assert _out(session, run.id, "report")["silent"] is True  # nothing to change: nothing posts
        garden = ledger.row(session, "goal_review:garden")
        assert garden.status == "resting" and aware(garden.rest_until) < utc_now() + timedelta(days=31)
        assert reviews.file_next(session, thread_id=THREAD)["goal"] == "piano"
        assert ledger.row(session, "goal_review:chess").status == "new"  # never started while its count is quiet


def test_a_goal_review_plans_at_ultra_and_always_pitches_as_a_redesign(shop: dict[str, Any]) -> None:
    seen: dict[str, Any] = {}

    def plan(item: WorkItem) -> dict[str, Any]:
        seen.setdefault("plans", []).append((item.runtime_contract["model_profile"], item.context.get("review")))
        if not item.context.get("review"):
            return _no_change(item)
        return _plan("Garden, rethought", "tweak", ["templates"], pitch="Two beds a week instead of five.")(item)

    def build(item: WorkItem) -> dict[str, Any]:
        seen["build"] = item.runtime_contract["model_profile"]
        return _template_build(item)

    with session_scope() as session:
        pipeline.start_change(session, request="is the reply short enough?", thread_id=THREAD)
        drive(session, {"plan": plan})
        _count(session, "garden")
        run_id = reviews.file_next(session, thread_id=THREAD)["run_id"]
        drive(session, {"plan": plan})
        assert seen["plans"] == [("high", None), ("ultra", {"goal": "garden"})]  # a plain change keeps its tier
        classify = _out(session, run_id, "classify")
        assert (classify["kind"], classify["tier"], classify["profile"]) == ("redesign", "plan", "ultra")
        card = _gate(session, run_id).input["card"]
        assert card.startswith(
            "**Workshop: Garden, rethought** (needs your OK: a redesign; the Workshop filed it)\n\n"
            "Two beds a week instead of five."
        )
        WorkflowService(session).answer_gate(workflow_run_id=run_id, node_key="plan_gate", answer="Approve")
        drive(session, {"build": build})
        assert seen["build"] == "ultra"
        text, release_id = pipeline.report_text(session, session.get(WorkflowRun, run_id))
        assert text.startswith("**Live now: Garden, rethought**") and release_id
        assert _out(session, run_id, "report").get("silent") is None  # approved: the owner hears it went live
        assert ledger.row(session, "goal_review:garden").status == "done"


def test_revise_keeps_a_goal_review_one_and_a_discard_rests_it_60_days(shop: dict[str, Any]) -> None:
    planned: list[tuple[str, dict[str, Any]]] = []

    def plan(item: WorkItem) -> dict[str, Any]:
        planned.append((item.runtime_contract["model_profile"], dict(item.context)))
        return _plan("Garden, rethought", "feature", ["templates"])(item)

    with session_scope() as session:
        _count(session, "garden")
        run = session.get(WorkflowRun, reviews.file_next(session, thread_id=THREAD)["run_id"])
        drive(session, {"plan": plan})
        service = WorkflowService(session)
        service.add_gate_note(workflow_run_id=run.id, node_key="plan_gate", text="fewer beds, more pots")
        service.answer_gate(workflow_run_id=run.id, node_key="plan_gate", answer="Revise")
        drive(session, {"plan": plan})
        profile, context = planned[-1]
        assert len(planned) == 2 and profile == "ultra"
        assert context["review"] == {"goal": "garden"} and context["notes"] == ["fewer beds, more pots"]
        assert _out(session, run.id, "classify")["kind"] == "redesign"
        assert _gate(session, run.id).node_key == "plan_gate"
        service.answer_gate(workflow_run_id=run.id, node_key="plan_gate", answer="Discard")
        drive(session, {})
        assert _out(session, run.id, "report")["silent"] is True
        garden = ledger.row(session, "goal_review:garden")
        assert garden.status == "resting" and aware(garden.rest_until) > utc_now() + timedelta(days=59)
        _count(session, "garden")  # its count still says off plan: the user's Discard holds
        assert garden.status == "resting" and reviews.file_next(session, thread_id=THREAD) is None


def test_the_fixes_keep_their_three_slots_beside_an_open_goal_review(shop: dict[str, Any]) -> None:
    with session_scope() as session:
        _count(session, "garden")
        assert reviews.file_next(session, thread_id=THREAD) is not None
        ledger.record(session, [Signal(key=f"warning:{name}", source="warning", title=name) for name in "abcd"])
        for name in "abcd":
            proposal = {"id": name, "title": f"Fix {name}", "request": f"Fix {name}.", "files": [f"src/{name}.py"]}
            ledger.propose(session, [f"warning:{name}"], proposal)
        filed = triage.file_waiting(session, thread_id=THREAD)
        assert [change["title"] for change in filed] == ["Fix a", "Fix b", "Fix c"]
        assert ledger.row(session, "warning:d").status == "waiting"


# --- the monthly look ------------------------------------------------------------------------------------------


def _look(session) -> WorkflowRun | None:
    """The look's schedule fires: the run it started, or None."""
    work = WorkItem(
        title="Workshop look",
        task_instruction="x",
        worker_kind=look.LAUNCH_WORKER,
        discord_thread_id=THREAD,
        context={"journal_digest": "journal/digest.md"},
    )
    session.add(work)
    session.flush()
    produced = look.launch_worker(work)["produces"]
    assert produced["silent"] is True
    return session.get(WorkflowRun, produced["look_run_id"]) if produced.get("look_run_id") else None


def _thread(session, thread_id: str, lane: str, *, private: bool = False) -> None:
    owner = WorkItem(title=f"{lane} opener", task_instruction="x", worker_kind="function.notify", lane=lane)
    owner.context = {"private": True} if private else {}
    session.add(owner)
    session.flush()
    session.add(
        DiscordThread(purpose="work", discord_channel_id="jobs", discord_thread_id=thread_id, work_item_id=owner.id)
    )


def _message(session, thread_id: str, text: str, *, days_ago: float, inbound: bool = False) -> None:
    session.add(
        DiscordMessage(
            discord_message_id=f"m-{thread_id}-{days_ago}",
            discord_channel_id="jobs",
            discord_thread_id=thread_id,
            direction="inbound" if inbound else "outbound",
            author="owner" if inbound else "tasque",
            content_preview=text,
            created_at=utc_now() - timedelta(days=days_ago),
        )
    )


def _prompt(session, lane: str, chars: int, *, days_ago: float) -> None:
    item = WorkItem(title=f"{lane} run", task_instruction="x", worker_kind="provider.default", lane=lane)
    session.add(item)
    session.flush()
    attempt = WorkAttempt(work_item_id=item.id, attempt_number=1, status="succeeded", worker_kind=item.worker_kind)
    session.add(attempt)
    session.flush()
    at = utc_now() - timedelta(days=days_ago)
    session.add(ProviderRun(attempt_id=attempt.id, provider="claude", usage={"prompt_chars": chars}, created_at=at))


def test_the_look_reads_each_threads_answers_outside_private_lanes_prompt_sizes_and_the_reviews(
    shop: dict[str, Any],
) -> None:
    with session_scope() as session:
        _thread(session, "t-garden", "garden")
        _thread(session, "t-diary", "diary", private=True)
        _message(session, "t-garden", "Water the beds today?", days_ago=3)
        _message(session, "t-garden", "done", days_ago=2.9, inbound=True)
        _message(session, "t-garden", "Plant the bulbs this week?", days_ago=40)
        _message(session, "t-diary", "Saved: 2 pages.", days_ago=1)
        _message(session, "t-diary", "thanks", days_ago=0.9, inbound=True)
        for chars, days in ((12_000, 5), (20_000, 6), (9_000, 35)):
            _prompt(session, "garden", chars, days_ago=days)
        _count(session, "piano")
        run = _look(session)
        given = run.input
    assert [entry["thread"] for entry in given["engagement"]] == ["garden"]  # a private lane is never read
    garden = given["engagement"][0]
    assert garden["last_28_days"] == {"posts": 1, "messages": 1, "asks": 1, "answered": 1, "pushback": 0}
    assert garden["28_days_before"] == {"posts": 1, "messages": 0, "asks": 1, "answered": 0, "pushback": 0}
    assert given["prompt_sizes"] == [
        {
            "lane": "garden",
            "last_28_days": {"runs": 2, "median": 16_000, "largest": 20_000},
            "28_days_before": {"runs": 1, "median": 9_000, "largest": 9_000},
        }
    ]
    assert given["goal_reviews"] == [
        {"key": "goal_review:piano", "title": "The piano goal is off plan", "status": "new", "rest_until": None}
    ]
    assert given["last_look"] is None and given["journal_digest"].endswith("digest.md")
    assert given["memory_canonical_keys"] == pipeline.DOCTRINE_KEYS


def test_the_looks_input_stays_small_the_quieter_threads_summed_and_only_the_largest_prompts_kept(
    shop: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(look, "THREADS_SHOWN", 1)
    monkeypatch.setattr(look, "LANES_SHOWN", 1)
    with session_scope() as session:
        for thread_id, lane, posts in (("t-garden", "garden", 3), ("t-cellar", "cellar", 2), ("t-attic", "attic", 1)):
            _thread(session, thread_id, lane)
            for number in range(posts):
                _message(session, thread_id, f"Update {number}?", days_ago=2 + number / 10)
        _prompt(session, "garden", 12_000, days_ago=2)
        _prompt(session, "cellar", 30_000, days_ago=2)
        given = _look(session).input
    garden, rest = given["engagement"]
    assert garden["thread"] == "garden" and garden["last_28_days"]["posts"] == 3
    assert rest["thread"] == "2 quieter thread(s), summed" and rest["thread_id"] is None
    assert rest["last_28_days"] == {"posts": 3, "messages": 0, "asks": 3, "answered": 0, "pushback": 0}
    assert [entry["lane"] for entry in given["prompt_sizes"]] == ["cellar"]


def test_the_look_runs_at_most_every_two_weeks_and_one_at_a_time(shop: dict[str, Any]) -> None:
    with session_scope() as session:
        first = _look(session)
        node = {node["key"]: node for node in first.definition.definition["nodes"]}["look"]
        assert node["runtime_contract"]["model_profile"] == "ultra" and node["lane"] == "workshop-look"
        assert {"Write", "Edit", "Bash"} <= set(node["runtime_contract"]["disallowed_tools"])
        assert _look(session) is None  # one is still open
        drive(session, {"look": lambda item: {"inventory": ["Garden: active, on track"], "silent": True}})
        assert session.get(WorkflowRun, first.id).status == "completed"
        assert _look(session) is None  # the last look was under two weeks ago
        first.created_at = utc_now() - timedelta(days=15)
        session.flush()
        second = _look(session)
        assert second.input["last_look"]["inventory"] == ["Garden: active, on track"]
        drive(session, {"look": lambda item: None})  # a look that fails is no look
        third = _look(session)
        assert third is not None and third.input["last_look"]["inventory"] == ["Garden: active, on track"]


def test_the_look_keeps_its_findings_for_triage_and_its_goals_as_reviews_and_posts_nothing(
    shop: dict[str, Any],
) -> None:
    named = [
        {"goal": "Garden_goal", "title": "Garden: the new term leaves no mornings", "evidence": ["classes at 8"]},
        {"goal": "chess", "title": "Chess: stalled", "evidence": ["no game in a month"]},
        {"goal": "piano", "title": "Piano: practice moved to evenings", "evidence": ["said so on 10-02"]},
    ]
    findings = [{"key": "Money asks wait", "title": "Money asks go unanswered", "evidence": ["3 of 5 waited"]}]
    findings += [{"title": f"Finding {number}"} for number in range(10)]
    with session_scope() as session:
        _count(session, "piano", hours_ago=1)
        _count(session, "chess")
        ledger.set_aside(session, ["goal_review:chess"], until=utc_now() + ledger.SET_ASIDE)  # discarded lately
        crashed = _look(session)
        drive(session, {"look": lambda item: None})
        assert _out(session, crashed.id, "file") == {"silent": True, "reviews": [], "findings": 0}
        assert ledger.row(session, "goal_review:garden") is None  # a failed look records nothing
        run = _look(session)  # a failed look is no look: the next may start at once
        produced = {"inventory": ["Garden: changed"], "reviews": named, "findings": findings, "silent": True}
        drive(session, {"look": lambda item: produced})
        filed = _out(session, run.id, "file")
        assert filed["silent"] is True and filed["reviews"] == ["garden", "piano"] and filed["skipped"] == ["chess"]
        assert filed["findings"] == 8
        garden = ledger.row(session, "goal_review:garden")
        assert (garden.kind, garden.source, garden.status) == ("review", "look", "new")
        assert ledger.row(session, "goal_review:chess").status == "resting"  # the user's Discard holds
        finding = ledger.row(session, "look:money-asks-wait")
        assert (finding.kind, finding.source, finding.status) == ("issue", "look", "new")
        assert "look:money-asks-wait" in [found.key for found in ledger.new_items(session)]
        _count(session, "piano")  # its count refreshes the row: the look's reasons still go with the review
        started = reviews.file_next(session, thread_id=THREAD)
        assert started["goal"] == "piano"
        evidence = session.get(WorkflowRun, started["run_id"]).input["evidence"]
        assert evidence == ["said so on 10-02", "piano: 3 weeks off plan"]
        later = utc_now() + timedelta(days=20)
        assert ledger.review_due(garden, later) and not ledger.review_due(garden, later + timedelta(days=16))
