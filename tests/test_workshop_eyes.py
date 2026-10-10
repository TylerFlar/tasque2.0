"""The Workshop's eyes, end to end on fixture repositories: triage files its fixes as the Workshop's own changes
(at most three open, never two on one file), those changes post only their cards and their "Live now" and tell
the ledger how they ended, a card nobody answers expires, typed words find the card they mean, and idea cards
build, rest or retire their ideas."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import select
from workshop_shop import THREAD, _gate, _out, _plan, _template_build, drive, open_shop

from tasque2.db import session_scope
from tasque2.models import DiscordMessage, WorkflowNode, WorkflowRun, WorkItem, utc_now
from tasque2.workflows import WorkflowService
from tasque2.workshop import ideas, ledger, pipeline, reply, triage
from tasque2.workshop.ledger import Signal


@pytest.fixture()
def shop(fresh_db: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    return open_shop(tmp_path, monkeypatch)


def _issues(session, *keys: str) -> None:
    ledger.record(session, [Signal(key=key, source="warning", title=f"Warning {key}") for key in keys])


def _triage(session, groups: list[dict[str, Any]], noise: list[str] | None = None) -> WorkflowRun:
    """A triage of the ledger's new rows whose model run answers with ``groups`` and ``noise`` (the changes it
    files are left waiting for their plans)."""
    run = triage.start(session, thread_id=THREAD)
    drive(session, {"triage": lambda item: {"groups": groups, "noise": noise or [], "silent": True}}, steps=2)
    return session.get(WorkflowRun, run.id)


def _group(title: str, items: list[str], files: list[str]) -> dict[str, Any]:
    return {"title": title, "request": f"{title}, please.", "items": items, "files": files, "evidence": ["seen 3x"]}


def _card_message(session, run: WorkflowRun, message_id: str) -> None:
    """The card as Discord shows it, so a reply can point at it."""
    session.add(
        DiscordMessage(
            discord_message_id=message_id,
            discord_channel_id="jobs",
            discord_thread_id=THREAD,
            direction="outbound",
            author="tasque",
            content_preview="card",
            workflow_run_id=run.id,
        )
    )
    session.flush()


# --- triage files its fixes -------------------------------------------------------------------------------------


def test_triage_files_at_most_three_fixes_never_two_on_one_file_and_rests_the_noise(shop: dict[str, Any]) -> None:
    with session_scope() as session:
        _issues(session, "warning:a", "warning:b", "warning:c", "warning:d", "warning:e", "warning:f", "warning:g")
        run = _triage(
            session,
            [
                _group("Quiet the pin warning", ["warning:a", "warning:b"], ["src/tasque2/sticky.py"]),
                _group("Shorter cooking replies", ["warning:c"], ["data/work-templates/cooking/reply.template.md"]),
                _group("Pin again", ["warning:d"], ["./src/tasque2/sticky.py"]),  # the same file as the first
                _group("A gentler morning page", ["warning:e"], ["data/doctrine/daybook/daybook_direction.md"]),
                _group("Fewer scans", ["warning:f"], ["data/lanes.json"]),
                {"title": "No request", "items": ["warning:g"]},
            ],
            noise=["warning:g", "warning:unknown"],
        )
        produced = _out(session, run.id, "file")
        assert produced["groups"] == 5 and produced["noise"] == 1 and len(produced["filed"]) == 3
        filed = [change["title"] for change in produced["filed"]]
        assert filed == ["Quiet the pin warning", "Shorter cooking replies", "A gentler morning page"]
        statuses = {key: ledger.row(session, f"warning:{key}").status for key in "abcdefg"}
        assert statuses == {
            "a": "filed",
            "b": "filed",
            "c": "filed",
            "d": "waiting",  # shares a file with an open change
            "e": "filed",
            "f": "waiting",  # no slot: three of the Workshop's own are open
            "g": "resting",
        }
        assert ledger.row(session, "warning:g").rest_until > utc_now() + timedelta(days=59)
        change = session.get(WorkflowRun, produced["filed"][0]["run_id"])
        assert change.input["origin"] == "workshop" and change.input["issues"] == ["warning:a", "warning:b"]
        assert change.input["files"] == ["src/tasque2/sticky.py"] and change.input["evidence"] == ["seen 3x"]
        assert change.input["request"] == "Quiet the pin warning, please."
        # the owner's own asks never wait for the Workshop's
        pipeline.start_change(session, request="my own ask", thread_id=THREAD)
        assert triage.file_waiting(session, thread_id=THREAD) == []
        assert "Waiting for a free slot:" in pipeline.status_text(session)


def test_a_crashed_triage_leaves_its_rows_new_and_a_paused_workshop_files_nothing(shop: dict[str, Any]) -> None:
    with session_scope() as session:
        _issues(session, "warning:a")
        run = triage.start(session, thread_id=THREAD)
        drive(session, {"triage": lambda item: None})
        assert _out(session, run.id, "file")["filed"] == []
        assert ledger.row(session, "warning:a").status == "new"
        assert triage.start(session, thread_id=THREAD) is not None  # the next sweep tries again
        ledger.propose(session, ["warning:a"], {"id": "g1", "title": "Fix", "request": "Fix it.", "files": []})
        pipeline.pause("paused by hand")
        assert triage.file_waiting(session, thread_id=THREAD) == []
        assert ledger.row(session, "warning:a").status == "waiting"


# --- the Workshop's own changes -------------------------------------------------------------------------------


def _filed(session, title: str = "Shorter cooking replies") -> WorkflowRun:
    _issues(session, "warning:a")
    run = _triage(session, [_group(title, ["warning:a"], ["data/work-templates/cooking/reply.template.md"])])
    return session.get(WorkflowRun, _out(session, run.id, "file")["filed"][0]["run_id"])


def test_a_change_the_workshop_filed_waits_for_a_tap_and_posts_its_live_now(shop: dict[str, Any]) -> None:
    with session_scope() as session:
        change = _filed(session)
        drive(session, {"plan": _plan("Shorter cooking replies", "tweak", ["templates"]), "build": _template_build})
        card = _gate(session, change.id).input["card"]
        assert "The Workshop filed this itself: seen 3x" in card  # its own initiative: one tap
        reply.handle(session, text="ship", thread_id=THREAD, author="owner", referenced=None)
        drive(session, {})
        report = _out(session, change.id, "report")
        assert report["undo_release"] == change.input["change_id"] and not report.get("silent")
        assert pipeline.report_text(session, session.get(WorkflowRun, change.id))[0].startswith("**Live now:")
        row = ledger.row(session, "warning:a")
        assert row.status == "done" and row.change_id == change.input["change_id"]


def test_a_change_the_workshop_filed_ends_quietly_and_rests_its_rows(shop: dict[str, Any]) -> None:
    with session_scope() as session:
        dropped = _filed(session)
        drive(session, {"plan": _plan("Shorter cooking replies", "tweak", ["templates"]), "build": _template_build})
        reply.handle(session, text="discard", thread_id=THREAD, author="owner", referenced=None)
        drive(session, {})
        assert _out(session, dropped.id, "report")["silent"] is True
        row = ledger.row(session, "warning:a")
        assert row.status == "resting" and row.rest_until > utc_now() + timedelta(days=59)
        assert reply.handle(session, text="redo", thread_id=THREAD, author="owner", referenced=None).startswith(
            "Nothing to redo"  # its rows come back through the ledger instead
        )
        _issues(session, "warning:b")
        run = _triage(session, [_group("Gone already", ["warning:b"], ["src/tasque2/sticky.py"])])
        stale = session.get(WorkflowRun, _out(session, run.id, "file")["filed"][0]["run_id"])
        drive(session, {"plan": lambda item: {"no_change": True, "answer": "The warning stopped on 10-02."}})
        assert _out(session, stale.id, "report")["silent"] is True  # its plan found the evidence no longer holds
        row = ledger.row(session, "warning:b")
        assert row.status == "resting" and row.rest_until < utc_now() + timedelta(days=31)


# --- cards that wait --------------------------------------------------------------------------------------------


def test_a_card_nobody_answers_for_two_weeks_expires(shop: dict[str, Any]) -> None:
    questions = [{"question": "How short?", "default": "two sentences"}]
    with session_scope() as session:
        mine = pipeline.start_change(session, request="rethink the cooking replies", thread_id=THREAD)
        drive(session, {"plan": _plan("Cooking replies, rethought", "redesign", ["templates"], questions=questions)})
        gate = _gate(session, mine.id)
        assert pipeline.expire_cards(session) == 0
        gate.updated_at = utc_now() - timedelta(days=15)
        session.flush()
        assert pipeline.expire_cards(session) == 1
        drive(session, {})
        assert _out(session, mine.id, "plan_gate") == {"answer": "Discard", "expired": True}
        assert pipeline.report_text(session, session.get(WorkflowRun, mine.id))[0].startswith(
            "Expired: Cooking replies, rethought waited 14 days for your OK."
        )


def test_typed_words_find_the_card_they_mean(shop: dict[str, Any]) -> None:
    with session_scope() as session:
        filed = _filed(session)
        drive(session, {"plan": _plan("Shorter cooking replies", "tweak", ["templates"]), "build": _template_build})
        # a card the Workshop filed takes a note only as a reply: anything else is a new change
        assert reply.handle(session, text="make it warmer", thread_id=THREAD, author="owner", referenced=None) == (
            "On it: planning that now. Small changes ship on their own with an Undo; bigger ones come back here first."
        )
        _card_message(session, filed, "card-1")
        assert reply.handle(
            session, text="keep the greeting", thread_id=THREAD, author="owner", referenced="card-1"
        ).startswith("Noted for Shorter cooking replies")
        drive(session, {"plan": _plan("Warmer replies", "tweak", ["templates"], questions=[{"question": "How?"}])})
        # two cards wait now: a typed button word must reply to its card
        assert reply.handle(session, text="discard", thread_id=THREAD, author="owner", referenced=None) == (
            "2 cards are waiting: reply to the one you mean with discard."
        )
        assert reply.handle(session, text="build it", thread_id=THREAD, author="owner", referenced=None) == (
            "No card waiting here takes build it."
        )
        assert reply.handle(session, text="Discard", thread_id=THREAD, author="owner", referenced="card-1") == (
            "Discard: Shorter cooking replies."
        )
        notes = _out(session, filed.id, "ship_gate")["notes"]
        assert [note["text"] for note in notes] == ["keep the greeting"]


# --- idea cards -------------------------------------------------------------------------------------------------


def _idea(title: str, *, exploratory: bool = False) -> dict[str, Any]:
    return {
        "title": title,
        "idea": f"{title}, every week.",
        "why": "it fits",
        "takes": "a template",
        "area": "money",
        "exploratory": exploratory,
    }


def _launch(session) -> WorkflowRun | None:
    work = WorkItem(title="ideas", task_instruction="x", worker_kind=ideas.LAUNCH_WORKER, discord_thread_id=THREAD)
    session.add(work)
    session.flush()
    produced = ideas.launch_worker(work)["produces"]
    return session.get(WorkflowRun, produced["ideas_run_id"]) if produced.get("ideas_run_id") else None


def _cards(session) -> list[WorkflowRun]:
    return list(
        session.scalars(
            select(WorkflowRun)
            .where(WorkflowRun.name == ideas.CARD_WORKFLOW, WorkflowRun.status.in_(pipeline.OPEN_RUN_STATUSES))
            .order_by(WorkflowRun.created_at)
        ).all()
    )


def test_idea_cards_build_rest_or_retire_their_ideas_and_steer_the_next_week(shop: dict[str, Any]) -> None:
    with session_scope() as session:
        run = _launch(session)
        assert run.input["slots"] == 5 and run.input["exploratory"] == 2 and run.input["history"] == []
        nodes = {node["key"]: node for node in run.definition.definition["nodes"]}
        assert "Write" in nodes["ideas"]["runtime_contract"]["disallowed_tools"]
        assert "WebSearch" not in nodes["ideas"]["runtime_contract"]["disallowed_tools"]
        offered = [_idea(f"Idea {number}", exploratory=number < 3) for number in range(1, 8)]
        drive(session, {"ideas": lambda item: {"ideas": offered, "silent": True}})
        cards = _cards(session)
        assert len(cards) == 5 and _launch(session) is None  # five cards wait: no more this week
        card = _gate(session, cards[0].id).input["card"]
        assert card.startswith("**Idea: Idea 1** (exploratory)\nIdea 1, every week.\nWhy it fits: it fits")
        WorkflowService(session).answer_gate(workflow_run_id=cards[0].id, node_key="choice", answer="Build it")
        WorkflowService(session).answer_gate(workflow_run_id=cards[1].id, node_key="choice", answer="Not now")
        WorkflowService(session).answer_gate(workflow_run_id=cards[2].id, node_key="choice", answer="Never")
        _gate(session, cards[3].id).updated_at = utc_now() - timedelta(days=15)
        session.flush()
        assert pipeline.expire_cards(session) == 1
        drive(session, {"plan": _plan("Idea 1", "feature", ["templates"], questions=[{"question": "When?"}])})
        built = pipeline.latest_runs(session, thread_id=THREAD)[0]
        assert built.input["origin"] == "user" and built.input["request"].startswith("Idea 1: Idea 1, every week.")
        outcomes = {entry["title"]: entry["outcome"] for entry in ledger.idea_history(session, now=utc_now())}
        assert outcomes == {
            "Idea 1": "built",
            "Idea 2": "rested",
            "Idea 3": "declined",
            "Idea 4": "rested",
            "Idea 5": "waiting on its card",
        }
        again = _launch(session)
        assert again.input["slots"] == 4 and len(again.input["history"]) == 5
        drive(session, {"ideas": lambda item: {"ideas": [_idea("Idea 3"), _idea("Idea 2"), _idea("Idea 6")]}})
        assert [run.input["title"] for run in _cards(session)] == ["Idea 5", "Idea 6"]  # never and not-now stay out


def test_a_reply_to_an_idea_card_goes_with_its_build(shop: dict[str, Any]) -> None:
    with session_scope() as session:
        _launch(session)
        drive(session, {"ideas": lambda item: {"ideas": [_idea("Birthday nudges", exploratory=True)]}})
        card = _cards(session)[0]
        assert reply.handle(session, text="build it", thread_id=THREAD, author="owner", referenced=None) == (
            "Build it: Birthday nudges."
        )
        assert (
            session.scalar(
                select(WorkflowNode).where(WorkflowNode.workflow_run_id == card.id, WorkflowNode.node_key == "choice")
            ).output["answer"]
            == "Build it"
        )
