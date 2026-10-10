"""Idea cards: every Monday, up to five ideas for Tasque, each on a card with Build it, Not now and Never.

``function.workshop_ideas`` (its schedule: Mondays at 07:30) starts ``workshop-ideas`` while card slots
are free:

1. ``ideas`` (a model run that reads Tasque and the web, and never writes): one idea per free slot, at
   least 2 exploratory, steered by what became of the earlier ones (``history``: built, rested, declined);
2. ``cards`` (no model, ``function.workshop_idea_cards``): each idea not declined, resting or already on
   a card gets a ledger row and its own ``workshop-idea`` run: a card in the Workshop thread.

A card's answer (``function.workshop_idea_apply``): Build it starts a change as the user's own ask, with
any note typed in reply to the card; Not now rests the idea 60 days; Never retires it. A card left
unanswered for 14 days expires, and the idea rests.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from tasque2.config import get_settings
from tasque2.models import WorkflowDefinition, WorkflowNode, WorkflowRun, WorkItem, utc_now
from tasque2.workshop import ledger, pipeline
from tasque2.workshop.ledger import clip

WORKFLOW_NAME = "workshop-ideas"
CARD_WORKFLOW = "workshop-idea"
LAUNCH_WORKER = "function.workshop_ideas"
CARDS_WORKER = "function.workshop_idea_cards"
APPLY_WORKER = "function.workshop_idea_apply"
LANE = "workshop-ideas"
TEMPLATE = "work-templates/workshop/ideas.template.md"
BUILD_IT, NOT_NOW, NEVER = "Build it", "Not now", "Never"
MAX_CARDS = 5
MIN_EXPLORATORY = 2
FALLBACK = """\
# Workshop: this week's ideas
Offer up to `slots` ideas for Tasque, at least `exploratory` of them exploratory, steered by `history`,
following the pinned `tasque_workshop` document. Read, never write. `produces`: {ideas: [{title, idea, why,
takes, area, exploratory}], silent: true}.
"""


def definition() -> dict[str, Any]:
    from tasque2.workshop.triage import READ_ONLY

    data = get_settings().resolved_data_dir
    return {
        "nodes": [
            {
                "key": "ideas",
                "kind": "work",
                "title": "Workshop: this week's ideas",
                "worker_kind": "provider.default",
                "lane": LANE,
                "task_template_path": str(data / TEMPLATE),
                "task_instruction": FALLBACK,
                "runtime_contract": {
                    "model_profile": "high",
                    "mcp_servers": [],
                    "disallowed_tools": [*pipeline.denied_tools(), *READ_ONLY],
                },
                "max_attempts": 1,
                "tolerate_failure": True,
            },
            {
                "key": "cards",
                "kind": "work",
                "title": "Workshop: the idea cards",
                "worker_kind": CARDS_WORKER,
                "task_instruction": "Put each new idea on a card in the Workshop thread (no model).",
                "depends_on": ["ideas"],
            },
        ]
    }


def card_definition() -> dict[str, Any]:
    return {
        "nodes": [
            {"key": "choice", "kind": "gate", "prompt": "Build this idea?", "choices": [BUILD_IT, NOT_NOW, NEVER]},
            {
                "key": "apply",
                "kind": "work",
                "title": "Workshop: the idea card's answer",
                "worker_kind": APPLY_WORKER,
                "task_instruction": "Carry out the card's answer (no model).",
                "depends_on": ["choice"],
            },
        ]
    }


def ensure_definition(session: Session) -> WorkflowDefinition:
    return pipeline.ensure_bundled(session, WORKFLOW_NAME, definition())


def ensure_card_definition(session: Session) -> WorkflowDefinition:
    return pipeline.ensure_bundled(session, CARD_WORKFLOW, card_definition())


def open_cards(session: Session) -> int:
    statement = (
        select(func.count())
        .select_from(WorkflowRun)
        .where(WorkflowRun.name == CARD_WORKFLOW, WorkflowRun.status.in_(pipeline.OPEN_RUN_STATUSES))
    )
    return int(session.scalar(statement) or 0)


def launch_worker(work_item: WorkItem) -> dict[str, Any]:
    """``function.workshop_ideas``: this week's ideas, when a card slot is free. Posts nothing itself."""
    from tasque2.workflows import WorkflowService

    session = pipeline._session(work_item)
    slots = MAX_CARDS - open_cards(session)
    if slots <= 0:
        return {"summary": f"{MAX_CARDS} idea cards still wait for an answer.", "produces": {"silent": True}}
    running = session.scalar(
        select(WorkflowRun).where(WorkflowRun.name == WORKFLOW_NAME, WorkflowRun.status.in_(pipeline.OPEN_RUN_STATUSES))
    )
    if running is not None:
        return {"summary": "This week's ideas are still being thought up.", "produces": {"silent": True}}
    run = WorkflowService(session).start_run(
        workflow_definition_id=ensure_definition(session).id,
        input={
            "lane": pipeline.LANE,
            "slots": slots,
            "exploratory": min(MIN_EXPLORATORY, slots),
            "history": ledger.idea_history(session, now=utc_now()),
            "live_data": str(get_settings().resolved_data_dir),
            "memory_namespace": "global",
            "memory_canonical_keys": pipeline.DOCTRINE_KEYS,
            "context_limits": {"memories": 2, "artifacts": 0},
        },
        discord_thread_id=work_item.discord_thread_id,
    )
    return {"summary": f"Thinking up {slots} idea(s).", "produces": {"silent": True, "ideas_run_id": run.id}}


def ideas_of(value: Any) -> list[dict[str, Any]]:
    found = []
    for item in value if isinstance(value, list) else []:
        if not isinstance(item, dict) or not str(item.get("title") or "").strip():
            continue
        found.append(
            {
                "title": clip(item["title"], 80),
                "idea": clip(item.get("idea") or item["title"], 600),
                "why": clip(item.get("why"), 300),
                "takes": clip(item.get("takes"), 300),
                "area": clip(item.get("area"), 40) or None,
                "exploratory": bool(item.get("exploratory")),
            }
        )
    return found


def card_text(idea: dict[str, Any]) -> str:
    lines = [f"**Idea: {idea['title']}**" + (" (exploratory)" if idea.get("exploratory") else ""), idea["idea"]]
    if idea.get("why"):
        lines.append(f"Why it fits: {idea['why']}")
    if idea.get("takes"):
        lines.append(f"What it takes: {idea['takes']}")
    lines.append("Build it starts it as your own ask. Not now rests it for 60 days; Never retires it.")
    return "\n".join(lines)


def cards_worker(work_item: WorkItem) -> dict[str, Any]:
    """``function.workshop_idea_cards``: one card per idea that may be offered, up to the free slots."""
    from tasque2.workflows import WorkflowService

    session, run = pipeline._run(work_item)
    produced = pipeline.node_output(session, run.id, "ideas")
    if not produced or produced.get("tolerated_failure"):
        return {"summary": "No ideas this week: the run did not finish.", "produces": {"silent": True}}
    slots = int((run.input or {}).get("slots") or MAX_CARDS)
    now = utc_now()
    offered: list[str] = []
    for idea in ideas_of(produced.get("ideas")):
        if len(offered) >= slots:
            break
        key = ledger.idea_key(idea["title"])
        if not ledger.idea_open(ledger.row(session, key), now):
            continue
        card = WorkflowService(session).start_run(
            workflow_definition_id=ensure_card_definition(session).id,
            input={"lane": pipeline.LANE, "key": key, **idea},
            discord_thread_id=run.discord_thread_id,
        )
        gate = session.scalar(
            select(WorkflowNode).where(WorkflowNode.workflow_run_id == card.id, WorkflowNode.node_key == "choice")
        )
        gate.input = {"card": card_text(idea)}
        detail = {name: idea[name] for name in ("idea", "why", "takes", "area", "exploratory")}
        ledger.offer_idea(session, key=key, title=idea["title"], detail=detail, run_id=card.id, now=now)
        offered.append(idea["title"])
    session.flush()
    summary = f"{len(offered)} idea card(s): {'; '.join(offered)}" if offered else "No new idea to offer."
    return {"summary": summary, "produces": {"silent": True, "offered": offered}}


def apply_worker(work_item: WorkItem) -> dict[str, Any]:
    """``function.workshop_idea_apply``: Build it starts a change, Not now (or no answer) rests the idea,
    Never retires it."""
    session, run = pipeline._run(work_item)
    given = run.input or {}
    choice = pipeline.node_output(session, run.id, "choice")
    answer = str(choice.get("answer") or NOT_NOW)
    key = str(given.get("key") or "")
    notes = [str(note.get("text") or "").strip() for note in choice.get("notes") or [] if isinstance(note, dict)]
    if answer == BUILD_IT and not choice.get("expired"):
        request = "\n".join([f"{given.get('title')}: {given.get('idea')}", *[note for note in notes if note]])
        change = pipeline.start_change(
            session, request=request, origin="user", thread_id=run.discord_thread_id, author="an idea card"
        )
        ledger.answer_idea(session, key, ledger.DONE, change_id=str(change.input["change_id"]))
        summary = f"Building {given.get('title')}."
        return {"summary": summary, "produces": {"silent": True, "change_run_id": change.id}}
    if answer == NEVER:
        ledger.answer_idea(session, key, ledger.RETIRED)
        return {"summary": f"Retired: {given.get('title')}.", "produces": {"silent": True}}
    ledger.answer_idea(session, key, ledger.RESTING, until=utc_now() + ledger.SET_ASIDE)
    why = "its card expired" if choice.get("expired") else "not now"
    return {"summary": f"Resting {given.get('title')} ({why}).", "produces": {"silent": True}}
