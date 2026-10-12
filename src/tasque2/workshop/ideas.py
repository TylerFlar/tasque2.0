"""Idea cards: every Monday, up to five ideas for Tasque, each on a card with Build it, Revise, Not now and
Never.

``function.workshop_ideas`` (its schedule: Mondays at 07:30) starts ``workshop-ideas`` while card slots
are free:

1. ``ideas`` (a model run that reads Tasque and the web, and never writes): one idea per free slot, at
   least 2 exploratory, steered by what became of the earlier ones (``history``: built, rested, declined);
2. ``cards`` (no model, ``function.workshop_idea_cards``): each idea not declined, resting or already on
   a card gets a ledger row and its own ``workshop-idea`` run: a card in the Workshop thread.

A card's run:

1. ``rethink`` (a model run that reads, like ``ideas``): after a Revise, the same idea reworked with the
   user's notes. The first round runs no model;
2. ``card`` (no model, ``function.workshop_idea_card``): a reworked idea replaces the run's, and its ledger
   row's title and detail (the row keeps its key); a rework that did not finish keeps the idea, and the card
   says so. Then the card's text;
3. ``choice``: Build it, Revise, Not now or Never. Revise opens a text box for what should change;
4. ``apply`` (no model, ``function.workshop_idea_apply``): Build it starts a change as the user's own ask,
   with every note (those Revise took, and any typed in reply to the card); Revise sends the run back to
   ``rethink`` (the workflow's ``rewind_to``) with the notes; Not now rests the idea 60 days; Never retires
   it.

A card left unanswered for 14 days expires, and the idea rests.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from tasque2.config import get_settings
from tasque2.models import WorkflowDefinition, WorkflowRun, WorkItem, utc_now
from tasque2.workshop import ledger, pipeline
from tasque2.workshop.ledger import clip

WORKFLOW_NAME = "workshop-ideas"
CARD_WORKFLOW = "workshop-idea"
LAUNCH_WORKER = "function.workshop_ideas"
CARDS_WORKER = "function.workshop_idea_cards"
CARD_WORKER = "function.workshop_idea_card"
APPLY_WORKER = "function.workshop_idea_apply"
LANE = "workshop-ideas"
TEMPLATE = "work-templates/workshop/ideas.template.md"
REWORK_TEMPLATE = "work-templates/workshop/idea.template.md"
BUILD_IT, REVISE, NOT_NOW, NEVER = "Build it", pipeline.REVISE, "Not now", "Never"
MAX_CARDS = 5
MIN_EXPLORATORY = 2
DETAIL = ("idea", "why", "takes", "area", "exploratory")
# What a card's run carries from the ideas run, so its rework reads the same doctrine.
PACKET_KEYS = ("live_data", "memory_namespace", "memory_canonical_keys", "context_limits")
KEPT = "The rework didn't finish, so this is the idea as it was; Revise tries again."
FALLBACK = """\
# Workshop: this week's ideas
Offer up to `slots` ideas for Tasque, at least `exploratory` of them exploratory, steered by `history`,
following the pinned `tasque_workshop` document. Read, never write. `produces`: {ideas: [{title, idea, why,
takes, area, exploratory}], silent: true}.
"""
REWORK_FALLBACK = """\
# Workshop: rework one idea
Rework the idea in the task context (`title`, `idea`, `why`, `takes`, `area`, `exploratory`) with the user's
`notes`, keeping what they did not object to, following the pinned `tasque_workshop` document. Read, never
write. `produces`: {idea: {title, idea, why, takes, area, exploratory}, silent: true}.
"""


def _read_only_run(key: str, title: str, template: str, fallback: str) -> dict[str, Any]:
    """A model run that reads Tasque and the web and writes nothing but its result."""
    from tasque2.workshop.triage import READ_ONLY

    return {
        "key": key,
        "kind": "work",
        "title": title,
        "worker_kind": "provider.default",
        "lane": LANE,
        "task_template_path": str(get_settings().resolved_data_dir / template),
        "task_instruction": fallback,
        "runtime_contract": {
            "model_profile": "high",
            "mcp_servers": [],
            "disallowed_tools": [*pipeline.denied_tools(), *READ_ONLY],
        },
        "max_attempts": 1,
        "tolerate_failure": True,
    }


def definition() -> dict[str, Any]:
    return {
        "nodes": [
            _read_only_run("ideas", "Workshop: this week's ideas", TEMPLATE, FALLBACK),
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
            {
                **_read_only_run("rethink", "Workshop: rework the idea", REWORK_TEMPLATE, REWORK_FALLBACK),
                "skip_when": "input.fresh",
            },
            {
                "key": "card",
                "kind": "work",
                "title": "Workshop: the idea's card",
                "worker_kind": CARD_WORKER,
                "task_instruction": "Write the idea's card (no model).",
                "depends_on": ["rethink"],
            },
            {
                "key": "choice",
                "kind": "gate",
                "prompt": "Build this idea?",
                "choices": [BUILD_IT, REVISE, NOT_NOW, NEVER],
                "ask_note": {REVISE: pipeline.REVISE_NOTE},
                "card_from": "card.card",
                "depends_on": ["card"],
            },
            {
                "key": "apply",
                "kind": "work",
                "title": "Workshop: the idea card's answer",
                "worker_kind": APPLY_WORKER,
                "task_instruction": "Carry out the card's answer (no model).",
                "depends_on": ["choice"],
                "rewind_to": "rethink",
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


def rework_of(idea: dict[str, Any], produced: dict[str, Any]) -> dict[str, Any] | None:
    """The idea a rework gave back (``produces.idea``), any field it left out kept from ``idea``; None when it
    gave none."""
    given = produced.get("idea")
    if not isinstance(given, dict) or not given:
        return None
    found = ideas_of([{**idea, **{name: value for name, value in given.items() if value not in (None, "")}}])
    return found[0] if found else None


def card_text(idea: dict[str, Any], *, reworked: bool = False, kept: bool = False) -> str:
    """The card: the idea, reworked when a Revise changed it, or kept as it was when the rework did not
    finish."""
    head = "Idea, reworked" if reworked else "Idea"
    lines = [f"**{head}: {idea['title']}**" + (" (exploratory)" if idea.get("exploratory") else "")]
    if kept:
        lines.append(KEPT)
    lines.append(idea["idea"])
    if idea.get("why"):
        lines.append(f"Why it fits: {idea['why']}")
    if idea.get("takes"):
        lines.append(f"What it takes: {idea['takes']}")
    lines.append(
        "Build it starts it as your own ask. Revise reworks it with your note. Not now rests it for 60 days; "
        "Never retires it."
    )
    return "\n".join(lines)


def cards_worker(work_item: WorkItem) -> dict[str, Any]:
    """``function.workshop_idea_cards``: one card per idea that may be offered, up to the free slots."""
    from tasque2.workflows import WorkflowService

    session, run = pipeline._run(work_item)
    produced = pipeline.node_output(session, run.id, "ideas")
    if not produced or produced.get("tolerated_failure"):
        return {"summary": "No ideas this week: the run did not finish.", "produces": {"silent": True}}
    given = run.input or {}
    slots = int(given.get("slots") or MAX_CARDS)
    packet = {name: given[name] for name in PACKET_KEYS if name in given}
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
            input={"lane": pipeline.LANE, "key": key, **idea, "fresh": True, "notes": [], **packet},
            discord_thread_id=run.discord_thread_id,
        )
        detail = {name: idea[name] for name in DETAIL}
        ledger.offer_idea(session, key=key, title=idea["title"], detail=detail, run_id=card.id, now=now)
        offered.append(idea["title"])
    session.flush()
    summary = f"{len(offered)} idea card(s): {'; '.join(offered)}" if offered else "No new idea to offer."
    return {"summary": summary, "produces": {"silent": True, "offered": offered}}


def card_worker(work_item: WorkItem) -> dict[str, Any]:
    """``function.workshop_idea_card``: the card's text. After a Revise, the reworked idea replaces the run's
    and its ledger row's title and detail (the row keeps its key); a rework that did not finish keeps the idea,
    and the card says so in one line."""
    session, run = pipeline._run(work_item)
    given = run.input or {}
    idea = ideas_of([given])[0]
    if given.get("fresh"):
        return {"summary": f"The card for {idea['title']}.", "produces": {"silent": True, "card": card_text(idea)}}
    reworked = rework_of(idea, pipeline.node_output(session, run.id, "rethink"))
    if reworked is None:
        text = card_text(idea, kept=True)
        return {"summary": f"{idea['title']}: not reworked, kept.", "produces": {"silent": True, "card": text}}
    run.input = {**given, **reworked}
    ledger.rework_idea(
        session, str(given.get("key") or ""), title=reworked["title"], detail={name: reworked[name] for name in DETAIL}
    )
    text = card_text(reworked, reworked=True)
    return {"summary": f"Reworked: {reworked['title']}.", "produces": {"silent": True, "card": text}}


def apply_worker(work_item: WorkItem) -> dict[str, Any]:
    """``function.workshop_idea_apply``: Build it starts a change with the idea and every note, Revise sends
    the card back to its rework with the notes, Not now (or no answer) rests the idea, Never retires it."""
    session, run = pipeline._run(work_item)
    given = run.input or {}
    choice = pipeline.node_output(session, run.id, "choice")
    answer = str(choice.get("answer") or NOT_NOW)
    key = str(given.get("key") or "")
    typed = [str(note.get("text") or "").strip() for note in choice.get("notes") or [] if isinstance(note, dict)]
    notes = [*[str(note) for note in given.get("notes") or []], *[note for note in typed if note]]
    if answer == REVISE:
        run.input = {**given, "notes": notes, "fresh": False}
        summary = f"Reworking {given.get('title')} with your note."
        return {"summary": summary, "produces": {"silent": True, "rewind": True}}
    if answer == BUILD_IT and not choice.get("expired"):
        request = "\n".join([f"{given.get('title')}: {given.get('idea')}", *notes])
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
