"""The Workshop thread: what the owner types there, routed without a model.

``function.workshop_reply`` is the thread's reply processor (its lane file names it). A message is:

- a command: ``pause``, ``resume``, ``undo``, ``redo``, ``status``;
- a button's label typed out (``approve``, ``revise``, ``discard``, ``ship``, ``build it``, ``not now``,
  ``never``): that answer, for the card it replies to, or the only card waiting; while several wait, it
  must reply to its card;
- a note that goes with a card's answer (the plan gate's notes are the answers to its questions, and a
  Revise plans again with them), unless it starts with ``new:``: on the card it replies to, else on the
  newest card of the owner's own changes. A card the Workshop filed, and an idea card, takes a note only
  as a reply;
- a message right after the one that started a change, before its plan began: added to that request;
- anything else, and anything relayed from another thread: a new change, planned at once. Asking for
  ideas is a change too: its plan answers with them instead of changing anything.
"""

from __future__ import annotations

import re
from datetime import timedelta
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from tasque2.models import DiscordMessage, WorkflowNode, WorkflowRun, WorkItem, utc_now
from tasque2.workshop import ideas, pipeline

GATHER_WITHIN = timedelta(minutes=2)
IDEAS = re.compile(r"\b(ideas?|suggestions?|suggest)\b", re.IGNORECASE)
COMMANDS = {
    "pause": "pause",
    "pause the workshop": "pause",
    "stop": "pause",
    "resume": "resume",
    "unpause": "resume",
    "resume the workshop": "resume",
    "undo": "undo",
    "undo that": "undo",
    "undo it": "undo",
    "redo": "redo",
    "retry": "redo",
    "try again": "redo",
    "status": "status",
    "what's going on": "status",
    "whats going on": "status",
}
LABELS = {
    "approve": pipeline.APPROVE,
    "revise": pipeline.REVISE,
    "discard": pipeline.DISCARD,
    "ship": pipeline.SHIP,
    "build it": ideas.BUILD_IT,
    "not now": ideas.NOT_NOW,
    "never": ideas.NEVER,
}
CARD_WORKFLOWS = (pipeline.WORKFLOW_NAME, ideas.CARD_WORKFLOW)


def _command(text: str) -> str:
    return re.sub(r"[\s.!?]+$", "", text.strip().casefold())


def waiting_gates(session: Session, thread_id: str | None) -> list[tuple[WorkflowRun, WorkflowNode]]:
    """The cards waiting in this thread (changes' and ideas'), newest first."""
    if not thread_id:
        return []
    rows = session.execute(
        select(WorkflowRun, WorkflowNode)
        .join(WorkflowNode, WorkflowNode.workflow_run_id == WorkflowRun.id)
        .where(
            WorkflowRun.name.in_(CARD_WORKFLOWS),
            WorkflowRun.discord_thread_id == thread_id,
            WorkflowRun.status == "awaiting_input",
            WorkflowNode.kind == "gate",
            WorkflowNode.status == "awaiting_input",
        )
        .order_by(WorkflowRun.created_at.desc())
    ).all()
    return [(run, node) for run, node in rows]


def _owners_card(run: WorkflowRun) -> bool:
    """Whether a card is one of the owner's own changes: only such a card takes a note typed without a reply."""
    return run.name == pipeline.WORKFLOW_NAME and (run.input or {}).get("origin") == "user"


def _choices(node: WorkflowNode) -> list[str]:
    return [str(choice) for choice in (node.definition or {}).get("choices") or []]


def _referenced_run(session: Session, message_id: str | None) -> str | None:
    if not message_id:
        return None
    message = session.scalar(select(DiscordMessage).where(DiscordMessage.discord_message_id == str(message_id)))
    return message.workflow_run_id if message is not None else None


def _title(session: Session, run: WorkflowRun) -> str:
    given = run.input or {}
    title = pipeline.node_output(session, run.id, "classify").get("title") or given.get("title") or given.get("request")
    return str(title or "the change")[:80]


def gather(session: Session, thread_id: str | None, text: str) -> WorkflowRun | None:
    """A change started moments ago whose plan has not begun: the message joins its request."""
    if not thread_id:
        return None
    for run in pipeline.latest_runs(session, thread_id=thread_id, limit=3):
        if run.created_at is None or utc_now() - _aware(run.created_at) > GATHER_WITHIN:
            continue
        node = session.scalar(
            select(WorkflowNode).where(WorkflowNode.workflow_run_id == run.id, WorkflowNode.node_key == "plan")
        )
        item = session.get(WorkItem, node.work_item_id) if node is not None and node.work_item_id else None
        if node is None or node.status not in ("pending", "enqueued") or (item is not None and item.status != "ready"):
            continue
        request = f"{(run.input or {}).get('request') or ''}\n{text}".strip()
        run.input = {**(run.input or {}), "request": request}
        if item is not None:
            item.context = {**(item.context or {}), "request": request}
        session.flush()
        return run
    return None


def _aware(moment: Any) -> Any:
    return moment if moment.tzinfo else moment.replace(tzinfo=utc_now().tzinfo)


def handle(
    session: Session,
    *,
    text: str,
    thread_id: str | None,
    author: str | None,
    referenced: str | None,
    relayed: bool = False,
) -> str:
    """What the message does, as the reply posted back. A message relayed from another thread (an ask
    the Daybook passed on) is always a change of its own."""
    from tasque2.workflows import WorkflowService

    if relayed:
        text = f"new: {text.strip()}"
    command = COMMANDS.get(_command(text))
    if command == "pause":
        pipeline.pause(f"paused by {author or 'the owner'}")
        return "Paused: no change starts unless you ask for it, and nothing ships without your tap. Say resume."
    if command == "resume":
        was = pipeline.resume()
        return "Resumed." if was else "The Workshop was not paused."
    if command == "undo":
        return pipeline.undo_latest(session)
    if command == "status":
        return pipeline.status_text(session)
    if command == "redo":
        run = pipeline.redo(session, thread_id=thread_id, author=author)
        if run is None:
            return "Nothing to redo: the last changes here all shipped or are still going."
        return f"Building it again: {str((run.input or {}).get('request') or '')[:120]}"
    gates = waiting_gates(session, thread_id)
    target = _referenced_run(session, referenced)
    replied = next(((run, node) for run, node in gates if run.id == target), None)
    label = LABELS.get(_command(text))
    if label is not None:
        chosen = replied or (gates[0] if len(gates) == 1 else None)
        if chosen is not None and label in _choices(chosen[1]):
            run, node = chosen
            WorkflowService(session).answer_gate(workflow_run_id=run.id, node_key=node.node_key, answer=label)
            return f"{label}: {_title(session, run)}."
        if chosen is None and any(label in _choices(node) for _run, node in gates):
            return f"{len(gates)} cards are waiting: reply to the one you mean with {label.lower()}."
        return f"No card waiting here takes {label.lower()}."
    new = text.strip()
    noted = replied or next(((run, node) for run, node in gates if _owners_card(run)), None)
    if new.casefold().startswith("new:"):
        new = new[4:].strip()
    elif noted is not None:
        run, node = noted
        WorkflowService(session).add_gate_note(
            workflow_run_id=run.id, node_key=node.node_key, text=new, author=author or "owner"
        )
        others = f" ({len(gates) - 1} other card(s) wait too; reply to one to note it there)" if len(gates) > 1 else ""
        return (
            f"Noted for {_title(session, run)}: it goes with your answer when you tap a button{others}. "
            "To start something separate instead, begin the message with new:"
        )
    if not new:
        return "Say what to change, or status, pause, resume, undo or redo."
    joined = gather(session, thread_id, new)
    if joined is not None:
        return "Added to the change I just started."
    try:
        pipeline.start_change(session, request=new, origin="user", thread_id=thread_id, author=author)
    except pipeline.WorkshopPaused as exc:  # only for others' changes; the owner's own still start
        return f"The Workshop is paused ({exc})."
    if IDEAS.search(new):
        return "On it: thinking up ideas."
    return "On it: planning that now. Small changes ship on their own with an Undo; bigger ones come back here first."


def reply_worker(work_item: WorkItem) -> dict[str, Any]:
    """``function.workshop_reply``: the Workshop thread's reply processor."""
    session = pipeline._session(work_item)
    context = work_item.context or {}
    source = context.get("source_reply") or {}
    conversation = context.get("conversation") or {}
    text = handle(
        session,
        text=str(source.get("content") or ""),
        thread_id=str(conversation.get("discord_thread_id") or work_item.discord_thread_id or "") or None,
        author=str(source.get("author") or "") or None,
        referenced=conversation.get("referenced_discord_message_id"),
        relayed=bool(context.get("relayed_from")),
    )
    return {"summary": text, "produces": {"workshop_reply": True}}
