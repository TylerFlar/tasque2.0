"""Goal reviews: a goal off plan by its own count, or one the monthly look names, gets a deep review.

One review row per goal in the ledger (kind ``review``, ``goal_review:<goal>``): the sweep keeps it from an
extension's count (an ``add_signal_source`` item that names a ``goal``), and the monthly look
(``tasque2.workshop.look``) names a goal that is new, changed, stalled, done or dropped. Triage never sees
these rows.

``file_next`` (the sweep calls it after filing the fixes) starts one review at a time, none while the Workshop
is paused: the oldest new row (the first to join the ledger) that was not tried in 30 days and still stands
(``ledger.review_due``: its count saw it within a day, so a goal back on plan is left alone, or the look
named it within 35 days). The
review is a change of the Workshop's own (``pipeline.start_change`` with ``review``): planned at ultra from the
goal rather than from the current design, always a pitch for the owner (a redesign whatever its plan says),
and silent when its plan finds nothing to change. Its row then follows the ledger's rules for fixes: shipped,
it rests 30 days; discarded or expired, 60; any other ending, 30.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from tasque2.models import WorkflowRun, WorkshopIssue, utc_now
from tasque2.workshop import ledger, pipeline
from tasque2.workshop.ledger import aware, clip

logger = logging.getLogger(__name__)


def open_review(session: Session) -> WorkflowRun | None:
    """The goal review in flight, if any."""
    return next((run for run in pipeline.open_changes(session) if (run.input or {}).get("review")), None)


def evidence_of(found: WorkshopIssue, now: datetime) -> list[str]:
    """What the review starts from: the look's reasons when it named the goal lately, then the row's own."""
    lines = [str(line) for line in found.evidence or []]
    named = ledger.looked_on(found)
    if named is not None and now - named <= ledger.LOOK_SEEN:
        lines = [str(line) for line in (found.detail or {}).get("look_evidence") or []] + lines
    return list(dict.fromkeys(line for line in lines if line.strip()))[: ledger.EVIDENCE_LINES]


def file_next(session: Session, *, thread_id: str | None, now: datetime | None = None) -> dict[str, Any] | None:
    """Start the next goal review that is due, unless the Workshop is paused or a review is open; what it
    started, or None."""
    now = aware(now or utc_now())
    if not thread_id or pipeline.paused() or open_review(session) is not None:
        return None
    for found in ledger.review_rows(session, ledger.NEW):
        if found.tried_at is not None and now - aware(found.tried_at) < ledger.ONE_TRY:
            continue
        if not ledger.review_due(found, now):
            continue
        goal = found.key.split(":", 1)[-1]
        try:
            run = pipeline.start_change(
                session,
                request=f"Goal review: {found.title}",
                origin="workshop",
                thread_id=thread_id,
                author="workshop",
                evidence=evidence_of(found, now),
                issues=[found.key],
                review={"goal": goal},
            )
        except pipeline.WorkshopPaused:
            return None
        except Exception:  # noqa: BLE001 - a review that cannot start stays new for the next sweep
            logger.exception("The Workshop could not start the review of the %s goal", goal)
            return None
        change_id = str(run.input["change_id"])
        ledger.mark_filed(session, [found.key], change_id=change_id, run_id=run.id, now=now)
        return {"title": clip(found.title, 80), "goal": goal, "change_id": change_id, "run_id": run.id}
    return None
