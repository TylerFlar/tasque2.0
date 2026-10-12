"""The Workshop's ledger (``WorkshopIssue``): one row per distinct issue its sweep saw, idea a card offered, or
goal due a review.

The sweep (``tasque2.workshop.sweep``) hands ``record`` what it sees as ``Signal``s, each keyed for good
(``fault:<signature>``). A signal is either a run of events (``occurrences``: each time it happened, so a
row counts each one once however often the sweep looks) or a condition that stands now (``count``: its
size, when it has one). A row is worth a look once it happened ``min_count`` times and has stood
``min_age``. A goal's review row (kind ``review``, ``goal_review:<goal>``: its own count or the monthly look
names it) is never triage's: ``tasque2.workshop.reviews`` starts its review.

Where a row stands (``status``):

- ``watching``: seen, not yet worth a look;
- ``new``: worth a look; the next triage takes it;
- ``waiting``: triage proposed a fix for its group (``proposal``), waiting for a free slot;
- ``filed``: a change, or for an idea its card, is open for it;
- ``resting``: set aside until ``rest_until``: noise, a Discard, an expired card or an idea rested (60
  days), a change that ended without shipping (30 days: one try in 30 days);
- ``done``: a change shipped for it; seen again after 30 days, it is looked at again;
- ``retired``: an idea the user declined, never offered again.
"""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from tasque2.models import WorkshopIssue, utc_now

WATCHING, NEW, WAITING, FILED, RESTING, DONE, RETIRED = (
    "watching",
    "new",
    "waiting",
    "filed",
    "resting",
    "done",
    "retired",
)
ISSUE, IDEA, REVIEW = "issue", "idea", "review"
SET_ASIDE = timedelta(days=60)
ONE_TRY = timedelta(days=30)
# A goal's review row stands while it is seen: within a day of the sweep whose count last saw it (a goal back on
# plan is left alone), or within 35 days of the monthly look that named it (``LOOK_ON``, in its ``detail``).
COUNT_SEEN, LOOK_SEEN = timedelta(days=1), timedelta(days=35)
LOOK_ON = "look_on"
EVIDENCE_LINES = 5
EVIDENCE_CHARS = 300
TITLE_CHARS = 240
SHIPPED, DISCARDED, ENDED = "shipped", "discarded", "ended"


@dataclass
class Signal:
    """One distinct issue as a sweep sees it now."""

    key: str
    source: str
    title: str
    evidence: list[str] = field(default_factory=list)
    occurrences: list[datetime] = field(default_factory=list)
    count: int | None = None
    min_count: int = 1
    min_age: timedelta = timedelta(0)
    since: datetime | None = None
    kind: str = ISSUE


def aware(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=utc_now().tzinfo)


def clip(text: Any, limit: int) -> str:
    flat = " ".join(str(text or "").split())
    return flat if len(flat) <= limit else flat[: limit - 1] + "…"


def _evidence(lines: list[str]) -> list[str]:
    return [clip(line, EVIDENCE_CHARS) for line in lines if str(line or "").strip()][:EVIDENCE_LINES]


def row(session: Session, key: str) -> WorkshopIssue | None:
    return session.scalar(select(WorkshopIssue).where(WorkshopIssue.key == key))


def rows(session: Session, keys: list[str]) -> list[WorkshopIssue]:
    if not keys:
        return []
    return list(session.scalars(select(WorkshopIssue).where(WorkshopIssue.key.in_(list(keys)))).all())


def _wake(found: WorkshopIssue) -> None:
    found.status, found.rest_until = WATCHING, None
    found.proposal = found.change_id = found.workflow_run_id = None


def record(session: Session, signals: list[Signal], *, now: datetime | None = None) -> Counter[str]:
    """Fold each signal into its row; returns how many rows were ``created``, ``seen``, ``woken`` from a rest
    that ended, and turned ``new``."""
    now = aware(now or utc_now())
    tally: Counter[str] = Counter()
    done: set[str] = set()
    for signal in signals:
        if signal.key in done:
            continue
        done.add(signal.key)
        times = sorted(aware(moment) for moment in signal.occurrences)
        found = row(session, signal.key)
        if found is None:
            found = WorkshopIssue(
                key=signal.key[:240],
                kind=signal.kind,
                source=signal.source,
                title=clip(signal.title, TITLE_CHARS),
                evidence=_evidence(signal.evidence),
                first_seen_at=times[0] if times else min(aware(signal.since or now), now),
                last_seen_at=times[-1] if times else now,
                count=len(times) if times else (signal.count if signal.count is not None else 1),
                status=WATCHING,
            )
            session.add(found)
            tally["created"] += 1
        else:
            fresh = [moment for moment in times if moment > aware(found.last_seen_at)]
            if signal.occurrences:
                again = bool(fresh)
                if fresh:
                    found.count += len(fresh)
                    found.last_seen_at = fresh[-1]
            else:
                again = True
                found.last_seen_at = now
                if signal.count is not None:
                    found.count = signal.count
            if again:
                found.title, found.evidence = clip(signal.title, TITLE_CHARS), _evidence(signal.evidence)
                if found.status in (RESTING, DONE) and (found.rest_until is None or now >= aware(found.rest_until)):
                    _wake(found)
                    tally["woken"] += 1
            tally["seen"] += 1
        ripe = found.count >= signal.min_count and now - aware(found.first_seen_at) >= signal.min_age
        if found.status == WATCHING and ripe:
            found.status = NEW
            tally["new"] += 1
    session.flush()
    return tally


def new_items(session: Session, *, limit: int = 40) -> list[WorkshopIssue]:
    """The issues triage has not looked at yet, oldest first."""
    statement = (
        select(WorkshopIssue)
        .where(WorkshopIssue.kind == ISSUE, WorkshopIssue.status == NEW)
        .order_by(WorkshopIssue.first_seen_at)
        .limit(limit)
    )
    return list(session.scalars(statement).all())


def item_data(found: WorkshopIssue) -> dict[str, Any]:
    """A row as triage reads it (its evidence trimmed, so a triage's packet stays small)."""
    return {
        "key": found.key,
        "source": found.source,
        "title": found.title,
        "evidence": [clip(line, 220) for line in (found.evidence or [])[:4]],
        "count": found.count,
        "first_seen": aware(found.first_seen_at).date().isoformat(),
        "last_seen": aware(found.last_seen_at).isoformat(timespec="minutes"),
    }


def set_aside(session: Session, keys: list[str], *, until: datetime) -> int:
    """Rest these rows until ``until`` (an idea the user declined stays retired)."""
    rested = 0
    for found in rows(session, keys):
        if found.status == RETIRED:
            continue
        found.status, found.rest_until, found.proposal = RESTING, until, None
        rested += 1
    session.flush()
    return rested


def propose(session: Session, keys: list[str], proposal: dict[str, Any]) -> int:
    """Triage's fix for a group of rows: they wait for a free slot together."""
    proposed = 0
    for found in rows(session, keys):
        if found.status not in (WATCHING, NEW, WAITING):
            continue
        found.status, found.proposal = WAITING, dict(proposal)
        proposed += 1
    session.flush()
    return proposed


def waiting_groups(session: Session) -> list[tuple[dict[str, Any], list[WorkshopIssue]]]:
    """Each proposed fix still waiting for a slot, with its rows, the group seen first coming first."""
    groups: dict[str, tuple[dict[str, Any], list[WorkshopIssue]]] = {}
    statement = (
        select(WorkshopIssue)
        .where(WorkshopIssue.kind == ISSUE, WorkshopIssue.status == WAITING)
        .order_by(WorkshopIssue.first_seen_at)
    )
    for found in session.scalars(statement).all():
        proposal = dict(found.proposal or {})
        group = str(proposal.get("id") or found.key)
        groups.setdefault(group, (proposal, []))[1].append(found)
    return list(groups.values())


def mark_filed(session: Session, keys: list[str], *, change_id: str, run_id: str, now: datetime) -> None:
    for found in rows(session, keys):
        found.status, found.change_id, found.workflow_run_id, found.tried_at = FILED, change_id, run_id, now
    session.flush()


def settle(session: Session, change_id: str, outcome: str, *, now: datetime | None = None) -> int:
    """A change the Workshop filed has ended: what it means for the rows it answered. Shipped, they are done;
    discarded or expired, they rest 60 days; any other ending gives them back for one try in 30 days."""
    now = aware(now or utc_now())
    settled = 0
    statement = select(WorkshopIssue).where(WorkshopIssue.change_id == change_id, WorkshopIssue.status == FILED)
    for found in session.scalars(statement).all():
        if outcome == SHIPPED:
            found.status, found.rest_until = DONE, now + ONE_TRY
        else:
            found.status = RESTING
            found.rest_until = now + (SET_ASIDE if outcome == DISCARDED else ONE_TRY)
        settled += 1
    session.flush()
    return settled


def counts(session: Session) -> dict[str, int]:
    statement = (
        select(WorkshopIssue.status, func.count()).where(WorkshopIssue.kind == ISSUE).group_by(WorkshopIssue.status)
    )
    return {status: number for status, number in session.execute(statement).all()}


def status_lines(session: Session, *, now: datetime | None = None) -> list[str]:
    """The ledger in a few lines, for ``status``: the issues, the fixes waiting for a slot, and the goal reviews
    open or waiting their turn."""
    now = aware(now or utc_now())
    numbers = counts(session)
    order = (NEW, WAITING, FILED, WATCHING, RESTING, DONE)
    parts = [f"{numbers[status]} {status}" for status in order if numbers.get(status)]
    lines = [f"Ledger: {', '.join(parts)}." if parts else "Ledger: empty."]
    groups = waiting_groups(session)
    if groups:
        lines.append("Waiting for a free slot:")
        lines += [f"- {clip(proposal.get('title') or found[0].title, 70)}" for proposal, found in groups[:5]]
    reviews = [found for found in review_rows(session, FILED, NEW) if found.status == FILED or review_due(found, now)]
    if reviews:
        lines.append("Goal reviews:")
        lines += [
            f"- {clip(found.title, 70)}: {'under review' if found.status == FILED else 'waiting its turn'}"
            for found in reviews[:5]
        ]
    return lines


# --- goal reviews ----------------------------------------------------------------------------------


def goal_name(value: Any) -> str:
    """A goal as its review row names it: its goal document's key without ``_goal``, in plain characters."""
    name = re.sub(r"[^a-z0-9]+", "_", str(value or "").strip().lower()).strip("_")
    return name.removesuffix("_goal")[:60]


def review_key(goal: str) -> str:
    return f"goal_review:{goal}"


def review_rows(session: Session, *statuses: str) -> list[WorkshopIssue]:
    """The goals' review rows (those in ``statuses``, when given), in the order they joined the ledger: rows
    that joined in one sweep keep the order their source gave them, else go by key."""
    statement = select(WorkshopIssue).where(WorkshopIssue.kind == REVIEW)
    if statuses:
        statement = statement.where(WorkshopIssue.status.in_(statuses))
    return list(session.scalars(statement.order_by(WorkshopIssue.created_at, WorkshopIssue.key)).all())


def looked_on(found: WorkshopIssue) -> datetime | None:
    """When the monthly look last named this goal for a review, if it did."""
    try:
        return aware(datetime.fromisoformat(str((found.detail or {}).get(LOOK_ON))))
    except (TypeError, ValueError):
        return None


def review_due(found: WorkshopIssue, now: datetime) -> bool:
    """Whether a goal's review row still stands: its count saw it within a day, or the look named it within 35
    days."""
    if aware(now) - aware(found.last_seen_at) <= COUNT_SEEN:
        return True
    named = looked_on(found)
    return named is not None and aware(now) - named <= LOOK_SEEN


def mark_looked(session: Session, key: str, *, evidence: list[str], now: datetime) -> None:
    """The monthly look named this goal for a review: its row stands 35 days, and the look's reasons go with the
    review even after its count refreshes the row."""
    found = row(session, key)
    if found is not None:
        found.detail = {**(found.detail or {}), LOOK_ON: aware(now).isoformat(), "look_evidence": _evidence(evidence)}
        session.flush()


# --- ideas ---------------------------------------------------------------------------------------


def idea_key(title: str) -> str:
    words = re.findall(r"[a-z0-9]+", str(title or "").lower())
    return "idea:" + "-".join(words)[:120]


def idea_open(found: WorkshopIssue | None, now: datetime) -> bool:
    """Whether an idea may be offered: never declined, no card open for it, not resting."""
    if found is None:
        return True
    if found.status in (RETIRED, FILED):
        return False
    return not (found.status == RESTING and found.rest_until is not None and aware(found.rest_until) > now)


def offer_idea(
    session: Session, *, key: str, title: str, detail: dict[str, Any], run_id: str, now: datetime
) -> WorkshopIssue:
    found = row(session, key)
    if found is None:
        found = WorkshopIssue(key=key, kind=IDEA, source="idea", first_seen_at=now)
        session.add(found)
    found.title, found.detail, found.evidence = clip(title, TITLE_CHARS), dict(detail), []
    found.last_seen_at, found.count = now, (found.count or 0) + 1
    found.status, found.rest_until, found.workflow_run_id, found.tried_at = FILED, None, run_id, now
    session.flush()
    return found


def answer_idea(
    session: Session, key: str, status: str, *, until: datetime | None = None, change_id: str | None = None
) -> None:
    found = row(session, key)
    if found is None:
        return
    found.status, found.rest_until = status, until
    if change_id:
        found.change_id = change_id
    session.flush()


def idea_history(session: Session, *, now: datetime, days: int = 365, limit: int = 60) -> list[dict[str, Any]]:
    """What became of the ideas offered lately (the newest ``limit``): built, rested, declined, or still on a
    card. Older ones still steer: a declined idea is never offered again."""
    outcomes = {DONE: "built", RESTING: "rested", RETIRED: "declined", FILED: "waiting on its card"}
    statement = (
        select(WorkshopIssue)
        .where(WorkshopIssue.kind == IDEA, WorkshopIssue.last_seen_at >= now - timedelta(days=days))
        .order_by(WorkshopIssue.last_seen_at.desc())
        .limit(limit)
    )
    history = []
    for found in session.scalars(statement).all():
        detail = found.detail or {}
        history.append(
            {
                "title": found.title,
                "area": detail.get("area"),
                "exploratory": bool(detail.get("exploratory")),
                "outcome": outcomes.get(found.status, found.status),
                "offered": aware(found.last_seen_at).date().isoformat(),
                "rests_until": aware(found.rest_until).date().isoformat() if found.rest_until else None,
            }
        )
    return history
