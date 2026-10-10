"""The Workshop's sweep: what it should know, gathered into its ledger without a model.

``function.workshop_sweep`` runs five times a day (its schedules) and posts nothing. Each pass gathers
``Signal``s (``tasque2.workshop.ledger``):

- faults that recur (twice, or on two days) in the fault ledger;
- runs that failed and never recovered (the dead letters), by lane and error;
- Tasque tool calls that answered ``{ok: false}`` alike at least twice (the tool-error ledger);
- warnings logged at least twice (the warning ledger);
- runs that took over 3 times their lane's usual time;
- the user's pushback in any thread but the Workshop's and a private one (a lane file with
  ``"private": true``), with the same correction made twice flagged;
- schedules whose gate skipped every run for 30 days, and schedules and Tasque tools nothing used in 30
  days;
- doctrine within 10% of its ``max_chars`` cap, and run packets over their budget (60k characters, 40k
  for a reply);
- decisions never answered: ``Open for the user's decision: ... (asked)`` lines a week old, and what
  extensions report (``add_signal_source``: a board decision that took its default, say);
- the journal's weekly notes when they changed (the sweep reads only the file's date; triage reads them).

Then it expires the Workshop's cards left unanswered for 14 days, files proposed fixes into free slots,
and starts a triage when the ledger holds new rows (``tasque2.workshop.triage``).
"""

from __future__ import annotations

import hashlib
import logging
import re
import statistics
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from tasque2.config import get_settings
from tasque2.lane_context import effective_context
from tasque2.localtime import local_date
from tasque2.memory.service import canonical_budget
from tasque2.models import (
    DiscordMessage,
    DiscordThread,
    FailedWork,
    Memory,
    ProviderRun,
    Schedule,
    ScheduleOccurrence,
    WorkAttempt,
    WorkEvent,
    WorkflowRun,
    WorkItem,
    utc_now,
)
from tasque2.ops.faults import read_faults, read_tool_errors, read_warnings
from tasque2.workshop import ledger
from tasque2.workshop.ledger import Signal, aware, clip

logger = logging.getLogger(__name__)

SWEEP_WORKER = "function.workshop_sweep"
WINDOW = timedelta(days=7)
UNUSED = timedelta(days=30)
SLOW_FACTOR, SLOW_FLOOR_MINUTES, SLOW_SAMPLES = 3, 5.0, 5
PACKET_LIMIT, REPLY_PACKET_LIMIT = 60_000, 40_000
REPLY_SOURCE = "discord_reply_followup"
CAP_SHARE = 0.9
GATE_RUNS = 10
OPEN_DECISION = "Open for the user's decision:"
DECISION_AGE = timedelta(days=7)
JOURNAL_FRESH = timedelta(days=8)
TASQUE_TOOLS = "mcp__tasque2__"
ENVIRONMENTAL = re.compile(
    r"usage limit|rate limit|session limit|capacity|oauth|token|unauthori[sz]ed|forbidden|\b40[13]\b|log ?in|"
    r"sign[- ]?in|duo|timed? ?out|timeout|connection|connect|network|dns|ssl|certificate|"
    r"submit_worker_result|TransientProviderError",
    re.IGNORECASE,
)
PUSHBACK = re.compile(
    "|".join(
        (
            r"^\s*(no|nope|nah)\s*([,.!]|$)",
            r"^\s*(wrong|stop)\b",
            r"\bi (already |just )?(said|told you|asked|answered)\b",
            r"\b(as|like) i (said|told you)\b",
            r"\byou (keep|still|forgot|ignored|missed)\b",
            r"\bstop (asking|sending|posting|doing|reminding|suggesting|adding)\b",
            r"\b(don'?t|do not|never) (ask|send|post|remind|suggest|tell|message|ping|schedule|book|buy|add)\b",
            r"\bthat'?s (wrong|not (right|true|what i))\b",
            r"\bnot what i (asked|wanted|meant|said)\b",
            r"\bwhy (did|do|does|would|are|is) (you|it|this|tasque)\b",
            r"\bshould(n'?t| not) have\b",
            r"\btoo (many|much|often|long|early|late)\b",
            r"\bagain\?",
            r"\bplease (stop|don'?t)\b",
        )
    ),
    re.IGNORECASE,
)
STOP_WORDS = frozenset(
    "about after again already also asked asking because been being could didn does doing dont from have just "
    "keep know like made make more need only please really said should some still stop sure tell than that thats "
    "their them then there they this told what when where which while will with would your youre".split()
)
_ID = re.compile(r"\b[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}\b|\b[0-9a-f]{12,}\b", re.IGNORECASE)
_QUOTED = re.compile(r"'[^']*'|\"[^\"]*\"")
_NUMBER = re.compile(r"\d+")


def environmental(text: str) -> bool:
    return bool(ENVIRONMENTAL.search(text or ""))


def _moment(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return aware(value)
    try:
        return aware(datetime.fromisoformat(str(value)))
    except (TypeError, ValueError):
        return None


def _day(value: datetime | None) -> str:
    return local_date(aware(value)) if value is not None else "?"


def _stem(text: Any) -> str:
    """An error message with its ids, quoted values and numbers left out: one stem per kind of error."""
    flat = _NUMBER.sub("#", _QUOTED.sub("'…'", _ID.sub("<id>", str(text or ""))))
    return clip(flat, 80)


def _key_hash(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:10]


# --- the ledgers on disk ---------------------------------------------------------------------------


def faults(now: datetime) -> list[Signal]:
    """Faults that recur: one row per fault signature."""
    groups: dict[str, list[dict[str, Any]]] = {}
    for entry in read_faults(since=now - WINDOW):
        groups.setdefault(str(entry.get("signature")), []).append(entry)
    found = []
    for signature, entries in groups.items():
        last = entries[-1]
        frame = last.get("frame") or {}
        what = last.get("exc_type") or "error"
        message = last.get("exc_message") or last.get("message")
        days = len({str(entry.get("at"))[:10] for entry in entries})
        evidence = [
            f"{what}: {message}",
            f"logged by {last.get('logger')}, raised at {frame.get('file')}:{frame.get('line')} in "
            f"{frame.get('function')}",
            f"{len(entries)}x on {days} day(s) this week",
        ]
        if environmental(f"{what} {message} {last.get('logger')}"):
            evidence.append("reads as environmental (a usage limit, a sign-in or the network)")
        found.append(
            Signal(
                key=f"fault:{signature}",
                source="fault",
                title=f"{what} in {frame.get('file')}:{frame.get('function')}",
                evidence=evidence,
                occurrences=[moment for entry in entries if (moment := _moment(entry.get("at")))],
                min_count=2,
            )
        )
    return found


def tool_errors(session: Session, now: datetime) -> list[Signal]:
    """Tasque tool calls that answered alike with an error: one row per tool, error type and message stem."""
    groups: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for entry in read_tool_errors(since=now - WINDOW):
        stem = _stem(entry.get("error"))
        groups.setdefault((str(entry.get("tool")), str(entry.get("error_type")), stem), []).append(entry)
    lanes: dict[str, str] = {}

    def lane(work_item_id: Any) -> str:
        if not work_item_id:
            return "no run"
        if work_item_id not in lanes:
            item = session.get(WorkItem, str(work_item_id))
            lanes[work_item_id] = (item.lane or item.title) if item is not None else "a run since removed"
        return lanes[work_item_id]

    found = []
    for (tool, error_type, stem), entries in groups.items():
        evidence = [
            f"{_day(_moment(entry.get('at')))} in {lane(entry.get('work_item_id'))}: {clip(entry.get('error'), 200)}"
            for entry in entries[-3:]
        ]
        found.append(
            Signal(
                key=f"tool_error:{tool}:{error_type}:{_key_hash(stem)}",
                source="tool_error",
                title=f"{tool} answered {error_type}: {stem}",
                evidence=[*evidence, f"{len(entries)}x this week"],
                occurrences=[moment for entry in entries if (moment := _moment(entry.get("at")))],
                min_count=2,
            )
        )
    return found


def warnings(now: datetime) -> list[Signal]:
    """Warnings logged again: one row per warning signature."""
    groups: dict[str, list[dict[str, Any]]] = {}
    for entry in read_warnings(since=now - WINDOW):
        groups.setdefault(str(entry.get("signature")), []).append(entry)
    found = []
    for signature, entries in groups.items():
        last = entries[-1]
        frame = last.get("frame") or {}
        days = len({str(entry.get("at"))[:10] for entry in entries})
        found.append(
            Signal(
                key=f"warning:{signature}",
                source="warning",
                title=f"Warning: {clip(last.get('message'), 120)}",
                evidence=[
                    clip(last.get("message"), 280),
                    f"logged by {last.get('logger')} at {frame.get('file')}:{frame.get('function')}",
                    f"{len(entries)}x on {days} day(s) this week",
                ],
                occurrences=[moment for entry in entries if (moment := _moment(entry.get("at")))],
                min_count=2,
            )
        )
    return found


def journal(context: dict[str, Any], now: datetime) -> list[Signal]:
    """The journal's weekly notes, when the file changed this past week; the sweep never reads them."""
    path = journal_digest(context)
    if path is None:
        return []
    try:
        stamp = datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)
    except OSError:
        return []
    if now - stamp > JOURNAL_FRESH:
        return []
    day = local_date(stamp)
    return [
        Signal(
            key=f"journal:{day}",
            source="journal",
            title=f"New weekly journal notes ({day})",
            evidence=["The journal's weekly notes changed; triage reads them."],
            since=stamp,
        )
    ]


def journal_digest(context: dict[str, Any]) -> Path | None:
    """The journal's weekly notes the sweep's context names (``journal_digest``; relative to the data
    directory), or None."""
    reference = str((context or {}).get("journal_digest") or "").strip()
    if not reference:
        return None
    path = Path(reference).expanduser()
    return path if path.is_absolute() else get_settings().resolved_data_dir / path


# --- runs ------------------------------------------------------------------------------------------


def failed_runs(session: Session) -> list[Signal]:
    """Runs that failed and never recovered (the unresolved dead letters), by lane and error type."""
    rows = session.execute(
        select(FailedWork, WorkItem)
        .join(WorkItem, WorkItem.id == FailedWork.work_item_id)
        .where(FailedWork.status == "unresolved")
        .order_by(FailedWork.created_at)
    ).all()
    groups: dict[tuple[str, str], list[tuple[FailedWork, WorkItem]]] = {}
    for failed, item in rows:
        groups.setdefault((item.lane or "unlabelled", failed.error_type or "error"), []).append((failed, item))
    found = []
    for (lane, error_type), entries in groups.items():
        evidence = [
            f"{_day(failed.created_at)}: {clip(item.title, 80)}: {clip(failed.error_message, 160)}"
            for failed, item in entries[-3:]
        ]
        if any(environmental(f"{failed.error_type} {failed.error_message}") for failed, _item in entries):
            evidence.append("reads as environmental (a usage limit, a sign-in or the network)")
        found.append(
            Signal(
                key=f"failed:{lane}:{error_type}",
                source="failed_run",
                title=f"{lane}: runs failed and never recovered ({error_type})",
                evidence=evidence,
                occurrences=[aware(failed.created_at) for failed, _item in entries],
            )
        )
    return found


def slow_runs(session: Session, now: datetime) -> list[Signal]:
    """Runs this week that took over ``SLOW_FACTOR`` times their lane's usual (median) time."""
    rows = session.execute(
        select(WorkAttempt.started_at, WorkAttempt.ended_at, WorkAttempt.status, WorkItem.lane, WorkItem.title)
        .join(WorkItem, WorkItem.id == WorkAttempt.work_item_id)
        .where(
            WorkAttempt.created_at >= now - UNUSED,
            WorkAttempt.started_at.is_not(None),
            WorkAttempt.ended_at.is_not(None),
        )
    ).all()
    lanes: dict[str, list[tuple[float, str, str, datetime]]] = {}
    for started, ended, status, lane, title in rows:
        minutes = (aware(ended) - aware(started)).total_seconds() / 60
        lanes.setdefault(lane or "unlabelled", []).append((minutes, status, title, aware(ended)))
    found = []
    for lane, runs in lanes.items():
        usual_runs = [minutes for minutes, status, _title, _ended in runs if status == "succeeded"]
        if len(usual_runs) < SLOW_SAMPLES:
            continue
        usual = statistics.median(usual_runs)
        limit = max(usual * SLOW_FACTOR, SLOW_FLOOR_MINUTES)
        slow = [
            (minutes, title, ended)
            for minutes, _status, title, ended in runs
            if ended >= now - WINDOW and minutes >= limit
        ]
        if not slow:
            continue
        slow.sort(key=lambda run: run[2])
        evidence = [f"usually {usual:.1f} min ({len(usual_runs)} runs in 30 days)"]
        evidence += [f"{_day(ended)}: {clip(title, 80)} took {minutes:.0f} min" for minutes, title, ended in slow[-3:]]
        found.append(
            Signal(
                key=f"slow:{lane}",
                source="slow_run",
                title=f"{lane}: runs took over {SLOW_FACTOR}x their usual time",
                evidence=evidence,
                occurrences=[ended for _minutes, _title, ended in slow],
            )
        )
    return found


def packets(session: Session, now: datetime) -> list[Signal]:
    """Runs this week whose prompt went over its budget: one row per lane (a reply's apart)."""
    rows = session.execute(
        select(ProviderRun.usage, ProviderRun.created_at, WorkItem.lane, WorkItem.title, WorkItem.source_kind)
        .join(WorkAttempt, WorkAttempt.id == ProviderRun.attempt_id)
        .join(WorkItem, WorkItem.id == WorkAttempt.work_item_id)
        .where(ProviderRun.created_at >= now - WINDOW)
    ).all()
    groups: dict[str, tuple[str, bool, int, list[tuple[int, datetime, str]]]] = {}
    for usage, created, lane, title, source_kind in rows:
        chars = (usage or {}).get("prompt_chars")
        if not isinstance(chars, int):
            continue
        reply = source_kind == REPLY_SOURCE
        limit = REPLY_PACKET_LIMIT if reply else PACKET_LIMIT
        if chars <= limit:
            continue
        lane = lane or "unlabelled"
        key = f"packet:{lane}" + (":reply" if reply else "")
        groups.setdefault(key, (lane, reply, limit, []))[3].append((chars, aware(created), title))
    found = []
    for key, (lane, reply, limit, runs) in groups.items():
        chars, _created, title = max(runs)
        found.append(
            Signal(
                key=key,
                source="packet",
                title=f"{lane}: {'reply ' if reply else ''}packets over {limit // 1000}k characters",
                evidence=[f"{len(runs)} run(s) this week; the largest {chars:,} characters ({clip(title, 80)})"],
                occurrences=[created for _chars, created, _title in runs],
            )
        )
    return found


# --- the user's pushback ------------------------------------------------------------------------------


def pushback(session: Session, now: datetime, *, skip_threads: set[str]) -> list[Signal]:
    """What the user pushed back on this week, one row per message; the same correction made again in a
    thread on another day is one row of its own, counting each repeat."""
    messages = session.scalars(
        select(DiscordMessage)
        .where(
            DiscordMessage.direction == "inbound",
            DiscordMessage.created_at >= now - UNUSED,
            DiscordMessage.discord_thread_id.is_not(None),
        )
        .order_by(DiscordMessage.created_at)
    ).all()
    candidates = [
        message
        for message in messages
        if message.discord_thread_id not in skip_threads and PUSHBACK.search(message.content_preview or "")
    ]
    private = private_threads(session, {str(message.discord_thread_id) for message in candidates})
    labels = thread_labels(session, {str(message.discord_thread_id) for message in candidates})
    earlier: dict[str, list[tuple[DiscordMessage, set[str]]]] = {}
    signals: dict[str, Signal] = {}
    for message in candidates:
        thread = str(message.discord_thread_id)
        if thread in private:
            continue
        words = content_words(message.content_preview)
        repeat_of = next(
            (
                first
                for first, first_words in earlier.get(thread, [])
                if _day(first.created_at) != _day(message.created_at) and similar(words, first_words)
            ),
            None,
        )
        earlier.setdefault(thread, []).append((message, words))
        if aware(message.created_at) < now - WINDOW:
            continue
        label = labels.get(thread) or f"thread {thread}"
        said = f"{_day(message.created_at)}: {clip(message.content_preview, 280)}"
        if repeat_of is not None:
            key = f"correction:{thread}:{repeat_of.discord_message_id}"
            signal = signals.setdefault(
                key,
                Signal(
                    key=key,
                    source="repeat_correction",
                    title=f"The same correction again in {label}: {clip(repeat_of.content_preview, 90)}",
                    evidence=[f"{_day(repeat_of.created_at)}: {clip(repeat_of.content_preview, 280)}"],
                ),
            )
            signal.evidence.append(said)
            signal.occurrences.append(aware(message.created_at))
            continue
        evidence = [said]
        answered = _previous_post(session, message)
        if answered:
            evidence.append(f"answering: {clip(answered, 200)}")
        signals[f"pushback:{message.discord_message_id}"] = Signal(
            key=f"pushback:{message.discord_message_id}",
            source="pushback",
            title=f"Pushback in {label}: {clip(message.content_preview, 90)}",
            evidence=evidence,
            occurrences=[aware(message.created_at)],
        )
    return list(signals.values())


def content_words(text: str | None) -> set[str]:
    return {word for word in re.findall(r"[a-z0-9]+", (text or "").lower()) if len(word) >= 4} - STOP_WORDS


def similar(first: set[str], second: set[str]) -> bool:
    """Two corrections about the same thing: they share at least three words, a third of them all."""
    shared = first & second
    return len(shared) >= 3 and len(shared) / max(1, len(first | second)) >= 0.3


def _previous_post(session: Session, message: DiscordMessage) -> str | None:
    post = session.scalar(
        select(DiscordMessage.content_preview)
        .where(
            DiscordMessage.discord_thread_id == message.discord_thread_id,
            DiscordMessage.direction == "outbound",
            DiscordMessage.created_at <= message.created_at,
        )
        .order_by(DiscordMessage.created_at.desc())
        .limit(1)
    )
    return str(post) if post else None


def _thread_owner(session: Session, binding: DiscordThread) -> WorkItem | None:
    if binding.work_item_id:
        return session.get(WorkItem, binding.work_item_id)
    return None


def private_threads(session: Session, threads: set[str]) -> set[str]:
    """Threads whose lane file says ``private``: nothing outside the lane reads them."""
    if not threads:
        return set()
    found = set()
    for binding in session.scalars(select(DiscordThread).where(DiscordThread.discord_thread_id.in_(threads))).all():
        owner = _thread_owner(session, binding)
        if owner is not None and effective_context(owner.context).get("private"):
            found.add(binding.discord_thread_id)
    return found


def thread_labels(session: Session, threads: set[str]) -> dict[str, str]:
    """A name for each thread: its lane, else its workflow or its owner's title."""
    labels: dict[str, str] = {}
    if not threads:
        return labels
    for binding in session.scalars(select(DiscordThread).where(DiscordThread.discord_thread_id.in_(threads))).all():
        owner = _thread_owner(session, binding)
        run = session.get(WorkflowRun, binding.workflow_run_id) if binding.workflow_run_id else None
        label = (owner.lane or owner.title) if owner is not None else (run.name if run is not None else None)
        if label:
            labels[binding.discord_thread_id] = f"the {label} thread"
    return labels


# --- schedules, tools, doctrine, decisions ----------------------------------------------------------------


def schedules(session: Session, now: datetime) -> list[Signal]:
    """Schedules a month old that launched nothing in 30 days: a gate that skipped every run, a schedule
    that never came due or ran, a one-time schedule whose time is long past, one switched off for 30 days."""
    from tasque2.schedules import ScheduleService

    since = now - UNUSED
    service = ScheduleService(session)
    found = []
    for schedule in session.scalars(select(Schedule).order_by(Schedule.created_at)).all():
        if aware(schedule.created_at) > since:
            continue
        what = f"{schedule.schedule_type} {schedule.expression}"
        if not schedule.enabled:
            if aware(schedule.updated_at) <= since:
                found.append(
                    Signal(
                        key=f"unused_schedule:{schedule.id}",
                        source="unused_schedule",
                        title=f"{schedule.name}: switched off for 30 days or more",
                        evidence=[f"off since {_day(schedule.updated_at)} at the latest ({what})"],
                    )
                )
            continue
        statuses = session.scalars(
            select(ScheduleOccurrence.status).where(
                ScheduleOccurrence.schedule_id == schedule.id, ScheduleOccurrence.scheduled_for >= since
            )
        ).all()
        if any(status != "skipped" for status in statuses):
            continue
        skipped = len(statuses)
        if skipped >= GATE_RUNS and (schedule.payload or {}).get("gate"):
            found.append(
                Signal(
                    key=f"gate_skip:{schedule.id}",
                    source="gate_skip",
                    title=f"{schedule.name}: its gate skipped every run for 30 days",
                    evidence=[
                        f"{skipped} runs skipped since {_day(since)} ({what})",
                        f"latest reason: {_skip_reason(session, schedule) or 'not recorded'}",
                    ],
                    count=skipped,
                )
            )
            continue
        upcoming = service.next_fire_time(schedule, now=since)
        if schedule.schedule_type == "date" and upcoming is None:
            title = f"{schedule.name}: its one time passed over 30 days ago and it is still on"
        elif upcoming is not None and aware(upcoming) <= now:
            title = f"{schedule.name}: no run in 30 days"
        else:
            continue  # its cadence is longer than 30 days, or its one time is ahead
        evidence = [what] + ([f"{skipped} run(s) skipped by its gate"] if skipped else [])
        found.append(
            Signal(key=f"unused_schedule:{schedule.id}", source="unused_schedule", title=title, evidence=evidence)
        )
    return found


def _skip_reason(session: Session, schedule: Schedule) -> str | None:
    event = session.scalar(
        select(WorkEvent)
        .where(WorkEvent.event_type == "schedule.occurrence_skipped", WorkEvent.entity_id == schedule.id)
        .order_by(WorkEvent.created_at.desc(), WorkEvent.id.desc())
        .limit(1)
    )
    if event is None:
        return None
    return str((event.payload or {}).get("reason") or "") or None


def unused_tools(session: Session, now: datetime) -> list[Signal]:
    """Tasque tools no run called in 30 days, once the runs record their tool calls for that long: one row
    for the core's and one for each extension's."""
    from tasque2.extensions import registry
    from tasque2.mcp.tools import CORE_TOOLS

    since = now - UNUSED
    usages = session.execute(
        select(ProviderRun.usage, ProviderRun.created_at).where(ProviderRun.created_at >= since)
    ).all()
    recording_since = min(
        (aware(created) for usage, created in usages if (usage or {}).get("tool_calls")), default=None
    )
    if recording_since is None or recording_since > since + timedelta(days=3):
        return []
    used = {
        name[len(TASQUE_TOOLS) :]
        for usage, _created in usages
        for name in ((usage or {}).get("tool_calls") or {})
        if str(name).startswith(TASQUE_TOOLS)
    }
    idle: dict[str, list[str]] = {}
    for tool in (*CORE_TOOLS, *registry().mcp_tools):
        if tool.__name__ in used or tool.__name__ == "submit_worker_result":
            continue
        owner = "core" if tool.__module__.startswith("tasque2.") else tool.__module__.split(".")[0]
        idle.setdefault(owner, []).append(tool.__name__)
    found = []
    for owner, names in sorted(idle.items()):
        names.sort()
        lines = [", ".join(names[start : start + 12]) for start in range(0, len(names), 12)]
        found.append(
            Signal(
                key=f"unused_tools:{owner}",
                source="unused_tool",
                title=f"{len(names)} {owner} tool(s): no run called them in 30 days",
                evidence=[f"{len(usages)} runs in 30 days called none of: {lines[0]}", *lines[1:]],
                count=len(names),
            )
        )
    return found


def _documents(session: Session) -> list[Memory]:
    return list(
        session.scalars(select(Memory).where(Memory.canonical_key.is_not(None), Memory.archived_at.is_(None))).all()
    )


def doctrine(session: Session) -> list[Signal]:
    """Documents within 10% of the cap their marker declares (or over it)."""
    found = []
    for memory in _documents(session):
        budget = canonical_budget(memory.content)
        if not budget or len(memory.content) < CAP_SHARE * budget:
            continue
        name = f"{memory.namespace}/{memory.canonical_key}"
        found.append(
            Signal(
                key=f"doctrine_cap:{name}",
                source="doctrine_cap",
                title=f"{name} is at {round(100 * len(memory.content) / budget)}% of its cap",
                evidence=[f"{len(memory.content):,} of {budget:,} characters"],
            )
        )
    return found


def open_decisions(session: Session) -> list[Signal]:
    """Questions a lane asked the user (``(asked)``) and nobody answered: once they stood a week."""
    found = []
    for memory in _documents(session):
        name = f"{memory.namespace}/{memory.canonical_key}"
        for line in (memory.content or "").splitlines():
            if OPEN_DECISION not in line or "(asked)" not in line:
                continue
            question = line.split(OPEN_DECISION, 1)[1].replace("(asked)", "").strip()
            found.append(
                Signal(
                    key=f"open_decision:{name}:{_key_hash(line.strip())}",
                    source="open_decision",
                    title=f"Unanswered in {name}: {clip(question, 120)}",
                    evidence=[clip(line.strip(), 280)],
                    since=aware(memory.updated_at),
                    min_age=DECISION_AGE,
                )
            )
    return found


def extension_signals(session: Session, now: datetime, *, strict: bool = False) -> list[Signal]:
    """What extensions report (``add_signal_source``); a source that fails is left out of this sweep."""
    from tasque2.extensions import registry

    found = []
    for name, source in registry().signal_sources:
        try:
            items = list(source(session, now) or [])
        except Exception:  # noqa: BLE001 - one broken source must not cost the rest
            if strict:
                raise
            logger.exception("Signal source %s failed", name)
            continue
        for item in items:
            if not isinstance(item, dict) or not item.get("key") or not item.get("title"):
                continue
            found.append(
                Signal(
                    key=f"{name}:{item['key']}",
                    source=str(item.get("source") or name),
                    title=str(item["title"]),
                    evidence=[str(line) for line in item.get("evidence") or []],
                    since=_moment(item.get("since")),
                )
            )
    return found


# --- the pass -------------------------------------------------------------------------------------------


def gather(
    session: Session,
    *,
    now: datetime | None = None,
    context: dict[str, Any] | None = None,
    skip_threads: set[str] | None = None,
    strict: bool = False,
) -> list[Signal]:
    """Every signal there is now. A gatherer that fails is logged and left out (``strict`` raises instead)."""
    now = aware(now or utc_now())
    steps: list[tuple[str, Callable[[], list[Signal]]]] = [
        ("faults", lambda: faults(now)),
        ("failed runs", lambda: failed_runs(session)),
        ("tool errors", lambda: tool_errors(session, now)),
        ("warnings", lambda: warnings(now)),
        ("slow runs", lambda: slow_runs(session, now)),
        ("pushback", lambda: pushback(session, now, skip_threads=set(skip_threads or ()))),
        ("schedules", lambda: schedules(session, now)),
        ("tools", lambda: unused_tools(session, now)),
        ("doctrine", lambda: doctrine(session)),
        ("packets", lambda: packets(session, now)),
        ("decisions", lambda: open_decisions(session)),
        ("extensions", lambda: extension_signals(session, now, strict=strict)),
        ("journal", lambda: journal(context or {}, now)),
    ]
    found: list[Signal] = []
    for name, step in steps:
        try:
            found += step()
        except Exception:  # noqa: BLE001 - one source that breaks must not cost the sweep
            if strict:
                raise
            logger.exception("The Workshop's sweep could not gather %s", name)
    return found


def sweep_worker(work_item: WorkItem) -> dict[str, Any]:
    """``function.workshop_sweep``: gather into the ledger, expire old cards, file fixes into free slots, and
    start a triage when there is something new. Posts nothing."""
    from tasque2.workshop import pipeline, triage

    session = pipeline._session(work_item)
    now = utc_now()
    context = effective_context(work_item.context)
    thread = work_item.discord_thread_id
    signals = gather(session, now=now, context=context, skip_threads={thread} if thread else set())
    tally = ledger.record(session, signals, now=now)
    expired = pipeline.expire_cards(session, now=now)
    filed = triage.file_waiting(session, thread_id=thread, now=now)
    run = triage.start(session, thread_id=thread, context=context)
    summary = (
        f"Swept {len(signals)} signal(s): {tally['new']} new, {tally['created']} first seen; "
        f"{len(filed)} fix(es) filed, {expired} card(s) expired" + ("; triage started." if run is not None else ".")
    )
    produced = {
        "silent": True,
        "signals": len(signals),
        **dict(tally),
        "expired": expired,
        "filed": filed,
        "triage_run_id": run.id if run is not None else None,
    }
    return {"summary": summary, "produces": produced}
