"""The Workshop's ledger: one row per distinct issue, counting each occurrence once, worth a look once it
recurs or stands long enough, and back for another look only when a rest ends."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

from tasque2.db import session_scope
from tasque2.models import utc_now
from tasque2.workshop import ledger
from tasque2.workshop.ledger import Signal

NOW = utc_now()


def _fault(*days_ago: float) -> Signal:
    return Signal(
        key="fault:abc",
        source="fault",
        title="KeyError in tasque2/digest.py:build",
        evidence=["KeyError: 'lane'"],
        occurrences=[NOW - timedelta(days=days) for days in days_ago],
        min_count=2,
    )


def test_an_event_is_counted_once_however_often_the_sweep_looks(fresh_db: Path) -> None:
    with session_scope() as session:
        assert ledger.record(session, [_fault(1)])["created"] == 1
        row = ledger.row(session, "fault:abc")
        assert (row.count, row.status) == (1, ledger.WATCHING)  # once is not yet worth a look
        ledger.record(session, [_fault(1)])  # the same occurrence, seen again
        assert row.count == 1 and row.status == ledger.WATCHING
        tally = ledger.record(session, [_fault(1, 0.5)])
        assert tally["new"] == 1 and (row.count, row.status) == (2, ledger.NEW)
        assert row.first_seen_at < row.last_seen_at


def test_a_condition_stands_and_takes_its_size_and_a_minimum_age(fresh_db: Path) -> None:
    now = utc_now()
    standing = Signal(key="open_decision:x", source="open_decision", title="Unanswered", min_age=timedelta(days=7))
    with session_scope() as session:
        ledger.record(session, [Signal(key="gate_skip:s1", source="gate_skip", title="Skips", count=12)], now=now)
        assert ledger.row(session, "gate_skip:s1").count == 12
        assert ledger.row(session, "gate_skip:s1").status == ledger.NEW
        ledger.record(session, [Signal(key="gate_skip:s1", source="gate_skip", title="Skips", count=15)], now=now)
        assert ledger.row(session, "gate_skip:s1").count == 15
        ledger.record(session, [standing], now=now)
        assert ledger.row(session, "open_decision:x").status == ledger.WATCHING  # asked a moment ago
        ledger.record(session, [standing], now=now + timedelta(days=8))
        assert ledger.row(session, "open_decision:x").status == ledger.NEW
        older = Signal(key="open_decision:y", source="open_decision", title="Old", since=now - timedelta(days=10))
        ledger.record(session, [older], now=now)
        assert ledger.row(session, "open_decision:y").status == ledger.NEW  # it stood a week before it was seen


def test_a_resting_row_comes_back_only_when_its_rest_ends_and_it_happens_again(fresh_db: Path) -> None:
    now = NOW
    with session_scope() as session:
        ledger.record(session, [_fault(2, 1)], now=now)
        ledger.set_aside(session, ["fault:abc"], until=now + ledger.SET_ASIDE)
        row = ledger.row(session, "fault:abc")
        later = Signal(**{**_fault().__dict__, "occurrences": [now + timedelta(days=1)]})
        ledger.record(session, [later], now=now + timedelta(days=1))
        assert row.status == ledger.RESTING and row.count == 3  # still counted while it rests
        quiet = Signal(**{**_fault().__dict__, "occurrences": [now + timedelta(days=1)]})
        ledger.record(session, [quiet], now=now + timedelta(days=61))
        assert row.status == ledger.RESTING  # its rest ended, but nothing new happened
        again = Signal(**{**_fault().__dict__, "occurrences": [now + timedelta(days=61)]})
        assert ledger.record(session, [again], now=now + timedelta(days=61))["woken"] == 1
        assert row.status == ledger.NEW and row.rest_until is None


def test_a_filed_change_settles_its_rows_by_how_it_ended(fresh_db: Path) -> None:
    now = utc_now()
    signals = [Signal(key=f"warning:{name}", source="warning", title=name) for name in ("a", "b", "c")]
    with session_scope() as session:
        ledger.record(session, signals, now=now)
        ledger.propose(session, ["warning:a", "warning:b"], {"id": "g1", "title": "Quiet the warnings"})
        ledger.propose(session, ["warning:c"], {"id": "g2", "title": "Another"})
        groups = ledger.waiting_groups(session)
        assert [(proposal["id"], [row.key for row in rows]) for proposal, rows in groups] == [
            ("g1", ["warning:a", "warning:b"]),
            ("g2", ["warning:c"]),
        ]
        ledger.mark_filed(session, ["warning:a", "warning:b"], change_id="c1", run_id="r1", now=now)
        ledger.mark_filed(session, ["warning:c"], change_id="c2", run_id="r2", now=now)
        ledger.refile(session, "c2", new_change_id="c3", run_id="r3")  # planned again
        assert ledger.settle(session, "c1", ledger.SHIPPED, now=now) == 2
        assert ledger.settle(session, "c3", ledger.DISCARDED, now=now) == 1
        a, c = ledger.row(session, "warning:a"), ledger.row(session, "warning:c")
        assert a.status == ledger.DONE and a.rest_until == now + ledger.ONE_TRY and a.tried_at == now
        assert c.status == ledger.RESTING and c.rest_until == now + ledger.SET_ASIDE
        assert "Ledger: 1 resting, 2 done." in ledger.status_lines(session)


def test_ideas_are_offered_once_rested_for_a_while_and_retired_for_good(fresh_db: Path) -> None:
    now = utc_now()
    with session_scope() as session:
        key = ledger.idea_key("A birthday nudge, a week ahead!")
        assert key == "idea:a-birthday-nudge-a-week-ahead"
        assert ledger.idea_open(ledger.row(session, key), now)
        ledger.offer_idea(session, key=key, title="A birthday nudge", detail={"area": "people"}, run_id="r1", now=now)
        assert not ledger.idea_open(ledger.row(session, key), now)  # on a card
        ledger.answer_idea(session, key, ledger.RESTING, until=now + ledger.SET_ASIDE)
        assert not ledger.idea_open(ledger.row(session, key), now + timedelta(days=59))
        assert ledger.idea_open(ledger.row(session, key), now + timedelta(days=61))
        ledger.answer_idea(session, key, ledger.RETIRED)
        assert not ledger.idea_open(ledger.row(session, key), now + timedelta(days=400))
        history = ledger.idea_history(session, now=now)
        assert history == [
            {
                "title": "A birthday nudge",
                "area": "people",
                "exploratory": False,
                "outcome": "declined",
                "offered": history[0]["offered"],
                "rests_until": None,
            }
        ]
        assert ledger.counts(session) == {}  # ideas are not issues
