from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from tasque2.db import session_scope
from tasque2.models import AgentResult, utc_now
from tasque2.worker import results


def _age(token: str, delta: timedelta) -> None:
    with session_scope() as session:
        row = session.get(AgentResult, token)
        row.created_at = utc_now() - delta


def test_tokens_are_unique_hex_strings() -> None:
    tokens = {results.mint_token() for _ in range(20)}

    assert len(tokens) == 20
    assert all(len(token) == 32 and int(token, 16) >= 0 for token in tokens)


def test_deposited_result_is_visible_until_consumed(fresh_db: Path) -> None:
    token = results.mint_token()

    assert results.peek(token) is False
    results.deposit(result_token=token, payload={"summary": "Done.", "report": "All good."})

    assert results.peek(token) is True
    assert results.read_and_consume(token) == {"summary": "Done.", "report": "All good."}
    assert results.peek(token) is False
    assert results.read_and_consume(token) is None


def test_a_second_deposit_replaces_the_first(fresh_db: Path) -> None:
    token = results.mint_token()

    results.deposit(result_token=token, payload={"summary": "first"})
    results.deposit(result_token=token, payload={"summary": "second"})

    assert results.read_and_consume(token) == {"summary": "second"}
    assert results.peek(token) is False


def test_results_are_scoped_to_their_agent_kind(fresh_db: Path) -> None:
    token = results.mint_token()

    results.deposit(result_token=token, payload={"summary": "planned"}, agent_kind="planner")

    assert results.peek(token) is False
    assert results.peek(token, agent_kind="planner") is True
    assert results.read_and_consume(token, agent_kind="planner") == {"summary": "planned"}


def test_deposit_requires_a_token(fresh_db: Path) -> None:
    with pytest.raises(ValueError, match="result_token is required."):
        results.deposit(result_token=" ", payload={"summary": "x"})


def test_values_json_cannot_hold_are_stored_as_text(fresh_db: Path) -> None:
    token = results.mint_token()
    when = datetime(2026, 9, 22, 7, 30, tzinfo=UTC)

    results.deposit(result_token=token, payload={"when": when, "path": Path("reports/a.md")})

    assert results.read_and_consume(token) == {"when": str(when), "path": str(Path("reports/a.md"))}


def test_unparseable_payload_reads_as_missing(fresh_db: Path) -> None:
    with session_scope() as session:
        session.add(AgentResult(result_token="bad", agent_kind="worker", payload_json="not json"))
        session.add(AgentResult(result_token="list", agent_kind="worker", payload_json="[1, 2]"))

    assert results.read_and_consume("bad") is None
    assert results.read_and_consume("list") is None


def test_consume_for_work_item_takes_the_newest_payload_in_the_callers_session(fresh_db: Path) -> None:
    older, newer, other = results.mint_token(), results.mint_token(), results.mint_token()
    results.deposit(result_token=older, payload={"summary": "older", "work_item_id": "work-1"})
    results.deposit(result_token=newer, payload={"summary": "newer", "work_item_id": "work-1"})
    results.deposit(result_token=other, payload={"summary": "other", "work_item_id": "work-2"})
    _age(older, timedelta(minutes=5))

    with session_scope() as session:
        payload = results.consume_for_work_item(session, "work-1")
        assert session.get(AgentResult, newer) is None

    assert payload == {"summary": "newer", "work_item_id": "work-1"}
    assert results.peek(newer) is False
    assert results.peek(older) is True
    assert results.peek(other) is True


def test_consume_for_work_item_skips_unparseable_rows_and_unknown_work(fresh_db: Path) -> None:
    token = results.mint_token()
    results.deposit(result_token=token, payload={"summary": "ok", "work_item_id": "work-1"})
    with session_scope() as session:
        session.add(AgentResult(result_token="bad", agent_kind="worker", payload_json="{broken"))

    with session_scope() as session:
        assert results.consume_for_work_item(session, "work-9") is None
        assert results.consume_for_work_item(session, "work-1") == {"summary": "ok", "work_item_id": "work-1"}
        with pytest.raises(ValueError, match="work_item_id is required."):
            results.consume_for_work_item(session, " ")


def test_reap_stale_deletes_only_old_payloads(fresh_db: Path) -> None:
    old, fresh = results.mint_token(), results.mint_token()
    results.deposit(result_token=old, payload={"summary": "old"})
    results.deposit(result_token=fresh, payload={"summary": "fresh"})
    _age(old, timedelta(hours=2))

    assert results.reap_stale() == 1
    assert results.peek(old) is False
    assert results.peek(fresh) is True
    assert results.reap_stale(max_age_seconds=0) == 1
    assert results.peek(fresh) is False
