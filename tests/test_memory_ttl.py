from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest
from sqlalchemy import select

from tasque2.config import reset_settings
from tasque2.daemon import TasqueDaemon
from tasque2.db import session_scope
from tasque2.memory import MemoryService, expire_ttl_memories
from tasque2.models import Memory, utc_now


def _note(session, *, age_days: int, ttl_days: int | None, **kwargs) -> str:
    memory = MemoryService(session).create_memory(
        namespace="global",
        kind="working",
        content=f"note aged {age_days}d ttl {ttl_days}",
        ttl_days=ttl_days,
        **kwargs,
    )
    memory.created_at = utc_now() - timedelta(days=age_days)
    session.flush()
    return memory.id


def test_expire_ttl_archives_only_elapsed_unpinned_rows(fresh_db: Path) -> None:
    # Every write may carry ttl_days, but nothing ever evaluated it, so reply
    # notes and run reports piled up forever. Expiry archives what has lapsed and
    # leaves alone what is still inside its window, has no TTL, or is pinned --
    # a TTL on a pinned doc is a mistake to surface, never to act on.
    with session_scope() as session:
        expired = _note(session, age_days=200, ttl_days=120)
        fresh = _note(session, age_days=10, ttl_days=120)
        forever = _note(session, age_days=400, ttl_days=None)
        pinned = _note(session, age_days=400, ttl_days=30, pinned=True)
        canonical = MemoryService(session).upsert_canonical(
            namespace="global",
            canonical_key="state_doc",
            kind="canonical",
            content="state doc body",
            ttl_days=1,
        )
        canonical.created_at = utc_now() - timedelta(days=30)
        canonical_id = canonical.id
        session.flush()

    with session_scope() as session:
        assert expire_ttl_memories(session) == 1

    with session_scope() as session:
        archived = {row.id for row in session.scalars(select(Memory).where(Memory.archived_at.is_not(None)))}
        assert archived == {expired}
        for kept in (fresh, forever, pinned, canonical_id):
            assert session.get(Memory, kept).archived_at is None

    with session_scope() as session:
        assert expire_ttl_memories(session) == 0  # idempotent


def test_daemon_tick_expires_ttl_once_per_interval(fresh_db: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TASQUE2_MEMORY_TTL_INTERVAL_SECONDS", "3600")
    reset_settings()
    try:
        with session_scope() as session:
            first = _note(session, age_days=200, ttl_days=120)

        with session_scope() as session:
            result = TasqueDaemon(session).run_once(max_work_items=0)
        assert result.memories_expired == 1
        assert result.has_activity

        with session_scope() as session:
            assert session.get(Memory, first).archived_at is not None
            # Another lapsed row appears, but the interval has not elapsed.
            second = _note(session, age_days=200, ttl_days=120)

        with session_scope() as session:
            result = TasqueDaemon(session).run_once(max_work_items=0)
        assert result.memories_expired == 0

        with session_scope() as session:
            assert session.get(Memory, second).archived_at is None
    finally:
        reset_settings()


def test_daemon_ttl_pass_disabled_by_zero_interval(fresh_db: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TASQUE2_MEMORY_TTL_INTERVAL_SECONDS", "0")
    reset_settings()
    try:
        with session_scope() as session:
            lapsed = _note(session, age_days=200, ttl_days=120)
        with session_scope() as session:
            result = TasqueDaemon(session).run_once(max_work_items=0)
        assert result.memories_expired == 0
        with session_scope() as session:
            assert session.get(Memory, lapsed).archived_at is None
    finally:
        reset_settings()
