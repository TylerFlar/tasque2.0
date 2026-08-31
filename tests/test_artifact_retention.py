from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest
from sqlalchemy import select

from tasque2.artifacts import ArtifactStore, prune_artifacts
from tasque2.config import reset_settings
from tasque2.daemon import TasqueDaemon
from tasque2.db import session_scope
from tasque2.models import Artifact, utc_now


def _write(session, *, kind: str, age_days: int, body: str = "x" * 4096) -> Artifact:
    artifact = ArtifactStore().write_text(session, kind=kind, title=f"{kind}-{age_days}d", content=body)
    artifact.created_at = utc_now() - timedelta(days=age_days)
    session.flush()
    return artifact


def test_prune_removes_aged_files_but_keeps_the_record(fresh_db: Path) -> None:
    # Raw provider streams are debug material: one per run, and nothing ever
    # collected them (4.9GB of a 6.2GB store by 2026-08). Retention deletes the
    # bytes but keeps the row, so what-ran history survives the cleanup.
    with session_scope() as session:
        old = _write(session, kind="provider_stream", age_days=45)
        recent = _write(session, kind="provider_stream", age_days=3)
        other = _write(session, kind="worker_report", age_days=45)
        old_path, recent_path, other_path = (Path(a.local_path) for a in (old, recent, other))
        old_id = old.id

    assert old_path.is_file() and recent_path.is_file() and other_path.is_file()

    with session_scope() as session:
        result = prune_artifacts(session)

    assert result.pruned == 1
    assert result.bytes_freed >= 4096
    # Aged stream: bytes gone, row retained and archived.
    assert not old_path.exists()
    # Inside the window, and a kind outside the policy: both untouched.
    assert recent_path.is_file()
    assert other_path.is_file()

    with session_scope() as session:
        row = session.get(Artifact, old_id)
        assert row is not None
        assert row.archived_at is not None


def test_prune_is_idempotent_and_survives_a_missing_file(fresh_db: Path) -> None:
    # A half-cleaned store (file removed by hand, row still live) must converge
    # instead of being rescanned forever.
    with session_scope() as session:
        artifact = _write(session, kind="provider_stream", age_days=45)
        Path(artifact.local_path).unlink()

    with session_scope() as session:
        first = prune_artifacts(session)
    with session_scope() as session:
        second = prune_artifacts(session)

    assert first.pruned == 1
    assert first.bytes_freed == 0
    assert second.pruned == 0  # already archived; not revisited


def test_prune_disabled_by_zero_window(fresh_db: Path) -> None:
    with session_scope() as session:
        artifact = _write(session, kind="provider_stream", age_days=400)
        path = Path(artifact.local_path)

    with session_scope() as session:
        result = prune_artifacts(session, older_than_days=0)

    assert result.pruned == 0
    assert path.is_file()


def test_daemon_tick_prunes_once_per_interval(
    fresh_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Retention is bookkeeping: it must run on a tick, then stay quiet until the
    # interval elapses rather than rescanning the store every few seconds.
    monkeypatch.setenv("TASQUE2_ARTIFACT_RETENTION_INTERVAL_SECONDS", "3600")
    reset_settings()
    try:
        with session_scope() as session:
            first_path = Path(_write(session, kind="provider_stream", age_days=45).local_path)

        with session_scope() as session:
            result = TasqueDaemon(session).run_once(max_work_items=0)
        assert result.artifacts_pruned == 1
        assert not first_path.exists()

        # A second aged artifact appears, but the interval has not elapsed.
        with session_scope() as session:
            second_path = Path(_write(session, kind="provider_stream", age_days=45).local_path)

        with session_scope() as session:
            result = TasqueDaemon(session).run_once(max_work_items=0)
        assert result.artifacts_pruned == 0
        assert second_path.is_file()
    finally:
        reset_settings()


def test_pruned_artifacts_drop_out_of_live_listings(fresh_db: Path) -> None:
    # Read paths filter on archived_at, so a pruned artifact must not be offered
    # to a worker that would then try to open a file that is gone.
    from tasque2.artifacts import ArtifactService

    with session_scope() as session:
        _write(session, kind="provider_stream", age_days=45)
        _write(session, kind="provider_stream", age_days=1)

    with session_scope() as session:
        prune_artifacts(session)

    with session_scope() as session:
        live = ArtifactService(session).list_artifacts(kind="provider_stream", limit=50)
        assert len(live) == 1
        assert Path(live[0].local_path).is_file()

        rows = session.scalars(select(Artifact).where(Artifact.kind == "provider_stream")).all()
        assert len(rows) == 2  # nothing was actually deleted from the ledger
