from __future__ import annotations

import hashlib
import os
from datetime import timedelta
from pathlib import Path

import pytest
from sqlalchemy import select

from tasque2.artifacts import ArtifactService, ArtifactStore, prune_artifacts
from tasque2.config import get_settings, reset_settings
from tasque2.daemon import DaemonTick
from tasque2.db import session_scope
from tasque2.models import Artifact, utc_now
from tasque2.work.repository import WorkRepository


def _write(session, *, kind: str, age_days: int, body: str = "x" * 4096) -> Artifact:
    artifact = ArtifactStore().write_text(session, kind=kind, title=f"{kind}-{age_days}d", content=body)
    artifact.created_at = utc_now() - timedelta(days=age_days)
    session.flush()
    return artifact


def test_write_text_stores_the_file_with_its_metadata(fresh_db: Path) -> None:
    with session_scope() as session:
        work = WorkRepository(session).create_work_item(title="Report", task_instruction="Write.", worker_kind="manual")
        artifact = ArtifactStore().write_text(
            session,
            kind="worker_report",
            title="Run report",
            content="# Done\n",
            suffix=".md",
            work_item_id=work.id,
            tags=["report", "daily"],
            source_kind="provider_run",
            source_id="run-1",
        )

        path = Path(artifact.local_path)
        assert path.parent == get_settings().resolved_artifact_dir
        assert path.suffix == ".md"
        assert path.read_text(encoding="utf-8") == "# Done\n"
        assert artifact.size_bytes == len(b"# Done\n")
        assert artifact.sha256 == hashlib.sha256(b"# Done\n").hexdigest()
        assert artifact.content_type == "text/markdown; charset=utf-8"
        assert (artifact.kind, artifact.title, artifact.work_item_id) == ("worker_report", "Run report", work.id)
        assert artifact.tags == ["report", "daily"]
        assert (artifact.source_kind, artifact.source_id) == ("provider_run", "run-1")


def test_write_text_defaults_to_plain_text(fresh_db: Path) -> None:
    with session_scope() as session:
        artifact = ArtifactStore().write_text(session, kind="note", title="Note", content="hello")

        assert Path(artifact.local_path).suffix == ".txt"
        assert artifact.content_type == "text/plain; charset=utf-8"
        assert artifact.tags == []


def test_write_bytes_takes_type_and_suffix_from_the_title(fresh_db: Path) -> None:
    with session_scope() as session:
        artifact = ArtifactStore().write_bytes(session, kind="image", title="chart.png", content=b"\x89PNG")

        assert Path(artifact.local_path).suffix == ".png"
        assert Path(artifact.local_path).read_bytes() == b"\x89PNG"
        assert artifact.content_type == "image/png"
        assert artifact.size_bytes == 4


def test_suffix_cannot_leave_the_artifact_directory(fresh_db: Path) -> None:
    store = ArtifactStore()
    with session_scope() as session:
        artifact = store.write_text(session, kind="note", title="Sneaky", content="x", suffix="/../../evil sh")

    path = Path(artifact.local_path)
    assert path.parent == store.base_dir
    assert "/" not in path.name and "\\" not in path.name and " " not in path.name


def test_capture_file_copies_the_source(fresh_db: Path, tmp_path: Path) -> None:
    source = tmp_path / "page.html"
    source.write_text("<html></html>", encoding="utf-8")
    stamp = 1_700_000_000
    os.utime(source, (stamp, stamp))

    with session_scope() as session:
        artifact = ArtifactStore().capture_file(session, path=source, kind="capture", tags=["web"])

        copy = Path(artifact.local_path)
        assert copy != source
        assert copy.read_text(encoding="utf-8") == "<html></html>"
        assert copy.stat().st_mtime == pytest.approx(stamp)
        assert source.is_file()
        assert artifact.title == "page.html"
        assert artifact.content_type == "text/html"
        assert artifact.sha256 == hashlib.sha256(b"<html></html>").hexdigest()
        assert artifact.size_bytes == len(b"<html></html>")


def test_capture_file_rejects_a_missing_source(fresh_db: Path, tmp_path: Path) -> None:
    with session_scope() as session, pytest.raises(FileNotFoundError):
        ArtifactStore().capture_file(session, path=tmp_path / "absent.txt", kind="capture")


def test_list_artifacts_filters(fresh_db: Path) -> None:
    store = ArtifactStore()
    with session_scope() as session:
        work = WorkRepository(session).create_work_item(title="Owner", task_instruction="Own.", worker_kind="manual")
        report = store.write_text(
            session, kind="worker_report", title="Weekly summary", content="r", tags=["report", "weekly"]
        )
        report.work_item_id = work.id
        stream = store.write_text(
            session, kind="provider_stream", title="claude stream", content="s", source_kind="provider_run"
        )
        archived = store.write_text(session, kind="worker_report", title="Old summary", content="o", tags=["report"])
        ArtifactService(session).archive_artifact(archived.id)
        service = ArtifactService(session)

        assert [item.id for item in service.list_artifacts(kind="worker_report")] == [report.id]
        assert [item.id for item in service.list_artifacts(tag=["report", "weekly"])] == [report.id]
        assert [item.id for item in service.list_artifacts(query="STREAM")] == [stream.id]
        assert [item.id for item in service.list_artifacts(work_item_id=work.id)] == [report.id]
        assert [item.id for item in service.list_artifacts(source_kind="provider_run")] == [stream.id]
        assert {item.id for item in service.list_artifacts(tag=["report"], include_archived=True)} == {
            report.id,
            archived.id,
        }
        assert len(service.list_artifacts(limit=1)) == 1


def test_filtered_listing_finds_matches_behind_many_newer_artifacts(fresh_db: Path, monkeypatch) -> None:
    monkeypatch.setattr("tasque2.artifacts._LIST_SCAN_BATCH", 4)
    store = ArtifactStore()
    with session_scope() as session:
        upload = store.write_text(session, kind="capture", title="resume.pdf", content="x", tags=["discord_upload"])
        upload.created_at = utc_now() - timedelta(days=1)
        for index in range(10):
            store.write_text(session, kind="provider_stream", title=f"stream {index}", content="x")
        service = ArtifactService(session)

        assert [item.id for item in service.list_artifacts(tag=["discord_upload"], limit=1)] == [upload.id]
        assert [item.id for item in service.list_artifacts(query="resume", limit=2)] == [upload.id]
        assert len(service.list_artifacts(query="stream", limit=6)) == 6


def test_get_and_archive_artifact(fresh_db: Path) -> None:
    with session_scope() as session:
        artifact = ArtifactStore().write_text(session, kind="note", title="Keep", content="x")
        service = ArtifactService(session)

        assert service.get_artifact(artifact.id) is artifact
        archived_at = service.archive_artifact(artifact.id).archived_at
        assert archived_at is not None
        assert service.archive_artifact(artifact.id).archived_at == archived_at
        assert Path(artifact.local_path).is_file()
        with pytest.raises(KeyError):
            service.get_artifact("no-such-artifact")


def test_prune_removes_aged_files_but_keeps_the_record(fresh_db: Path) -> None:
    with session_scope() as session:
        old = _write(session, kind="provider_stream", age_days=45)
        recent = _write(session, kind="provider_stream", age_days=3)
        other = _write(session, kind="worker_report", age_days=45)
        old_path, recent_path, other_path = (Path(item.local_path) for item in (old, recent, other))
        old_id = old.id

    with session_scope() as session:
        result = prune_artifacts(session)

    assert result.pruned == 1
    assert result.bytes_freed >= 4096
    assert not old_path.exists()
    assert recent_path.is_file()
    assert other_path.is_file()
    with session_scope() as session:
        assert session.get(Artifact, old_id).archived_at is not None


def test_prune_is_idempotent_and_survives_a_missing_file(fresh_db: Path) -> None:
    with session_scope() as session:
        Path(_write(session, kind="provider_stream", age_days=45).local_path).unlink()

    with session_scope() as session:
        first = prune_artifacts(session)
    with session_scope() as session:
        second = prune_artifacts(session)

    assert (first.pruned, first.bytes_freed) == (1, 0)
    assert second.pruned == 0


def test_prune_disabled_by_zero_window(fresh_db: Path) -> None:
    with session_scope() as session:
        path = Path(_write(session, kind="provider_stream", age_days=400).local_path)

    with session_scope() as session:
        result = prune_artifacts(session, older_than_days=0)

    assert result.pruned == 0
    assert path.is_file()


def test_prune_follows_the_configured_kinds_and_window(fresh_db: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TASQUE2_ARTIFACT_RETENTION_KINDS", "provider_stream, worker_report")
    monkeypatch.setenv("TASQUE2_ARTIFACT_RETENTION_DAYS", "10")
    reset_settings()
    with session_scope() as session:
        stream = _write(session, kind="provider_stream", age_days=11)
        report = _write(session, kind="worker_report", age_days=11)
        capture = _write(session, kind="capture", age_days=11)
        young = _write(session, kind="worker_report", age_days=9)

    with session_scope() as session:
        assert prune_artifacts(session).pruned == 2
        assert prune_artifacts(session, kinds=["capture"]).pruned == 1
        assert prune_artifacts(session, kinds=[]).pruned == 0
        archived = {row.id for row in session.scalars(select(Artifact).where(Artifact.archived_at.is_not(None)))}

    assert archived == {stream.id, report.id, capture.id}
    assert young.id not in archived


def test_prune_takes_the_oldest_first_up_to_the_limit(fresh_db: Path) -> None:
    with session_scope() as session:
        oldest = _write(session, kind="provider_stream", age_days=90)
        middle = _write(session, kind="provider_stream", age_days=60)
        newest = _write(session, kind="provider_stream", age_days=40)

    with session_scope() as session:
        assert prune_artifacts(session, limit=2).pruned == 2
        remaining = [row.id for row in session.scalars(select(Artifact).where(Artifact.archived_at.is_(None)))]

    assert remaining == [newest.id]
    assert not Path(oldest.local_path).exists()
    assert not Path(middle.local_path).exists()


def test_daemon_tick_prunes_once_per_interval(fresh_db: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TASQUE2_ARTIFACT_RETENTION_INTERVAL_SECONDS", "3600")
    reset_settings()
    with session_scope() as session:
        first_path = Path(_write(session, kind="provider_stream", age_days=45).local_path)
    tick = DaemonTick()

    with session_scope() as session:
        assert tick.run(session, max_claims=0).artifacts_pruned == 1
    assert not first_path.exists()

    with session_scope() as session:
        second_path = Path(_write(session, kind="provider_stream", age_days=45).local_path)
    with session_scope() as session:
        assert tick.run(session, max_claims=0).artifacts_pruned == 0
    assert second_path.is_file()


def test_pruned_artifacts_drop_out_of_live_listings(fresh_db: Path) -> None:
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
        assert len(rows) == 2
