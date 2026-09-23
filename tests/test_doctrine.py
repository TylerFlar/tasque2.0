from __future__ import annotations

import json
from pathlib import Path

from tasque2.db import session_scope
from tasque2.memory import MemoryService
from tasque2.ops.doctrine import SNAPSHOT_FILE, apply_doctrine, content_hash, export_doctrine


def _seed(session, key: str, content: str, namespace: str = "finance") -> None:
    MemoryService(session).upsert_canonical(
        namespace=namespace, canonical_key=key, kind="doctrine", content=content, tags=[namespace], pinned=True
    )


def _statuses(changes) -> dict[str, str]:
    return {f"{change.namespace}/{change.canonical_key}": change.status for change in changes}


def test_export_writes_each_document_and_a_snapshot_of_its_hash(fresh_db: Path, tmp_path: Path) -> None:
    with session_scope() as session:
        _seed(session, "finance_direction", "Pay every bill on time.")
        entries = export_doctrine(session, tmp_path / "doctrine")

    assert (tmp_path / "doctrine" / "finance" / "finance_direction.md").read_text(encoding="utf-8") == (
        "Pay every bill on time."
    )
    snapshot = json.loads((tmp_path / "doctrine" / SNAPSHOT_FILE).read_text(encoding="utf-8"))
    assert snapshot == entries
    assert snapshot[0]["sha256"] == content_hash("Pay every bill on time.")


def test_apply_updates_retires_creates_and_protects_drifted_documents(fresh_db: Path, tmp_path: Path) -> None:
    directory = tmp_path / "doctrine"
    with session_scope() as session:
        _seed(session, "finance_direction", "Pay every bill on time.")
        _seed(session, "finance_old_rules", "Rules nobody uses.")
        _seed(session, "finance_ledger", "Ledger conventions.")
        _seed(session, "finance_steady", "Unchanged rule.")
        export_doctrine(session, directory)

    (directory / "finance" / "finance_direction.md").write_bytes(b"Pay every bill on time.\r\nNever overdraw.")
    (directory / "finance" / "finance_old_rules.md").unlink()
    (directory / "finance" / "finance_ledger.md").write_text("Edited from the file.", encoding="utf-8")
    (directory / "career").mkdir()
    (directory / "career" / "career_direction.md").write_text("Apply where the fit is real.", encoding="utf-8")
    with session_scope() as session:
        _seed(session, "finance_ledger", "Edited by a worker after the export.")

    with session_scope() as session:
        preview = apply_doctrine(session, directory, dry_run=True)
        session.rollback()
    with session_scope() as session:
        applied = apply_doctrine(session, directory)
        service = MemoryService(session)
        direction = service.get_canonical(namespace="finance", canonical_key="finance_direction")
        retired = service.get_canonical(namespace="finance", canonical_key="finance_old_rules")
        ledger = service.get_canonical(namespace="finance", canonical_key="finance_ledger")
        created = service.get_canonical(namespace="career", canonical_key="career_direction")

        assert direction.content == "Pay every bill on time.\nNever overdraw."
        assert retired is None
        assert ledger.content == "Edited by a worker after the export."
        assert created.content == "Apply where the fit is real."
        assert created.pinned is True

    expected = {
        "finance/finance_direction": "applied",
        "finance/finance_old_rules": "retired",
        "finance/finance_ledger": "drifted",
        "finance/finance_steady": "unchanged",
        "career/career_direction": "created",
    }
    assert _statuses(preview) == expected
    assert _statuses(applied) == expected


def test_apply_leaves_a_live_document_it_never_exported_alone(fresh_db: Path, tmp_path: Path) -> None:
    directory = tmp_path / "doctrine"
    with session_scope() as session:
        export_doctrine(session, directory)
        _seed(session, "finance_state", "Live state the export did not include.")
    (directory / "finance").mkdir()
    (directory / "finance" / "finance_state.md").write_text("A file written blind.", encoding="utf-8")

    with session_scope() as session:
        changes = apply_doctrine(session, directory)
        state = MemoryService(session).get_canonical(namespace="finance", canonical_key="finance_state")

    assert _statuses(changes) == {"finance/finance_state": "unmanaged"}
    assert state.content == "Live state the export did not include."


def test_apply_reports_a_document_over_its_budget_and_keeps_the_live_one(fresh_db: Path, tmp_path: Path) -> None:
    directory = tmp_path / "doctrine"
    with session_scope() as session:
        _seed(session, "finance_profile", "<!-- tasque:max_chars=60 -->\nShort profile.")
        export_doctrine(session, directory)
    (directory / "finance" / "finance_profile.md").write_text(
        "<!-- tasque:max_chars=60 -->\n" + "Far too long. " * 20, encoding="utf-8"
    )

    with session_scope() as session:
        changes = apply_doctrine(session, directory)
        profile = MemoryService(session).get_canonical(namespace="finance", canonical_key="finance_profile")

    assert _statuses(changes) == {"finance/finance_profile": "over_budget"}
    assert profile.content.endswith("Short profile.")


def test_keys_that_are_not_valid_filenames_round_trip(fresh_db: Path, tmp_path: Path) -> None:
    directory = tmp_path / "doctrine"
    with session_scope() as session:
        _seed(session, "todo:career apply?", "Old todo.", namespace="global")
        export_doctrine(session, directory)
    [written] = list((directory / "global").glob("*.md"))
    written.write_text("Updated todo.", encoding="utf-8")

    with session_scope() as session:
        changes = apply_doctrine(session, directory)
        todo = MemoryService(session).get_canonical(namespace="global", canonical_key="todo:career apply?")

    assert written.name == "todo%3Acareer apply%3F.md"
    assert _statuses(changes) == {"global/todo:career apply?": "applied"}
    assert todo.content == "Updated todo."


def test_export_leaves_documents_that_hold_credentials_off_disk(fresh_db: Path, tmp_path: Path) -> None:
    directory = tmp_path / "doctrine"
    with session_scope() as session:
        _seed(session, "career_direction", "Apply where the fit is real.", namespace="career")
        _seed(session, "career_credentials", "site: hunter2", namespace="career")
        entries = export_doctrine(session, directory)
    (directory / "career" / "career_credentials.md").write_text("Overwritten blind.", encoding="utf-8")

    with session_scope() as session:
        changes = apply_doctrine(session, directory)
        credentials = MemoryService(session).get_canonical(namespace="career", canonical_key="career_credentials")

    assert [entry["canonical_key"] for entry in entries] == ["career_direction"]
    assert _statuses(changes) == {"career/career_credentials": "unmanaged", "career/career_direction": "unchanged"}
    assert credentials.content == "site: hunter2"
