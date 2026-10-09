from __future__ import annotations

import subprocess
from pathlib import Path

from tasque2.db import session_scope
from tasque2.memory import MemoryService
from tasque2.ops import datarepo
from tasque2.ops.doctrine import apply_held, held_conflicts, held_edits, reversed_edits


def _git(repo: Path, *args: str) -> None:
    subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True, check=True)


def _doc(namespace: str, key: str, content: str) -> None:
    with session_scope() as session:
        MemoryService(session).upsert_canonical(
            namespace=namespace, canonical_key=key, kind="doctrine", content=content, pinned=True
        )


def _live(namespace: str, key: str) -> str | None:
    with session_scope() as session:
        memory = MemoryService(session).get_canonical(namespace=namespace, canonical_key=key)
        return memory.content if memory is not None else None


def test_a_change_lands_where_the_live_text_is_what_it_saw_and_nowhere_else(fresh_db: Path) -> None:
    root = fresh_db.parent
    _doc("cooking", "cooking_direction", "Cook simply.\n")
    _doc("global", "desk", "The desk.\n")
    _doc("global", "old_rule", "Retire me.\n")
    datarepo.init(root)
    base = datarepo.head(root)
    _git(root, "checkout", "-q", "-b", "workshop/c1")
    (root / "doctrine" / "cooking" / "cooking_direction.md").write_text("Cook simply; one pan.\n", encoding="utf-8")
    (root / "doctrine" / "global" / "desk.md").write_text("The desk, changed.\n", encoding="utf-8")
    (root / "doctrine" / "global" / "old_rule.md").unlink()
    (root / "doctrine" / "global" / "tasque_workshop.md").write_text("The Workshop.\n", encoding="utf-8")
    _git(root, "add", "-A")
    _git(root, "commit", "-q", "-m", "edit doctrine")
    edits = {f"{edit.namespace}/{edit.canonical_key}": edit for edit in held_edits(root, base)}
    assert set(edits) == {"cooking/cooking_direction", "global/desk", "global/old_rule", "global/tasque_workshop"}
    assert edits["global/old_rule"].after is None and edits["global/tasque_workshop"].before is None

    _doc("global", "desk", "The desk, as a worker rewrote it.\n")  # live moved on after the change began
    with session_scope() as session:
        assert held_conflicts(session, list(edits.values())) == ["global/desk: changed live since the change began"]
        changes = {f"{c.namespace}/{c.canonical_key}": c.status for c in apply_held(session, list(edits.values()))}
    assert changes == {
        "cooking/cooking_direction": "applied",
        "global/desk": "conflict",
        "global/old_rule": "retired",
        "global/tasque_workshop": "created",
    }
    assert _live("cooking", "cooking_direction") == "Cook simply; one pan.\n"
    assert _live("global", "desk") == "The desk, as a worker rewrote it.\n"  # left alone
    assert _live("global", "old_rule") is None and _live("global", "tasque_workshop") == "The Workshop.\n"

    with session_scope() as session:  # twice changes nothing
        again = {c.status for c in apply_held(session, [e for k, e in edits.items() if k != "global/desk"])}
    assert again == {"unchanged"}
    with session_scope() as session:  # and the undo puts it back
        undo = [e for k, e in edits.items() if k in ("cooking/cooking_direction",)]
        apply_held(session, reversed_edits(undo))
    assert _live("cooking", "cooking_direction") == "Cook simply.\n"
