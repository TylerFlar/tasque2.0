"""The Workshop's test shop: a core repository with one extension and a config repository, scripted plan and
build runs, and the helpers that drive a change through them."""

from __future__ import annotations

import json
import subprocess
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import select

from tasque2.config import get_settings, reset_settings
from tasque2.db import session_scope
from tasque2.memory import MemoryService
from tasque2.models import WorkflowNode, WorkItem
from tasque2.ops import datarepo
from tasque2.ops.rehearse import Rehearsal
from tasque2.work.queue import WorkQueue
from tasque2.work.runner import WorkRunner
from tasque2.workflows import WorkflowService
from tasque2.workshop import pipeline, verify

TEMPLATE = "work-templates/cooking/reply.template.md"
THREAD = "thread-workshop"


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True, check=True).stdout.strip()


def _init(repo: Path, files: dict[str, str]) -> None:
    repo.mkdir(parents=True, exist_ok=True)
    _git(repo, "init", "-q", "-b", "main")
    _git(repo, "config", "user.email", "tests@example.com")
    _git(repo, "config", "user.name", "Tests")
    for name, text in files.items():
        (repo / name).parent.mkdir(parents=True, exist_ok=True)
        (repo / name).write_text(text, encoding="utf-8")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "base")


def open_shop(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """A core repository with one extension, a config repository with a template and a doctrine document;
    the suites and the rehearsal report success (each is tested on its own). Needs a fresh database."""
    project = tmp_path / "proj"
    _init(project, {"src/app.py": "x = 1\n", ".gitignore": "extensions/*\ndata/\n"})
    _init(
        project / "extensions" / "ext1",
        {"tool.py": "VALUE = 1\n", "tests/test_tool.py": "def test_tool():\n    pass\n"},
    )
    monkeypatch.setenv("TASQUE2_PROJECT_DIR", str(project))
    monkeypatch.setenv("TASQUE2_EXTENSIONS_DIR", str(project / "extensions"))
    reset_settings()
    data = get_settings().resolved_data_dir
    (data / "work-templates" / "cooking").mkdir(parents=True)
    (data / TEMPLATE).write_text("Reply.\n", encoding="utf-8")
    (data / "lanes.json").write_text(json.dumps({"schedules": {}}), encoding="utf-8")
    with session_scope() as session:
        MemoryService(session).upsert_canonical(
            namespace="cooking",
            canonical_key="cooking_direction",
            kind="doctrine",
            content="Cook simply.\n",
            pinned=True,
        )
    datarepo.init(data)
    suites: list[list[str]] = []
    monkeypatch.setattr(verify, "_run_command", lambda command, cwd: suites.append(command) or (True, "3 passed"))
    rehearsed: list[Any] = []

    def fake_rehearse(root: Path, plan: Any, **kwargs: Any) -> Rehearsal:
        rehearsed.append((plan, kwargs))
        outcome = Rehearsal()
        outcome.add("migrate", True)
        outcome.add("release-apply", True)
        return outcome

    monkeypatch.setattr("tasque2.ops.rehearse.rehearse", fake_rehearse)
    monkeypatch.setattr(pipeline, "denied_tools", lambda: ["mcp__tasque2__memory_save"])
    return {"project": project, "data": data, "suites": suites, "rehearsed": rehearsed}


def drive(session, script: dict[str, Callable[[WorkItem], dict[str, Any] | None]], steps: int = 60) -> None:
    """Advance every change until it waits or ends: function steps run, model runs answer from ``script``
    (None: the run crashed)."""
    service, runner = WorkflowService(session), WorkRunner(session)
    for _ in range(steps):
        service.tick_runs()
        claimed = WorkQueue(session).claim_next_ready_work(lease_owner="test")
        if claimed is None:
            break
        item = claimed.work_item
        if item.worker_kind.startswith("provider."):
            key = session.get(WorkflowNode, item.workflow_node_id).node_key
            produced = script[key](item)
            if produced is None:
                WorkQueue(session).fail_attempt(claimed.attempt.id, error_type="ProviderError", error_message="crash")
            else:
                WorkQueue(session).complete_attempt(claimed.attempt.id, summary="done", produces=produced)
        else:
            runner.execute(claimed)
    service.tick_runs()


def _plan(title: str, kind: str, touches: list[str], **extra: Any) -> Callable[[WorkItem], dict[str, Any]]:
    return lambda item: {
        "title": title,
        "kind": kind,
        "plan": f"1. {title}.",
        "touches": touches,
        "questions": [],
        "evidence": ["you asked"],
        **extra,
    }


def _commit(root: Path, name: str, text: str, message: str = "the change") -> None:
    path = root / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    _git(root, "add", "-A")
    _git(root, "commit", "-q", "-m", message)


def _template_build(item: WorkItem) -> dict[str, Any]:
    _commit(Path(item.context["cwd"]) / "data", TEMPLATE, "Reply, briefly.\n", "Shorter replies")
    return {"done": True, "changes": ["The cooking reply asks for shorter replies."]}


def _extension_build(item: WorkItem) -> dict[str, Any]:
    ext = Path(item.context["cwd"]) / "extensions" / "ext1"
    _commit(ext, "tool.py", "VALUE = 2\n")
    _commit(ext, "tests/test_tool.py", "def test_tool():\n    assert True\n")
    return {"done": True, "changes": ["The tool's value is 2."], "tests_added": ["tests/test_tool.py"]}


def _other_build(item: WorkItem) -> dict[str, Any]:
    ext = Path(item.context["cwd"]) / "extensions" / "ext1"
    _commit(ext, "other.py", "OTHER = 1\n")
    _commit(ext, "tests/test_other.py", "def test_other():\n    assert True\n")
    return {"done": True, "changes": ["The other value is set."], "tests_added": ["tests/test_other.py"]}


def _out(session, run_id: str, key: str) -> dict[str, Any]:
    return pipeline.node_output(session, run_id, key)


def _gate(session, run_id: str) -> WorkflowNode:
    return session.scalar(
        select(WorkflowNode).where(
            WorkflowNode.workflow_run_id == run_id, WorkflowNode.kind == "gate", WorkflowNode.status == "awaiting_input"
        )
    )
