from __future__ import annotations

import json
import subprocess
from datetime import timedelta
from pathlib import Path

import pytest
from sqlalchemy import select

from tasque2.config import reset_settings
from tasque2.daemon.restart import read_request
from tasque2.db import session_scope
from tasque2.models import FailedWork, WorkAttempt, WorkflowNode, WorkflowRun, WorkItem, utc_now
from tasque2.ops import repair
from tasque2.ops.faults import faults_path
from tasque2.ops.worktree import RepoWorktree, committed_files, create_worktrees, live_changes, remove_worktrees
from tasque2.workflows import WorkflowService


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


@pytest.fixture()
def project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "proj"
    _init(root, {"src/app.py": "x = 1\n", ".gitignore": "extensions/*\n"})
    _init(
        root / "extensions" / "ext1",
        {"__init__.py": "def register(registry):\n    pass\n", "tests/test_ok.py": "def test_ok():\n    pass\n"},
    )
    monkeypatch.setenv("TASQUE2_PROJECT_DIR", str(root))
    monkeypatch.setenv("TASQUE2_EXTENSIONS_DIR", str(root / "extensions"))
    reset_settings()
    return root


def _fault(signature: str, *, days_ago: int = 0, exc: str = "KeyError", message: str = "'missing'") -> dict:
    return {
        "at": (utc_now() - timedelta(days=days_ago)).isoformat(),
        "logger": "tasque2.digest",
        "signature": signature,
        "exc_type": exc,
        "message": message,
        "frame": {"file": "tasque2/digest.py", "function": "build"},
        "traceback": f"Traceback ... {exc}: {message}",
    }


def _ledger(*entries: dict) -> None:
    path = faults_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(entry) for entry in entries) + "\n", encoding="utf-8")


# ------------------------------------------------------------------------------ what needs repair


def test_recurring_code_faults_qualify_and_environmental_ones_do_not(fresh_db: Path) -> None:
    _ledger(
        _fault("aaaaaaaaaaaa"),
        _fault("aaaaaaaaaaaa", days_ago=1),
        _fault("bbbbbbbbbbbb"),  # once: not recurring
        _fault("cccccccccccc", exc="HTTPError", message="401 Unauthorized"),
        _fault("cccccccccccc", days_ago=1, exc="HTTPError", message="401 Unauthorized"),
    )
    with session_scope() as session:
        found = repair.candidates(session)
    assert [item["key"] for item in found] == ["fault:aaaaaaaaaaaa"]
    assert found[0]["traceback"].endswith("'missing'")


def test_dead_letters_qualify_unless_environmental_and_tried_ones_are_skipped(fresh_db: Path) -> None:
    with session_scope() as session:
        for error in ("KeyError: 'lane'", "usage limit reached"):
            work = WorkItem(title="Broken", task_instruction="x", worker_kind="function.echo", status="dead_letter")
            session.add(work)
            session.flush()
            attempt = WorkAttempt(work_item_id=work.id, attempt_number=1, status="failed", worker_kind="x")
            session.add(attempt)
            session.flush()
            session.add(
                FailedWork(work_item_id=work.id, attempt_id=attempt.id, status="unresolved", error_message=error)
            )
        session.flush()
        found = repair.candidates(session)
        assert [item["kind"] for item in found] == ["dead_letter"]
        assert "lane" in found[0]["message"]

        definition = repair.ensure_definition(session)
        session.add(
            WorkflowRun(
                workflow_definition_id=definition.id,
                name=repair.WORKFLOW_NAME,
                status="completed",
                input={"fault": {"key": found[0]["key"]}},
            )
        )
        session.flush()
        assert repair.candidates(session) == []


def test_the_gate_limits_repairs_to_one_open_and_two_a_week(fresh_db: Path) -> None:
    _ledger(_fault("aaaaaaaaaaaa"), _fault("aaaaaaaaaaaa", days_ago=1))
    with session_scope() as session:
        assert repair.repair_gate(session) is None
        definition = repair.ensure_definition(session)
        run = WorkflowRun(
            workflow_definition_id=definition.id,
            name=repair.WORKFLOW_NAME,
            status="awaiting_input",
            input={"fault": {"key": "fault:other"}},
        )
        session.add(run)
        session.flush()
        assert repair.repair_gate(session) == "a repair is still open"
        run.status = "completed"
        session.add(
            WorkflowRun(
                workflow_definition_id=definition.id,
                name=repair.WORKFLOW_NAME,
                status="canceled",
                input={"fault": {"key": "fault:third"}},
            )
        )
        session.flush()
        assert repair.repair_gate(session) == "2 repairs ran this week already"


def test_with_nothing_to_repair_the_gate_skips(fresh_db: Path) -> None:
    with session_scope() as session:
        assert repair.repair_gate(session) == "no code fault to repair"


def test_the_fix_run_may_read_but_not_write_push_or_switch_branches(isolated: Path) -> None:
    fix = repair.definition()["nodes"][0]
    denied = fix["runtime_contract"]["disallowed_tools"]
    assert "mcp__tasque2__memory_create" in denied and "mcp__tasque2__work_enqueue" in denied
    assert "mcp__tasque2__submit_worker_result" not in denied and "mcp__tasque2__memory_recall" not in denied
    assert "Bash(git push:*)" in denied and "PowerShell(git push:*)" in denied
    assert fix["runtime_contract"]["model_profile"] == "high" and fix["tolerate_failure"] is True
    gate = repair.definition()["nodes"][2]
    assert gate["choices"] == ["Merge and restart", "Discard"] and gate["card_from"] == "verify.card"


# ------------------------------------------------------------------------------ worktrees


def test_worktrees_cover_core_and_each_extension_and_notice_a_moving_live_checkout(project: Path) -> None:
    trees = create_worktrees("t1")
    try:
        names = {tree.name: tree for tree in trees}
        assert set(names) == {"core", "ext1"}
        assert Path(names["ext1"].path) == Path(names["core"].path) / "extensions" / "ext1"
        assert (Path(names["core"].path) / "src" / "app.py").is_file()
        assert live_changes(trees) == []
        (project / "src" / "app.py").write_text("x = 99\n", encoding="utf-8")
        assert live_changes(trees) == ["core"]
    finally:
        _git(project, "checkout", "--", "src/app.py")
        remove_worktrees(trees, delete_branches=True)
    assert not Path(trees[0].path).exists()
    assert not Path(trees[0].path).parent.exists()  # no empty repairs folder left behind
    assert _git(project, "branch", "--list", "repair/t1") == ""


# ------------------------------------------------------------------------------ verify, merge, discard


def _repair_run(session, trees: list[RepoWorktree], fix_output: dict) -> WorkflowRun:
    definition = repair.ensure_definition(session)
    run = WorkflowRun(
        workflow_definition_id=definition.id,
        name=repair.WORKFLOW_NAME,
        status="active",
        input={
            "repair_id": "t2",
            "fault": {
                "key": "fault:x",
                "exc_type": "KeyError",
                "count": 3,
                "days": 2,
                "frame": {"file": "tasque2/x.py", "function": "f"},
            },
            "worktrees": [tree.data() for tree in trees],
        },
    )
    session.add(run)
    session.flush()
    for key, output in (("fix", fix_output), ("approve", {})):
        session.add(
            WorkflowNode(
                workflow_run_id=run.id,
                node_key=key,
                kind="work",
                status="succeeded",
                definition={},
                input={},
                output=output,
            )
        )
    session.flush()
    return run


def _commit_fix(tree: RepoWorktree, name: str = "src/app.py", text: str = "x = 2\n") -> None:
    path = Path(tree.path) / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    _git(Path(tree.path), "add", "-A")
    _git(Path(tree.path), "commit", "-q", "-m", "Fix the missing key")


def test_verify_offers_a_clean_fix_with_its_card(fresh_db: Path, project: Path, monkeypatch) -> None:
    monkeypatch.setattr(repair, "_run_suite", lambda command, cwd: (True, "3 passed in 0.1s"))
    trees = create_worktrees("t2")
    try:
        _commit_fix(trees[0])
        with session_scope() as session:
            run = _repair_run(session, trees, {"fixed": True, "cause": "a missing key", "fix": "default it"})
            outcome = repair.verify(session, run)
        assert outcome["ok"] is True
        assert "Cause: a missing key" in outcome["card"] and "core: src/app.py" in outcome["card"]
        assert committed_files(trees[0]) == ["src/app.py"]
    finally:
        remove_worktrees(trees, delete_branches=True)


@pytest.mark.parametrize(
    ("fix_output", "file", "reason"),
    [
        ({"fixed": False, "why_not": "could not reproduce"}, None, "no fix: could not reproduce"),
        ({"fixed": True}, "data/state.json", "only Python code and tests may change"),
        ({"fixed": True}, "src/migrations/0009_add.py", "outside a repair"),
    ],
)
def test_verify_refuses_what_falls_outside_a_repair(fresh_db, project, monkeypatch, fix_output, file, reason) -> None:
    monkeypatch.setattr(repair, "_run_suite", lambda command, cwd: (True, "ok"))
    trees = create_worktrees("t3")
    try:
        if file:
            _commit_fix(trees[0], name=file, text="{}\n" if file.endswith(".json") else "x = 1\n")
        with session_scope() as session:
            outcome = repair.verify(session, _repair_run(session, trees, fix_output))
        assert outcome["ok"] is False
        assert any(reason in item for item in outcome["reasons"])
    finally:
        remove_worktrees(trees, delete_branches=True)


def test_a_failed_verify_ends_the_run_quietly_and_cleans_up(fresh_db: Path, project: Path) -> None:
    trees = create_worktrees("t4")
    with session_scope() as session:
        run = _repair_run(session, trees, {"fixed": False, "why_not": "flaky"})
        work = WorkItem(title="verify", task_instruction="x", worker_kind=repair.VERIFY_WORKER, workflow_run_id=run.id)
        session.add(work)
        session.flush()
        result = repair.verify_worker(work)
        assert result["produces"]["ok"] is False
        assert session.get(WorkflowRun, run.id).status == "canceled"
        assert all(not item.visible for item in session.scalars(select(WorkItem)).all())
    assert not Path(trees[0].path).exists()


def test_merge_queues_a_restart_that_switches_and_pushes_core(fresh_db: Path, project: Path) -> None:
    trees = create_worktrees("t5")
    _commit_fix(trees[0])
    with session_scope() as session:
        run = _repair_run(session, trees, {"fixed": True})
        approve = session.scalar(select(WorkflowNode).where(WorkflowNode.node_key == "approve"))
        approve.output = {"answer": "Merge and restart"}
        work = WorkItem(title="merge", task_instruction="x", worker_kind=repair.MERGE_WORKER, workflow_run_id=run.id)
        session.add(work)
        session.flush()
        result = repair.merge_worker(work)
    assert result["produces"]["merged"] is True
    request = read_request()
    assert request["window"] == "quiet"
    assert request["switch"] == [
        {"repo": str(project.resolve()), "ref": "repair/t5", "push": True, "remote": "origin", "branch": "main"}
    ]
    assert _git(project, "branch", "--list", "repair/t5")  # the branch stays for the switch
    assert not Path(trees[0].path).exists()


def test_discard_removes_the_worktrees_and_branches(fresh_db: Path, project: Path) -> None:
    trees = create_worktrees("t6")
    with session_scope() as session:
        run = _repair_run(session, trees, {"fixed": True})
        approve = session.scalar(select(WorkflowNode).where(WorkflowNode.node_key == "approve"))
        approve.output = {"answer": "Discard"}
        work = WorkItem(title="merge", task_instruction="x", worker_kind=repair.MERGE_WORKER, workflow_run_id=run.id)
        session.add(work)
        session.flush()
        assert repair.merge_worker(work)["produces"]["merged"] is False
    assert read_request() is None
    assert _git(project, "branch", "--list", "repair/t6") == ""


def test_launch_starts_the_workflow_in_the_worktree(fresh_db: Path, project: Path) -> None:
    _ledger(_fault("aaaaaaaaaaaa"), _fault("aaaaaaaaaaaa", days_ago=1))
    with session_scope() as session:
        work = WorkItem(
            title="repair", task_instruction="x", worker_kind=repair.LAUNCH_WORKER, discord_thread_id="thread-accounts"
        )
        session.add(work)
        session.flush()
        result = repair.launch_worker(work)
        run = session.get(WorkflowRun, result["produces"]["repair_run_id"])
        WorkflowService(session).tick_runs()
        fix = session.scalar(
            select(WorkflowNode).where(WorkflowNode.workflow_run_id == run.id, WorkflowNode.node_key == "fix")
        )
        fix_item = session.get(WorkItem, fix.work_item_id)
        assert run.discord_thread_id == "thread-accounts"
        assert fix_item.context["cwd"] == run.input["cwd"]
        assert fix_item.context["fault"]["key"] == "fault:aaaaaaaaaaaa"
        assert fix_item.context["memory_namespace"] == "global"
        assert fix_item.context["memory_canonical_keys"] == ["tasque_repair"]
        trees = [RepoWorktree.from_data(item) for item in run.input["worktrees"]]
    remove_worktrees(trees, delete_branches=True)
