from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest
from sqlalchemy import select
from typer.testing import CliRunner

from tasque2 import result_inbox
from tasque2.artifacts import ArtifactStore
from tasque2.cli import app
from tasque2.config import get_settings, reset_settings
from tasque2.db import session_scope
from tasque2.memory import MemoryService
from tasque2.models import Artifact, FailedWork, Memory, ProviderRun, WorkAttempt, WorkItem
from tasque2.providers import (
    ClaudeCodeProvider,
    CodexCliProvider,
    FakeProvider,
    ProviderRegistry,
    ProviderRequest,
    ProviderResponse,
    ProviderRuntime,
    SubprocessProvider,
    extract_session_id_from_stream,
    extract_structured_output,
    extract_text_from_stream,
)
from tasque2.queue import WorkQueue
from tasque2.repo import WorkRepository
from tasque2.runtime import WorkRunner


def test_fake_provider_success_records_run_and_artifacts(fresh_db: Path) -> None:
    captured = []
    registry = ProviderRegistry()
    registry.register(
        FakeProvider(
            capture_requests=captured,
            response=ProviderResponse(
                status="succeeded",
                summary="Fake success.",
                output_text='{"answer": 42}',
                structured_output={
                    "summary": "Fake success.",
                    "report": "Fake report.",
                    "produces": {"answer": 42},
                },
                stdout='{"answer": 42}',
                raw_stream='{"answer": 42}',
            ),
        )
    )

    with session_scope() as session:
        repo = WorkRepository(session)
        work = repo.create_work_item(
            title="Provider work",
            task_instruction="Return JSON.",
            worker_kind="provider.fake",
            runtime_contract={"expect_json": True, "model": "fake-model"},
            context={"cwd": str(fresh_db.parent)},
        )
        outcome = WorkRunner(
            session,
            provider_runtime=ProviderRuntime(registry=registry),
        ).run_next()

        assert outcome is not None
        assert outcome.status == "succeeded"
        assert captured[0].provider == "fake"
        assert captured[0].model == "fake-model"

        attempt = session.scalar(select(WorkAttempt).where(WorkAttempt.work_item_id == work.id))
        assert attempt is not None
        assert attempt.provider == "fake"

        provider_run = session.scalar(select(ProviderRun).where(ProviderRun.attempt_id == attempt.id))
        assert provider_run is not None
        assert provider_run.status == "succeeded"
        assert provider_run.stdout_artifact_id is not None
        assert provider_run.raw_stream_artifact_id is not None

        artifacts = session.scalars(select(Artifact).where(Artifact.attempt_id == attempt.id)).all()
        assert len(artifacts) == 5
        for artifact in artifacts:
            assert Path(artifact.local_path).exists()
        assert any("trace" in artifact.tags for artifact in artifacts)
        assert any("bundle" in artifact.tags for artifact in artifacts)
        assert attempt.report_artifact_id is not None
        assert attempt.produces["provider_run_bundle_artifact_id"]
        assert attempt.produces["ingested_memory_ids"]


def test_provider_records_declared_upload_paths_as_artifacts(fresh_db: Path, tmp_path: Path) -> None:
    output_file = tmp_path / "result.bin"
    output_file.write_bytes(b"\x00worker output")
    registry = ProviderRegistry()
    registry.register(
        FakeProvider(
            response=ProviderResponse(
                status="succeeded",
                summary="Produced a file.",
                structured_output={
                    "summary": "Produced a file.",
                    "report": "Produced a file.",
                    "produces": {"discord_upload_paths": [str(output_file)]},
                },
            )
        )
    )

    with session_scope() as session:
        work = WorkRepository(session).create_work_item(
            title="Provider file",
            task_instruction="Create a file.",
            worker_kind="provider.fake",
        )
        outcome = WorkRunner(
            session,
            provider_runtime=ProviderRuntime(registry=registry),
        ).run_next()

        assert outcome is not None
        attempt = session.scalar(select(WorkAttempt).where(WorkAttempt.work_item_id == work.id))
        assert attempt is not None
        artifact_id = attempt.produces["discord_upload_artifact_ids"][0]
        artifact = session.get(Artifact, artifact_id)
        assert artifact is not None
        assert "discord_upload" in artifact.tags
        assert Path(artifact.local_path).read_bytes() == b"\x00worker output"


def test_provider_prompt_includes_context_and_can_spawn_child_work(fresh_db: Path) -> None:
    captured: list[ProviderRequest] = []
    registry = ProviderRegistry()
    registry.register(
        FakeProvider(
            capture_requests=captured,
            response=ProviderResponse(
                status="succeeded",
                summary="Spawned.",
                structured_output={
                    "summary": "Spawned follow-up work.",
                    "report": "Spawned follow-up work.",
                    "produces": {
                        "child_work": [
                            {
                                "title": "Follow-up",
                                "task_instruction": "Run the follow-up.",
                                "worker_kind": "function.echo",
                            }
                        ]
                    },
                },
            ),
        )
    )

    with session_scope() as session:
        MemoryService(session).create_memory(
            namespace="global",
            kind="preference",
            content="Prefer concise status reports.",
            tags=["style"],
        )
        work = WorkRepository(session).create_work_item(
            title="Context work",
            task_instruction="Use concise status reports.",
            worker_kind="provider.fake",
        )
        outcome = WorkRunner(
            session,
            provider_runtime=ProviderRuntime(registry=registry),
        ).run_next()

        assert outcome is not None
        assert "## Context Packet JSON" in captured[0].prompt
        assert "## Standard Work Template Shape" in captured[0].prompt
        assert "## Template Interpretation" in captured[0].prompt
        assert "reusable work template" in captured[0].prompt
        assert "Assemble one or more complete, self-contained native-subagent prompts" in captured[0].prompt
        assert "## Work Template" in captured[0].prompt
        assert "## Coordinator Process" in captured[0].prompt
        assert "Tasque MCP tools" in captured[0].prompt
        assert "submit_worker_result" in captured[0].prompt
        assert "Construct the needed native-subagent prompt" in captured[0].prompt
        assert "straightforward Tasque routing or operations" in captured[0].prompt
        assert "Delegate only when there is actual domain reasoning" in captured[0].prompt
        assert "Prefer concise status reports" in captured[0].prompt
        children = session.scalars(
            select(WorkItem).where(WorkItem.source_kind == "provider_child_work")
        ).all()
        assert len(children) == 1
        assert children[0].context["parent_work_item_id"] == work.id


def test_provider_prompt_context_uses_default_memory_budget(fresh_db: Path) -> None:
    captured: list[ProviderRequest] = []
    registry = ProviderRegistry()
    registry.register(FakeProvider(capture_requests=captured))

    with session_scope() as session:
        memory = MemoryService(session)
        for index in range(40):
            memory.create_memory(
                namespace="global",
                kind="note",
                content=f"Budgeted memory {index} " + ("detail " * 500),
            )
        work = WorkRepository(session).create_work_item(
            title="Budgeted memory",
            task_instruction="Use budgeted memory details.",
            worker_kind="provider.fake",
        )

        WorkRunner(
            session,
            provider_runtime=ProviderRuntime(registry=registry),
        ).run_next()

        packet = captured[0].context["tasque_context_packet"]
        assert len(packet["memories"]) == 24
        assert packet["context_budget"]["limits"]["memories"] == 24
        assert any(memory["content_compacted"] for memory in packet["memories"])
        assert len(captured[0].prompt) < 120_000
        assert session.get(WorkItem, work.id).status == "succeeded"


def test_provider_prompt_uses_explicit_memory_queries_and_canonical_state(fresh_db: Path) -> None:
    captured: list[ProviderRequest] = []
    registry = ProviderRegistry()
    registry.register(FakeProvider(capture_requests=captured))

    with session_scope() as session:
        memory = MemoryService(session)
        memory.upsert_canonical(
            namespace="health",
            canonical_key="current_workout_state",
            kind="summary",
            content="Current workout state: last confirmed pull session.",
            tags=["workout", "state"],
        )
        memory.create_memory(
            namespace="health",
            kind="working",
            content="Completed workout actual loads: bench 95x10 RPE 8.",
            tags=["workout", "completion"],
        )
        WorkRepository(session).create_work_item(
            title="Workout generator",
            task_instruction="Generate today's workout.",
            worker_kind="provider.fake",
            context={
                "memory_namespace": "health",
                "memory_canonical_keys": ["current_workout_state"],
                "memory_queries": [
                    {
                        "query": "completed workout actual loads",
                        "tags": ["workout", "completion"],
                    }
                ],
            },
        )
        outcome = WorkRunner(
            session,
            provider_runtime=ProviderRuntime(registry=registry),
        ).run_next()

        assert outcome is not None
        assert "Current workout state: last confirmed pull session." in captured[0].prompt
        assert "Completed workout actual loads: bench 95x10 RPE 8." in captured[0].prompt


def test_provider_prompt_includes_parent_work_for_reply_processors(fresh_db: Path) -> None:
    captured: list[ProviderRequest] = []
    registry = ProviderRegistry()
    registry.register(FakeProvider(capture_requests=captured))

    with session_scope() as session:
        parent = WorkRepository(session).create_work_item(
            title="Workout generator",
            task_instruction="Generate today's workout.",
            worker_kind="provider.fake",
        )
        claimed = WorkQueue(session).claim_next_ready_work(lease_owner="test")
        assert claimed is not None
        report = ArtifactStore().write_text(
            session,
            kind="worker_report",
            title="Workout report",
            content="**Focus**: push\nBench press - 3x10 @ 95 lb",
            work_item_id=parent.id,
            attempt_id=claimed.attempt.id,
            tags=["provider", "report"],
        )
        WorkQueue(session).complete_attempt(
            claimed.attempt.id,
            summary="Prescribed push workout.",
            produces={"focus": "push"},
            report_artifact_id=report.id,
        )
        WorkRepository(session).create_work_item(
            title="Process workout reply",
            task_instruction="Process the user's workout reply.",
            worker_kind="provider.fake",
            context={
                "parent_work_item_id": parent.id,
                "source_reply": {
                    "discord_message_id": "reply-1",
                    "content": "I did the workout and bench was 95x10 RPE 8.",
                    "parent_work_item_id": parent.id,
                    "parent_report_artifact_id": report.id,
                },
            },
        )

        outcome = WorkRunner(
            session,
            provider_runtime=ProviderRuntime(registry=registry),
        ).run_next()

        assert outcome is not None
        assert '"parent_work"' in captured[0].prompt
        assert parent.id in captured[0].prompt
        assert report.id in captured[0].prompt
        assert "Prescribed push workout." in captured[0].prompt
        assert "When a parent report artifact is present, read it" in captured[0].prompt


def test_provider_memory_writes_create_and_upsert_memories(fresh_db: Path) -> None:
    registry = ProviderRegistry()
    registry.register(
        FakeProvider(
            response=ProviderResponse(
                status="succeeded",
                summary="Updated memory.",
                structured_output={
                    "summary": "Updated memory.",
                    "report": "Updated memory.",
                    "produces": {
                        "memory_writes": [
                            {
                                "operation": "create",
                                "namespace": "health",
                                "kind": "working",
                                "content": "Prescribed push workout.",
                                "tags": ["workout", "prescription"],
                                "ttl_days": 14,
                            },
                            {
                                "operation": "upsert_canonical",
                                "namespace": "health",
                                "kind": "summary",
                                "canonical_key": "current_workout_state",
                                "content": "Current workout state: prescribed push, unconfirmed.",
                                "tags": ["workout", "state"],
                            },
                        ]
                    },
                },
            )
        )
    )

    with session_scope() as session:
        old = MemoryService(session).upsert_canonical(
            namespace="health",
            canonical_key="current_workout_state",
            kind="summary",
            content="old state",
        )
        work = WorkRepository(session).create_work_item(
            title="Workout generator",
            task_instruction="Generate workout.",
            worker_kind="provider.fake",
        )
        outcome = WorkRunner(
            session,
            provider_runtime=ProviderRuntime(registry=registry),
        ).run_next()

        assert outcome is not None
        attempt = session.scalar(select(WorkAttempt).where(WorkAttempt.work_item_id == work.id))
        assert attempt is not None
        assert len(attempt.produces["memory_ids"]) == 2

        prescription = session.scalar(select(Memory).where(Memory.content == "Prescribed push workout."))
        assert prescription is not None
        assert prescription.work_item_id == work.id
        assert prescription.ttl_days == 14
        current = MemoryService(session).get_canonical(
            namespace="health",
            canonical_key="current_workout_state",
        )
        assert current is not None
        assert current.content == "Current workout state: prescribed push, unconfirmed."
        assert session.get(Memory, old.id).archived_at is not None


def test_provider_default_worker_kind_uses_env_provider(
    fresh_db: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("TASQUE2_DEFAULT_PROVIDER", "fake")
    monkeypatch.setenv("TASQUE2_ALLOW_TEST_PROVIDERS", "true")
    reset_settings()
    captured: list[ProviderRequest] = []
    registry = ProviderRegistry()
    registry.register(FakeProvider(capture_requests=captured))

    try:
        with session_scope() as session:
            work = WorkRepository(session).create_work_item(
                title="Default provider work",
                task_instruction="Run with the environment configured provider.",
                worker_kind="provider.default",
            )
            outcome = WorkRunner(
                session,
                provider_runtime=ProviderRuntime(registry=registry),
            ).run_next()

            assert outcome is not None
            assert captured[0].provider == "fake"
            attempt = session.scalar(select(WorkAttempt).where(WorkAttempt.work_item_id == work.id))
            assert attempt is not None
            assert attempt.provider == "fake"
    finally:
        reset_settings()


def test_model_profile_resolves_native_worker_while_orchestrator_is_high(
    fresh_db: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("TASQUE2_CODEX_MODEL_HIGH", "codex-high-test")
    monkeypatch.setenv("TASQUE2_CODEX_MODEL_MEDIUM", "codex-medium-test")
    reset_settings()
    captured: list[ProviderRequest] = []
    adapter = FakeProvider(capture_requests=captured)
    adapter.name = "codex"
    registry = ProviderRegistry()
    registry.register(adapter)

    try:
        with session_scope() as session:
            WorkRepository(session).create_work_item(
                title="Profiled provider work",
                task_instruction="Use the medium profile.",
                worker_kind="provider.codex",
                runtime_contract={"model_profile": "medium"},
            )
            outcome = WorkRunner(
                session,
                provider_runtime=ProviderRuntime(registry=registry),
            ).run_next()

            assert outcome is not None
            assert captured[0].model == "codex-high-test"
            assert '"native_worker"' in captured[0].prompt
            assert '"model": "codex-medium-test"' in captured[0].prompt
            assert '"model_profile": "medium"' in captured[0].prompt
    finally:
        reset_settings()


def test_tier_contract_aliases_to_model_profile(
    fresh_db: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("TASQUE2_CODEX_MODEL_HIGH", "codex-high-test")
    monkeypatch.setenv("TASQUE2_CODEX_MODEL_MEDIUM", "codex-medium-test")
    reset_settings()
    captured: list[ProviderRequest] = []
    adapter = FakeProvider(capture_requests=captured)
    adapter.name = "codex"
    registry = ProviderRegistry()
    registry.register(adapter)

    try:
        with session_scope() as session:
            WorkRepository(session).create_work_item(
                title="Tier provider work",
                task_instruction="Use an old tier contract.",
                worker_kind="provider.codex",
                runtime_contract={"tier": "medium"},
            )
            outcome = WorkRunner(
                session,
                provider_runtime=ProviderRuntime(registry=registry),
            ).run_next()

            assert outcome is not None
            assert captured[0].model == "codex-high-test"
            assert '"model": "codex-medium-test"' in captured[0].prompt
            assert '"model_profile": "medium"' in captured[0].prompt
    finally:
        reset_settings()


def test_semantic_model_profile_hints_are_rejected(
    fresh_db: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("TASQUE2_CODEX_MODEL_HIGH", "codex-high-test")
    reset_settings()
    captured: list[ProviderRequest] = []
    adapter = FakeProvider(capture_requests=captured)
    adapter.name = "codex"
    registry = ProviderRegistry()
    registry.register(adapter)

    try:
        with session_scope() as session:
            WorkRepository(session).create_work_item(
                title="Hinted provider work",
                task_instruction="Use a fast native worker.",
                worker_kind="provider.codex",
                runtime_contract={"model_profile": "hint:fast"},
            )
            outcome = WorkRunner(
                session,
                provider_runtime=ProviderRuntime(registry=registry),
            ).run_next()

            assert outcome is not None
            assert outcome.status == "dead_letter"
            assert captured == []
            failed = session.scalar(select(FailedWork).where(FailedWork.work_item_id == outcome.work_item_id))
            assert failed is not None
            assert failed.error_type == "ValueError"
            assert failed.error_message == "model_profile must be one of: high, low, medium."
    finally:
        reset_settings()


def test_explicit_model_sets_native_worker_while_orchestrator_is_high(
    fresh_db: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("TASQUE2_CODEX_MODEL_HIGH", "codex-high-test")
    reset_settings()
    captured: list[ProviderRequest] = []
    adapter = FakeProvider(capture_requests=captured)
    adapter.name = "codex"
    registry = ProviderRegistry()
    registry.register(adapter)

    try:
        with session_scope() as session:
            WorkRepository(session).create_work_item(
                title="Explicit model work",
                task_instruction="Use the explicit model.",
                worker_kind="provider.codex",
                runtime_contract={"model": "codex-explicit-test", "model_profile": "high"},
            )
            outcome = WorkRunner(
                session,
                provider_runtime=ProviderRuntime(registry=registry),
            ).run_next()

            assert outcome is not None
            assert captured[0].model == "codex-high-test"
            assert '"native_worker"' in captured[0].prompt
            assert '"model": "codex-explicit-test"' in captured[0].prompt
    finally:
        reset_settings()


def test_orchestrator_stays_high_even_if_contract_requests_low(
    fresh_db: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("TASQUE2_CODEX_MODEL_LOW", "codex-low-test")
    monkeypatch.setenv("TASQUE2_CODEX_MODEL_MEDIUM", "codex-medium-test")
    monkeypatch.setenv("TASQUE2_CODEX_MODEL_HIGH", "codex-high-test")
    reset_settings()
    captured: list[ProviderRequest] = []
    adapter = FakeProvider(capture_requests=captured)
    adapter.name = "codex"
    registry = ProviderRegistry()
    registry.register(adapter)

    try:
        with session_scope() as session:
            WorkRepository(session).create_work_item(
                title="Split model work",
                task_instruction="Run the domain worker medium.",
                worker_kind="provider.codex",
                runtime_contract={
                    "orchestrator_model_profile": "low",
                    "model_profile": "medium",
                },
            )
            outcome = WorkRunner(
                session,
                provider_runtime=ProviderRuntime(registry=registry),
            ).run_next()

            assert outcome is not None
            assert captured[0].model == "codex-high-test"
            assert '"native_worker"' in captured[0].prompt
            assert '"model": "codex-medium-test"' in captured[0].prompt
            assert '"model_profile": "medium"' in captured[0].prompt
    finally:
        reset_settings()


def test_default_provider_rejects_test_provider_without_test_flag(monkeypatch) -> None:
    monkeypatch.setenv("TASQUE2_DEFAULT_PROVIDER", "fake")
    monkeypatch.delenv("TASQUE2_ALLOW_TEST_PROVIDERS", raising=False)
    reset_settings()

    try:
        with pytest.raises(ValueError, match="codex or claude"):
            _ = get_settings().default_provider_name
    finally:
        reset_settings()


def test_fake_provider_failure_dead_letters_work(fresh_db: Path) -> None:
    registry = ProviderRegistry()
    registry.register(
        FakeProvider(
            response=ProviderResponse(
                status="failed",
                summary="Fake failure.",
                stderr="bad things happened",
                raw_stream="bad things happened",
                exit_code=9,
            )
        )
    )

    with session_scope() as session:
        repo = WorkRepository(session)
        work = repo.create_work_item(
            title="Provider failure",
            task_instruction="Fail.",
            worker_kind="provider.fake",
        )
        outcome = WorkRunner(
            session,
            provider_runtime=ProviderRuntime(registry=registry),
        ).run_next()

        assert outcome is not None
        assert outcome.status == "dead_letter"
        assert session.get(WorkItem, work.id).status == "dead_letter"

        failed = session.scalar(select(FailedWork).where(FailedWork.work_item_id == work.id))
        assert failed is not None
        assert failed.error_type == "ProviderExecutionError"
        assert failed.error_message == "Fake failure."

        provider_run = session.scalar(select(ProviderRun))
        assert provider_run is not None
        assert provider_run.status == "failed"
        assert provider_run.stderr_artifact_id is not None


def test_provider_missing_submit_result_dead_letters_work(fresh_db: Path) -> None:
    registry = ProviderRegistry()
    registry.register(
        FakeProvider(
            response=ProviderResponse(
                status="succeeded",
                summary="No submitted result.",
                output_text="plain text",
                stdout="plain text",
            ),
            deposit_structured_result=False,
        )
    )

    with session_scope() as session:
        repo = WorkRepository(session)
        work = repo.create_work_item(
            title="Need JSON",
            task_instruction="Submit a result.",
            worker_kind="provider.fake",
        )
        outcome = WorkRunner(
            session,
            provider_runtime=ProviderRuntime(registry=registry),
        ).run_next()

        assert outcome is not None
        assert outcome.status == "dead_letter"
        assert session.get(WorkItem, work.id).status == "dead_letter"

        failed = session.scalar(select(FailedWork).where(FailedWork.work_item_id == work.id))
        assert failed is not None
        assert "did not call submit_worker_result" in failed.error_message


def test_provider_blocked_result_with_error_completes_work(fresh_db: Path) -> None:
    registry = ProviderRegistry()
    registry.register(
        FakeProvider(
            response=ProviderResponse(
                status="succeeded",
                summary="Blocked.",
                structured_output={
                    "status": "blocked",
                    "summary": "Needs auth.",
                    "report": "Needs auth.",
                    "error": "login_required",
                    "produces": {"outcome": "blocked"},
                },
            )
        )
    )

    with session_scope() as session:
        work = WorkRepository(session).create_work_item(
            title="Blocked auth",
            task_instruction="Return blocked.",
            worker_kind="provider.fake",
        )
        outcome = WorkRunner(
            session,
            provider_runtime=ProviderRuntime(registry=registry),
        ).run_next()

        assert outcome is not None
        assert outcome.status == "succeeded"
        assert session.get(WorkItem, work.id).status == "succeeded"

        attempt = session.scalar(select(WorkAttempt).where(WorkAttempt.work_item_id == work.id))
        assert attempt is not None
        assert attempt.produces["completion_signal"] == "blocked"
        assert attempt.produces["blocker"] == "login_required"
        assert session.scalar(select(FailedWork).where(FailedWork.work_item_id == work.id)) is None


def test_provider_missing_result_reports_failed_submit_tool(fresh_db: Path) -> None:
    stdout = "\n".join(
        [
            '{"type":"turn.started"}',
            (
                '{"type":"item.completed","item":{"type":"mcp_tool_call",'
                '"tool":"submit_worker_result","status":"failed",'
                '"error":{"message":"user cancelled MCP tool call"}}}'
            ),
        ]
    )
    registry = ProviderRegistry()
    registry.register(
        FakeProvider(
            response=ProviderResponse(
                status="succeeded",
                summary="Attempted submit.",
                output_text="attempted submit",
                stdout=stdout,
                raw_stream=stdout,
            ),
            deposit_structured_result=False,
        )
    )

    with session_scope() as session:
        work = WorkRepository(session).create_work_item(
            title="Submit canceled",
            task_instruction="Submit a result.",
            worker_kind="provider.fake",
        )
        outcome = WorkRunner(
            session,
            provider_runtime=ProviderRuntime(registry=registry),
        ).run_next()

        assert outcome is not None
        assert outcome.status == "dead_letter"
        assert session.get(WorkItem, work.id).status == "dead_letter"

        failed = session.scalar(select(FailedWork).where(FailedWork.work_item_id == work.id))
        assert failed is not None
        assert "attempted submit_worker_result" in failed.error_message
        assert "user cancelled MCP tool call" in failed.error_message


def test_subprocess_provider_does_not_pass_timeout_to_runner() -> None:
    captured: dict[str, object] = {}

    def runner(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return subprocess.CompletedProcess(
            args=args[0],
            returncode=0,
            stdout="done\n",
            stderr="",
        )

    response = SubprocessProvider(runner=runner).run(
        ProviderRequest(
            provider="subprocess",
            prompt="Run without timeout.",
            argv=[sys.executable, "-c", "print('done')"],
        )
    )

    assert response.status == "succeeded"
    assert "timeout" not in captured["kwargs"]
    assert captured["kwargs"]["encoding"] == "utf-8"
    assert captured["kwargs"]["errors"] == "replace"


def test_subprocess_provider_tolerates_missing_process_streams() -> None:
    def runner(*args, **_kwargs):
        return subprocess.CompletedProcess(
            args=args[0],
            returncode=0,
            stdout=None,
            stderr=None,
        )

    response = SubprocessProvider(runner=runner).run(
        ProviderRequest(
            provider="subprocess",
            prompt="Run with missing streams.",
            argv=[sys.executable, "-c", "print('done')"],
        )
    )

    assert response.status == "succeeded"
    assert response.stdout == ""
    assert response.stderr == ""
    assert response.raw_stream == ""


def test_subprocess_provider_reports_windows_command_line_limit() -> None:
    class WindowsCommandLineTooLong(FileNotFoundError):
        winerror = 206

    def runner(*_args, **_kwargs):
        raise WindowsCommandLineTooLong("[WinError 206] The filename or extension is too long")

    response = SubprocessProvider(runner=runner).run(
        ProviderRequest(
            provider="subprocess",
            prompt="long",
            argv=["codex", "exec", "long-prompt"],
        )
    )

    assert response.status == "failed"
    assert response.summary == "Provider command line too long: codex"


def test_subprocess_provider_terminates_process_tree_after_result_deposit(fresh_db: Path) -> None:
    token = result_inbox.mint_token()
    script = (
        "from tasque2 import result_inbox; "
        f"result_inbox.deposit(result_token={token!r}, agent_kind='worker', payload={{"
        "'status':'succeeded','summary':'done','report':'done','produces':{}}); "
        "import subprocess, sys, time; "
        "subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)']); "
        "time.sleep(60)"
    )

    response = SubprocessProvider().run(
        ProviderRequest(
            provider="subprocess",
            prompt="",
            argv=[sys.executable, "-c", script],
            context={"result_token": token},
            env={"PYTHONPATH": str(Path.cwd() / "src")},
        )
    )

    assert response.status == "succeeded"
    assert response.summary == "Provider submitted result; terminated remaining subprocess tree."
    assert response.usage["terminated_after_result"] is True
    assert result_inbox.peek(token, agent_kind="worker") is True


def test_provider_stream_helpers_parse_claude_style_jsonl() -> None:
    stream = "\n".join(
        [
            '{"type":"assistant","message":{"content":[{"type":"text","text":"working"}]}}',
            (
                '{"type":"result","result":"{\\"ok\\": true, \\"message\\": \\"done\\"}",'
                '"session_id":"s-1","usage":{"input_tokens":3}}'
            ),
        ]
    )

    assert extract_text_from_stream(stream) == '{"ok": true, "message": "done"}'
    assert extract_session_id_from_stream(stream) == "s-1"
    assert extract_structured_output(stream) == {"ok": True, "message": "done"}


def test_codex_provider_builds_schema_arg_and_parses_jsonl(tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    def runner(argv, **kwargs):
        captured["argv"] = argv
        captured["input"] = kwargs["input"]
        schema_path = Path(argv[argv.index("--output-schema") + 1])
        captured["schema_path"] = schema_path
        captured["schema_exists_at_run"] = schema_path.exists()
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout='{"type":"result","result":"{\\"ok\\":true,\\"provider\\":\\"codex\\"}","session_id":"codex-session"}\n',
            stderr="",
        )

    provider = CodexCliProvider(runner=runner)
    response = provider.run(
        ProviderRequest(
            provider="codex",
            prompt="Return JSON.",
            cwd=str(tmp_path),
            output_schema={"type": "object"},
            expect_json=True,
        )
    )

    argv = captured["argv"]
    assert "--dangerously-bypass-approvals-and-sandbox" in argv
    assert "--output-schema" in argv
    assert "-c" in argv
    assert any("mcp_servers.tasque2.command=" in item for item in argv)
    assert any("mcp_servers.tasque2.args=" in item for item in argv)
    assert any("mcp_servers.tasque2.tool_timeout_sec=86400" in item for item in argv)
    assert argv[-1] == "-"
    assert "Return JSON." not in argv
    assert captured["input"] == "Return JSON."
    assert captured["schema_exists_at_run"] is True
    assert not captured["schema_path"].exists()
    assert response.status == "succeeded"
    assert response.provider_session_id == "codex-session"
    assert response.structured_output == {"ok": True, "provider": "codex"}


def test_codex_provider_failure_summary_uses_stream_error_message() -> None:
    def runner(argv, **kwargs):
        stream = "\n".join(
            [
                '{"type":"thread.started","thread_id":"t-1"}',
                (
                    "{\"type\":\"error\",\"message\":\"Codex ran out of room in the model's "
                    'context window. Start a new thread."}'
                ),
                (
                    "{\"type\":\"turn.failed\",\"error\":{\"message\":\"Codex ran out of room "
                    'in the model\'s context window. Start a new thread."}}'
                ),
            ]
        )
        return subprocess.CompletedProcess(argv, 1, stdout=stream, stderr="")

    response = CodexCliProvider(runner=runner).run(
        ProviderRequest(provider="codex", prompt="too much context")
    )

    assert response.status == "failed"
    assert response.summary == "Codex ran out of room in the model's context window. Start a new thread."


def test_claude_provider_passes_json_schema_and_parses_result(tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    def runner(argv, **kwargs):
        captured["argv"] = argv
        captured["env"] = kwargs["env"]
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout='{"type":"result","result":"{\\"ok\\":true,\\"provider\\":\\"claude\\"}","session_id":"claude-session","total_cost_usd":0.01}\n',
            stderr="",
        )

    provider = ClaudeCodeProvider(runner=runner)
    response = provider.run(
        ProviderRequest(
            provider="claude",
            prompt="Return JSON.",
            cwd=str(tmp_path),
            output_schema={"type": "object"},
            expect_json=True,
        )
    )

    argv = captured["argv"]
    assert "--permission-mode" in argv
    assert argv[argv.index("--permission-mode") + 1] == "bypassPermissions"
    assert "--json-schema" in argv
    assert "--mcp-config" in argv
    mcp_config = json.loads(argv[argv.index("--mcp-config") + 1])
    assert mcp_config["mcpServers"]["tasque2"]["args"] == ["-m", "tasque2.mcp"]
    assert captured["env"]["MCP_TIMEOUT"] == "86400000"
    assert captured["env"]["MCP_TOOL_TIMEOUT"] == "100000000"
    assert response.status == "succeeded"
    assert response.provider_session_id == "claude-session"
    assert response.usage["total_cost_usd"] == 0.01
    assert response.structured_output == {"ok": True, "provider": "claude"}


def test_provider_smoke_cli_runs_subprocess_when_test_providers_are_enabled(
    fresh_db: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("TASQUE2_ALLOW_TEST_PROVIDERS", "true")
    reset_settings()
    try:
        result = CliRunner().invoke(
            app,
            ["provider-smoke", "subprocess"],
        )

        assert result.exit_code == 0
        assert "succeeded" in result.output
    finally:
        reset_settings()
