from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

from sqlalchemy.orm import Session

from tasque2 import result_inbox
from tasque2.artifacts import ArtifactStore
from tasque2.compression import compress_text
from tasque2.config import DEFAULT_MODEL_PROVIDERS, get_settings
from tasque2.memory import MemoryService
from tasque2.memory_ingest import MemoryIngestService
from tasque2.models import ProviderRun, WorkAttempt, WorkItem, utc_now
from tasque2.repo import WorkRepository
from tasque2.worker_context import (
    WorkerContextBuilder,
    render_provider_prompt,
)

if TYPE_CHECKING:
    from tasque2.runtime import WorkerResult

DEFAULT_PROVIDER_WORKER_KINDS = {"provider.default", "provider.env"}
CODEX_MCP_TOOL_TIMEOUT_SECONDS = 24 * 60 * 60
CLAUDE_MCP_STARTUP_TIMEOUT_MS = 24 * 60 * 60 * 1000
CLAUDE_MCP_TOOL_TIMEOUT_MS = 100_000_000
SUBPROCESS_RESULT_POLL_SECONDS = 0.5
SUBPROCESS_TERMINATION_GRACE_SECONDS = 5.0
DEFAULT_PROVIDER_CONTEXT_LIMITS: dict[str, int | None] = {
    "memories": 24,
    "artifacts": 48,
    "events": 48,
    "workflow_nodes": None,
}
PROVIDER_CONTEXT_LIMIT_KEYS = frozenset(DEFAULT_PROVIDER_CONTEXT_LIMITS)


@dataclass(frozen=True)
class ProviderRequest:
    provider: str
    prompt: str
    cwd: str | None = None
    model: str | None = None
    argv: list[str] = field(default_factory=list)
    env: dict[str, str] = field(default_factory=dict)
    output_schema: dict[str, Any] | None = None
    expect_json: bool = False
    context: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ProviderResponse:
    status: str
    summary: str
    output_text: str = ""
    structured_output: dict[str, Any] | None = None
    stdout: str = ""
    stderr: str = ""
    raw_stream: str = ""
    provider_session_id: str | None = None
    usage: dict[str, Any] = field(default_factory=dict)
    exit_code: int | None = None


@dataclass(frozen=True)
class ProviderModelRouting:
    orchestrator_model: str | None = None
    orchestrator_profile: str | None = None
    native_worker_model: str | None = None
    native_worker_profile: str | None = None


class ProviderAdapter(Protocol):
    name: str

    def run(self, request: ProviderRequest) -> ProviderResponse:
        ...


class ProviderExecutionError(RuntimeError):
    pass


class ProviderRegistry:
    def __init__(self) -> None:
        self._providers: dict[str, ProviderAdapter] = {}

    def register(self, adapter: ProviderAdapter) -> None:
        self._providers[adapter.name] = adapter

    def get(self, name: str) -> ProviderAdapter:
        adapter = self._providers.get(name)
        if adapter is None:
            raise ProviderExecutionError(f"No provider adapter registered for {name!r}.")
        return adapter


class FakeProvider:
    name = "fake"

    def __init__(
        self,
        *,
        response: ProviderResponse | None = None,
        capture_requests: list[ProviderRequest] | None = None,
        deposit_structured_result: bool = True,
    ) -> None:
        self.response = response or ProviderResponse(
            status="succeeded",
            summary="Fake provider completed.",
            output_text="Fake provider completed.",
            structured_output={
                "summary": "Fake provider completed.",
                "report": "Fake provider completed.",
                "produces": {"ok": True},
            },
            stdout="Fake provider completed.",
            raw_stream="Fake provider completed.",
        )
        self.capture_requests = capture_requests
        self.deposit_structured_result = deposit_structured_result

    def run(self, request: ProviderRequest) -> ProviderResponse:
        if self.capture_requests is not None:
            self.capture_requests.append(request)
        result_token = request.context.get("result_token")
        if (
            self.deposit_structured_result
            and isinstance(result_token, str)
            and self.response.structured_output is not None
        ):
            result_inbox.deposit(
                result_token=result_token,
                agent_kind="worker",
                payload=self.response.structured_output,
            )
        return self.response


class SubprocessProvider:
    name = "subprocess"

    def __init__(self, *, runner: Callable[..., subprocess.CompletedProcess[Any]] | None = None) -> None:
        self._runner = runner

    def run(self, request: ProviderRequest) -> ProviderResponse:
        if not request.argv:
            return ProviderResponse(
                status="failed",
                summary="No subprocess argv was provided.",
                stderr="runtime_contract.argv is required for provider.subprocess.",
                exit_code=2,
            )

        env = os.environ.copy()
        env.update(request.env)
        if self._runner is not None:
            return self._run_with_runner(request, env)
        return self._run_live(request, env)

    def _run_with_runner(self, request: ProviderRequest, env: dict[str, str]) -> ProviderResponse:
        try:
            completed = self._runner(  # type: ignore[misc]
                request.argv,
                input=request.prompt,
                capture_output=True,
                cwd=request.cwd,
                env=env,
                text=True,
                encoding="utf-8",
                errors="replace",
                check=False,
            )
        except OSError as exc:
            return _provider_launch_error_response(request, exc)

        return _completed_process_response(completed)

    def _run_live(self, request: ProviderRequest, env: dict[str, str]) -> ProviderResponse:
        popen_kwargs: dict[str, Any] = {
            "stdin": subprocess.PIPE,
            "stdout": subprocess.PIPE,
            "stderr": subprocess.PIPE,
            "cwd": request.cwd,
            "env": env,
            "text": True,
            "encoding": "utf-8",
            "errors": "replace",
        }
        if os.name == "nt":
            popen_kwargs["creationflags"] = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        else:
            popen_kwargs["start_new_session"] = True

        try:
            process = subprocess.Popen(request.argv, **popen_kwargs)
        except OSError as exc:
            return _provider_launch_error_response(request, exc)

        stdout_parts: list[str] = []
        stderr_parts: list[str] = []
        readers = [
            _start_reader_thread(process.stdout, stdout_parts),
            _start_reader_thread(process.stderr, stderr_parts),
        ]
        stdin_writer = _start_stdin_writer_thread(process, request.prompt)

        terminated_after_result = False
        result_token = request.context.get("result_token")
        while process.poll() is None:
            if isinstance(result_token, str) and result_inbox.peek(result_token, agent_kind="worker"):
                terminated_after_result = True
                _terminate_process_tree(process, force=True)
                break
            time.sleep(SUBPROCESS_RESULT_POLL_SECONDS)

        returncode = _wait_for_process_exit(process)
        for reader in readers:
            if reader is not None:
                reader.join(timeout=SUBPROCESS_TERMINATION_GRACE_SECONDS)
        if stdin_writer is not None:
            stdin_writer.join(timeout=1.0)

        stdout = _coerce_process_text("".join(stdout_parts))
        stderr = _coerce_process_text("".join(stderr_parts))
        if terminated_after_result:
            note = "[tasque] Provider submitted result; terminated remaining subprocess tree."
            stderr = f"{stderr.rstrip()}\n{note}\n" if stderr.strip() else f"{note}\n"
            output_text = stdout.strip()
            return ProviderResponse(
                status="succeeded",
                summary="Provider submitted result; terminated remaining subprocess tree.",
                output_text=output_text,
                stdout=stdout,
                stderr=stderr,
                raw_stream=stdout + stderr,
                usage={"terminated_after_result": True},
                exit_code=returncode,
            )

        return _process_text_response(
            returncode=returncode,
            stdout=stdout,
            stderr=stderr,
        )


def _completed_process_response(completed: subprocess.CompletedProcess[Any]) -> ProviderResponse:
    return _process_text_response(
        returncode=completed.returncode,
        stdout=_coerce_process_text(completed.stdout),
        stderr=_coerce_process_text(completed.stderr),
    )


def _process_text_response(*, returncode: int | None, stdout: str, stderr: str) -> ProviderResponse:
    exit_code = returncode if returncode is not None else -1
    status = "succeeded" if exit_code == 0 else "failed"
    output_text = stdout.strip()
    summary = output_text.splitlines()[0] if output_text else f"Subprocess exited {exit_code}."
    return ProviderResponse(
        status=status,
        summary=summary,
        output_text=output_text,
        stdout=stdout,
        stderr=stderr,
        raw_stream=stdout + stderr,
        exit_code=exit_code,
    )


def _provider_launch_error_response(request: ProviderRequest, exc: OSError) -> ProviderResponse:
    summary = f"Provider command not found: {request.argv[0]}"
    if getattr(exc, "winerror", None) == 206:
        summary = f"Provider command line too long: {request.argv[0]}"
    return ProviderResponse(
        status="failed",
        summary=summary,
        stderr=str(exc),
        raw_stream=str(exc),
        exit_code=127,
    )


def _start_reader_thread(
    stream: Any,
    buffer: list[str],
) -> threading.Thread | None:
    if stream is None:
        return None

    def read_stream() -> None:
        try:
            buffer.append(_coerce_process_text(stream.read()))
        except OSError as exc:
            buffer.append(f"\n[tasque] Provider stream read failed: {exc}\n")

    thread = threading.Thread(target=read_stream, daemon=True)
    thread.start()
    return thread


def _start_stdin_writer_thread(
    process: subprocess.Popen[Any],
    prompt: str,
) -> threading.Thread | None:
    if process.stdin is None:
        return None

    def write_stdin() -> None:
        try:
            process.stdin.write(prompt)
            process.stdin.close()
        except OSError:
            return

    thread = threading.Thread(target=write_stdin, daemon=True)
    thread.start()
    return thread


def _wait_for_process_exit(process: subprocess.Popen[Any]) -> int | None:
    try:
        return process.wait(timeout=SUBPROCESS_TERMINATION_GRACE_SECONDS)
    except subprocess.TimeoutExpired:
        _terminate_process_tree(process, force=True)
        try:
            return process.wait(timeout=SUBPROCESS_TERMINATION_GRACE_SECONDS)
        except subprocess.TimeoutExpired:
            return process.poll()


def _terminate_process_tree(process: subprocess.Popen[Any], *, force: bool) -> None:
    if process.poll() is not None:
        return
    if os.name == "nt":
        args = ["taskkill", "/PID", str(process.pid), "/T"]
        if force:
            args.append("/F")
        subprocess.run(
            args,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        return
    try:
        os.killpg(process.pid, signal.SIGKILL if force else signal.SIGTERM)
    except ProcessLookupError:
        return
    except OSError:
        if force:
            process.kill()
        else:
            process.terminate()


class CodexCliProvider(SubprocessProvider):
    name = "codex"

    def run(self, request: ProviderRequest) -> ProviderResponse:
        argv = ["codex", "exec", "--json", "--dangerously-bypass-approvals-and-sandbox"]
        argv.extend(_codex_tasque_mcp_args())
        if request.cwd:
            argv.extend(["--cd", request.cwd])
        if request.model:
            argv.extend(["--model", request.model])
        schema_path = None
        if request.output_schema:
            schema_file = tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                suffix=".json",
                delete=False,
            )
            try:
                json.dump(request.output_schema, schema_file)
                schema_file.close()
                schema_path = schema_file.name
                argv.extend(["--output-schema", schema_path])
            finally:
                if not schema_file.closed:
                    schema_file.close()
        argv.append("-")
        request = dataclass_replace(request, argv=argv)
        try:
            response = super().run(request)
        finally:
            if schema_path is not None:
                Path(schema_path).unlink(missing_ok=True)
        return _normalize_stream_response(response, provider="codex")


class ClaudeCodeProvider(SubprocessProvider):
    name = "claude"

    def run(self, request: ProviderRequest) -> ProviderResponse:
        argv = [
            "claude",
            "--print",
            "--output-format",
            "stream-json",
            "--permission-mode",
            "bypassPermissions",
        ]
        argv.extend(["--mcp-config", _claude_tasque_mcp_config()])
        if request.model:
            argv.extend(["--model", request.model])
        if request.output_schema:
            argv.extend(["--json-schema", json.dumps(request.output_schema)])
        argv.append(request.prompt)
        env = dict(request.env)
        env.setdefault("MCP_TIMEOUT", str(CLAUDE_MCP_STARTUP_TIMEOUT_MS))
        env.setdefault("MCP_TOOL_TIMEOUT", str(CLAUDE_MCP_TOOL_TIMEOUT_MS))
        request = dataclass_replace(request, argv=argv, env=env)
        return _normalize_stream_response(super().run(request), provider="claude")


def dataclass_replace(request: ProviderRequest, **changes: Any) -> ProviderRequest:
    data = {
        "provider": request.provider,
        "prompt": request.prompt,
        "cwd": request.cwd,
        "model": request.model,
        "argv": list(request.argv),
        "env": dict(request.env),
        "output_schema": request.output_schema,
        "expect_json": request.expect_json,
        "context": dict(request.context),
    }
    data.update(changes)
    return ProviderRequest(**data)


def default_provider_registry() -> ProviderRegistry:
    registry = ProviderRegistry()
    registry.register(FakeProvider())
    registry.register(SubprocessProvider())
    registry.register(CodexCliProvider())
    registry.register(ClaudeCodeProvider())
    return registry


class ProviderRuntime:
    def __init__(
        self,
        *,
        registry: ProviderRegistry | None = None,
        artifact_store: ArtifactStore | None = None,
    ) -> None:
        self.registry = registry or default_provider_registry()
        self.artifact_store = artifact_store or ArtifactStore()

    def can_run(self, worker_kind: str) -> bool:
        return worker_kind.startswith("provider.")

    def run(self, session: Session, work_item: WorkItem, attempt: WorkAttempt) -> WorkerResult:
        from tasque2.runtime import WorkerResult

        provider_name = provider_name_for_worker_kind(work_item.worker_kind)
        adapter = self.registry.get(provider_name)
        result_token = result_inbox.mint_token()
        context_limits = _provider_context_limits(work_item)
        context_packet = WorkerContextBuilder(session).build_for_work(work_item, limits=context_limits)
        context_packet["context_budget"] = {
            "limits": context_limits,
            "note": (
                "The initial provider context is size-budgeted. Use Tasque MCP "
                "read/search tools for additional full memory, artifact, work, or "
                "workflow context when needed."
            ),
        }
        context_packet["result_submission"] = {
            "tool": "submit_worker_result",
            "result_token": result_token,
            "required": True,
        }
        request = self._build_request(
            provider_name,
            work_item,
            context_packet=context_packet,
            result_token=result_token,
        )
        now = utc_now()
        provider_run = ProviderRun(
            attempt_id=attempt.id,
            provider=provider_name,
            model=request.model,
            cwd=request.cwd,
            argv=request.argv,
            env_keys=sorted(request.env.keys()),
            status="running",
            started_at=now,
        )
        session.add(provider_run)
        session.flush()
        attempt.provider = provider_name
        attempt.provider_run_id = provider_run.id
        session.commit()

        response = adapter.run(request)
        provider_run.status = response.status
        provider_run.provider_session_id = response.provider_session_id
        provider_run.usage = response.usage
        provider_run.ended_at = utc_now()
        attempt.exit_code = response.exit_code

        if response.stdout:
            provider_run.stdout_artifact_id = self._write_provider_artifact(
                session,
                title=f"{provider_name} stdout",
                content=response.stdout,
                work_item=work_item,
                attempt=attempt,
                provider_run=provider_run,
                tag="stdout",
            )
        if response.stderr:
            provider_run.stderr_artifact_id = self._write_provider_artifact(
                session,
                title=f"{provider_name} stderr",
                content=response.stderr,
                work_item=work_item,
                attempt=attempt,
                provider_run=provider_run,
                tag="stderr",
            )
        if response.raw_stream:
            provider_run.raw_stream_artifact_id = self._write_provider_artifact(
                session,
                title=f"{provider_name} raw stream",
                content=response.raw_stream,
                work_item=work_item,
                attempt=attempt,
                provider_run=provider_run,
                tag="raw",
            )
        trace = _provider_trace_markdown(response)
        if trace:
            self._write_provider_artifact(
                session,
                title=f"{provider_name} trace",
                content=trace,
                work_item=work_item,
                attempt=attempt,
                provider_run=provider_run,
                tag="trace",
                suffix=".md",
            )

        session.flush()
        session.commit()

        payload = result_inbox.read_and_consume(result_token, agent_kind="worker")
        if payload is None:
            if response.status != "succeeded":
                raise ProviderExecutionError(response.summary)
            raise ProviderExecutionError(_missing_result_error_message(response))

        worker_status, summary, report, produces = _normalize_submitted_result(payload)
        if worker_status in {"failed", "error"}:
            raise ProviderExecutionError(str(payload.get("error") or summary))
        if worker_status in {"blocked", "awaiting_user", "deferred"}:
            produces["completion_signal"] = worker_status

        report_artifact_id = self._write_report_artifact(
            session,
            content=report,
            work_item=work_item,
            attempt=attempt,
            provider_run=provider_run,
        )
        if report_artifact_id is not None:
            ingest_result = MemoryIngestService(session).ingest_artifact(
                report_artifact_id,
                namespace=_work_memory_namespace(work_item),
                tags=["provider_report", provider_name],
            )
            if ingest_result is not None:
                produces["ingested_memory_ids"] = [
                    *_string_list(produces.get("ingested_memory_ids")),
                    *ingest_result.memory_ids,
                ]
        produces = self._apply_memory_writes(
            session,
            work_item=work_item,
            provider_run=provider_run,
            produces=produces,
        )
        produces = self._record_declared_output_artifacts(
            session,
            work_item=work_item,
            attempt=attempt,
            provider_run=provider_run,
            request=request,
            produces=produces,
        )
        produces = self._spawn_declared_child_work(
            session,
            work_item=work_item,
            provider_run=provider_run,
            produces=produces,
        )
        bundle_artifact_id = self._write_provider_bundle_artifact(
            session,
            work_item=work_item,
            attempt=attempt,
            provider_run=provider_run,
            report_artifact_id=report_artifact_id,
            produces=produces,
            summary=summary,
        )
        produces["provider_run_bundle_artifact_id"] = bundle_artifact_id
        return WorkerResult(
            summary=summary,
            produces=produces,
            report_artifact_id=report_artifact_id,
        )

    def _apply_memory_writes(
        self,
        session: Session,
        *,
        work_item: WorkItem,
        provider_run: ProviderRun,
        produces: dict[str, Any],
    ) -> dict[str, Any]:
        writes = produces.get("memory_writes")
        if writes is None:
            return produces
        if not isinstance(writes, list):
            raise ProviderExecutionError("memory_writes must be a list.")

        service = MemoryService(session)
        memory_ids: list[str] = []
        for index, write in enumerate(writes):
            if not isinstance(write, dict):
                raise ProviderExecutionError(f"memory_writes[{index}] must be an object.")
            operation = str(write.get("operation") or "create").strip().lower()
            namespace = _required_write_string(write, "namespace", index)
            kind = _required_write_string(write, "kind", index)
            content = _required_write_string(write, "content", index)
            tags = _string_list(write.get("tags"))
            ttl_days = _optional_int(write.get("ttl_days"))
            pinned = bool(write.get("pinned", False))

            if operation in {"create", "append"}:
                memory = service.create_memory(
                    namespace=namespace,
                    kind=kind,
                    content=content,
                    tags=tags,
                    source_kind="provider_memory_write",
                    source_id=provider_run.id,
                    work_item_id=work_item.id,
                    canonical_key=_optional_string(write.get("canonical_key")),
                    pinned=pinned,
                    ttl_days=ttl_days,
                )
            elif operation in {"upsert_canonical", "canonical_upsert"}:
                canonical_key = _required_write_string(write, "canonical_key", index)
                memory = service.upsert_canonical(
                    namespace=namespace,
                    canonical_key=canonical_key,
                    kind=kind,
                    content=content,
                    tags=tags,
                    source_kind="provider_memory_write",
                    source_id=provider_run.id,
                    work_item_id=work_item.id,
                    pinned=pinned,
                    ttl_days=ttl_days,
                )
            elif operation == "supersede":
                memory_id = _required_write_string(write, "memory_id", index)
                memory = service.supersede_memory(memory_id, content=content, tags=tags)
            else:
                raise ProviderExecutionError(
                    f"Unsupported memory_writes[{index}].operation: {operation!r}."
                )
            memory_ids.append(memory.id)

        if memory_ids:
            produces["memory_ids"] = [*_string_list(produces.get("memory_ids")), *memory_ids]
        return produces

    def _build_request(
        self,
        provider_name: str,
        work_item: WorkItem,
        *,
        context_packet: dict[str, Any],
        result_token: str,
    ) -> ProviderRequest:
        contract = work_item.runtime_contract or {}
        context = work_item.context or {}
        cwd = contract.get("cwd") or context.get("cwd")
        argv = contract.get("argv") or []
        env = contract.get("env") or {}
        if not isinstance(argv, list):
            raise ProviderExecutionError("runtime_contract.argv must be a list.")
        if not isinstance(env, Mapping):
            raise ProviderExecutionError("runtime_contract.env must be an object.")
        model_routing = model_routing_for_provider_request(provider_name, contract)
        context_packet["model_routing"] = _model_routing_context(
            provider_name,
            model_routing,
        )
        use_model_contract = bool(
            contract.get("use_model_contract", provider_name in {"codex", "claude", "fake"})
        )
        output_schema = None
        prompt = (
            render_provider_prompt(
                task_instruction=work_item.task_instruction,
                context_packet=context_packet,
                result_token=result_token,
            )
            if use_model_contract
            else work_item.task_instruction
        )
        request_env = {str(key): str(value) for key, value in env.items()}
        request_env.setdefault("TASQUE2_RESULT_TOKEN", result_token)
        return ProviderRequest(
            provider=provider_name,
            prompt=prompt,
            cwd=str(Path(cwd)) if cwd else None,
            model=model_routing.orchestrator_model,
            argv=[str(item) for item in argv],
            env=request_env,
            output_schema=output_schema if isinstance(output_schema, dict) else None,
            expect_json=False,
            context={
                **context,
                "tasque_context_packet": context_packet,
                "result_token": result_token,
            },
        )

    def _validate_structured_output(
        self,
        response: ProviderResponse,
        request: ProviderRequest,
    ) -> ProviderResponse:
        if response.status != "succeeded" or not request.expect_json:
            return response
        if response.structured_output is not None:
            return response
        parsed = extract_structured_output(
            response.output_text,
            response.stdout,
            response.raw_stream,
        )
        if parsed is None:
            return ProviderResponse(
                status="failed",
                summary="Provider did not return valid JSON.",
                output_text=response.output_text,
                stdout=response.stdout,
                stderr=response.stderr,
                raw_stream=response.raw_stream,
                provider_session_id=response.provider_session_id,
                usage=response.usage,
                exit_code=response.exit_code,
            )
        structured = parsed if isinstance(parsed, dict) else {"value": parsed}
        return ProviderResponse(
            status=response.status,
            summary=response.summary,
            output_text=response.output_text,
            structured_output=structured,
            stdout=response.stdout,
            stderr=response.stderr,
            raw_stream=response.raw_stream,
            provider_session_id=response.provider_session_id,
            usage=response.usage,
            exit_code=response.exit_code,
        )

    def _write_provider_artifact(
        self,
        session: Session,
        *,
        title: str,
        content: str,
        work_item: WorkItem,
        attempt: WorkAttempt,
        provider_run: ProviderRun,
        tag: str,
        suffix: str = ".txt",
    ) -> str:
        artifact = self.artifact_store.write_text(
            session,
            kind="provider_stream",
            title=title,
            content=content,
            suffix=suffix,
            work_item_id=work_item.id,
            attempt_id=attempt.id,
            workflow_run_id=work_item.workflow_run_id,
            tags=["provider", provider_run.provider, tag],
            source_kind="provider_run",
            source_id=provider_run.id,
        )
        return artifact.id

    def _write_report_artifact(
        self,
        session: Session,
        *,
        content: str,
        work_item: WorkItem,
        attempt: WorkAttempt,
        provider_run: ProviderRun,
    ) -> str | None:
        if not content.strip():
            return None
        artifact = self.artifact_store.write_text(
            session,
            kind="worker_report",
            title=f"{work_item.title} report",
            content=content,
            suffix=".md",
            work_item_id=work_item.id,
            attempt_id=attempt.id,
            workflow_run_id=work_item.workflow_run_id,
            tags=["provider", provider_run.provider, "report"],
            source_kind="provider_run",
            source_id=provider_run.id,
        )
        return artifact.id

    def _write_provider_bundle_artifact(
        self,
        session: Session,
        *,
        work_item: WorkItem,
        attempt: WorkAttempt,
        provider_run: ProviderRun,
        report_artifact_id: str | None,
        produces: dict[str, Any],
        summary: str,
    ) -> str:
        artifact_refs = {
            "stdout": provider_run.stdout_artifact_id,
            "stderr": provider_run.stderr_artifact_id,
            "raw_stream": provider_run.raw_stream_artifact_id,
            "report": report_artifact_id,
        }
        body = "\n".join(
            [
                "# Provider Run Bundle",
                "",
                f"- provider_run_id: {provider_run.id}",
                f"- work_item_id: {work_item.id}",
                f"- attempt_id: {attempt.id}",
                f"- provider: {provider_run.provider}",
                f"- model: {provider_run.model or ''}",
                f"- status: {provider_run.status}",
                f"- started_at: {provider_run.started_at.isoformat() if provider_run.started_at else ''}",
                f"- ended_at: {provider_run.ended_at.isoformat() if provider_run.ended_at else ''}",
                f"- provider_session_id: {provider_run.provider_session_id or ''}",
                f"- summary: {summary}",
                "",
                "## Artifacts",
                *[
                    f"- {name}: {artifact_id}"
                    for name, artifact_id in artifact_refs.items()
                    if artifact_id is not None
                ],
                "",
                "## Usage",
                "```json",
                json.dumps(provider_run.usage or {}, indent=2, sort_keys=True),
                "```",
                "",
                "## Produces",
                "```json",
                json.dumps(produces, indent=2, sort_keys=True, default=str),
                "```",
            ]
        )
        artifact = self.artifact_store.write_text(
            session,
            kind="provider_bundle",
            title=f"{work_item.title} provider run bundle",
            content=body,
            suffix=".md",
            work_item_id=work_item.id,
            attempt_id=attempt.id,
            workflow_run_id=work_item.workflow_run_id,
            tags=["provider", provider_run.provider, "bundle"],
            source_kind="provider_run",
            source_id=provider_run.id,
        )
        return artifact.id

    def _record_declared_output_artifacts(
        self,
        session: Session,
        *,
        work_item: WorkItem,
        attempt: WorkAttempt,
        provider_run: ProviderRun,
        request: ProviderRequest,
        produces: dict[str, Any],
    ) -> dict[str, Any]:
        declared_artifact_ids: list[str] = []
        upload_artifact_ids: list[str] = []
        missing_paths: list[str] = []

        for declaration in _declared_artifact_paths(produces.get("artifact_paths")):
            artifact = self._capture_declared_artifact(
                session,
                declaration=declaration,
                work_item=work_item,
                attempt=attempt,
                provider_run=provider_run,
                request=request,
                upload_to_discord=False,
                missing_paths=missing_paths,
            )
            if artifact is not None:
                declared_artifact_ids.append(artifact.id)

        for declaration in _declared_artifact_paths(produces.get("discord_upload_paths")):
            artifact = self._capture_declared_artifact(
                session,
                declaration=declaration,
                work_item=work_item,
                attempt=attempt,
                provider_run=provider_run,
                request=request,
                upload_to_discord=True,
                missing_paths=missing_paths,
            )
            if artifact is not None:
                declared_artifact_ids.append(artifact.id)
                upload_artifact_ids.append(artifact.id)

        if declared_artifact_ids:
            produces["artifact_ids"] = [
                *_string_list(produces.get("artifact_ids")),
                *declared_artifact_ids,
            ]
        if upload_artifact_ids:
            produces["discord_upload_artifact_ids"] = [
                *_string_list(produces.get("discord_upload_artifact_ids")),
                *upload_artifact_ids,
            ]
        if missing_paths:
            produces["missing_artifact_paths"] = missing_paths
        return produces

    def _capture_declared_artifact(
        self,
        session: Session,
        *,
        declaration: dict[str, str],
        work_item: WorkItem,
        attempt: WorkAttempt,
        provider_run: ProviderRun,
        request: ProviderRequest,
        upload_to_discord: bool,
        missing_paths: list[str],
    ):
        raw_path = declaration["path"]
        path = Path(raw_path).expanduser()
        if not path.is_absolute():
            path = Path(request.cwd or Path.cwd()) / path
        path = path.resolve()
        if not path.is_file():
            missing_paths.append(str(path))
            return None

        tags = ["provider", provider_run.provider, "declared_artifact"]
        if upload_to_discord:
            tags.append("discord_upload")
        return self.artifact_store.capture_file(
            session,
            path=path,
            kind="worker_file",
            title=declaration.get("title") or path.name,
            work_item_id=work_item.id,
            attempt_id=attempt.id,
            workflow_run_id=work_item.workflow_run_id,
            content_type=declaration.get("content_type"),
            tags=tags,
            source_kind="provider_run",
            source_id=provider_run.id,
        )

    def _spawn_declared_child_work(
        self,
        session: Session,
        *,
        work_item: WorkItem,
        provider_run: ProviderRun,
        produces: dict[str, Any],
    ) -> dict[str, Any]:
        child_work = produces.get("child_work")
        if not isinstance(child_work, list):
            return produces

        child_ids: list[str] = []
        repo = WorkRepository(session)
        for index, item in enumerate(child_work):
            if not isinstance(item, dict):
                continue
            title = item.get("title")
            task_instruction = item.get("task_instruction") or item.get("instruction")
            if not title or not task_instruction:
                continue
            context = item.get("context") if isinstance(item.get("context"), dict) else {}
            child = repo.create_work_item(
                title=str(title),
                task_instruction=str(task_instruction),
                worker_kind=str(item.get("worker_kind") or "manual"),
                runtime_contract=dict(item.get("runtime_contract") or {}),
                context={
                    "parent_work_item_id": work_item.id,
                    "parent_provider_run_id": provider_run.id,
                    **context,
                },
                priority=int(item.get("priority", work_item.priority)),
                max_attempts=int(item.get("max_attempts", 1)),
                source_kind="provider_child_work",
                source_id=f"{provider_run.id}:{index}",
                idempotency_key=f"provider-child:{provider_run.id}:{index}",
                workflow_run_id=work_item.workflow_run_id,
                discord_thread_id=work_item.discord_thread_id,
            )
            child_ids.append(child.id)

        if child_ids:
            produces["child_work_item_ids"] = child_ids
        return produces


def provider_name_for_worker_kind(worker_kind: str) -> str:
    if worker_kind in DEFAULT_PROVIDER_WORKER_KINDS:
        return get_settings().default_provider_name
    return worker_kind.removeprefix("provider.")


def model_for_provider_request(provider_name: str, contract: Mapping[str, Any]) -> str | None:
    return model_routing_for_provider_request(provider_name, contract).orchestrator_model


def model_routing_for_provider_request(
    provider_name: str,
    contract: Mapping[str, Any],
) -> ProviderModelRouting:
    settings = get_settings()
    orchestrator_model = None
    native_worker_model = _contract_string(
        contract,
        "native_worker_model",
        "worker_model",
        "model",
    )
    orchestrator_profile = None
    native_worker_profile = None

    if provider_name in DEFAULT_MODEL_PROVIDERS:
        orchestrator_profile = settings.normalize_model_profile(settings.orchestrator_model_profile)
        orchestrator_model = settings.model_for_profile(provider_name, orchestrator_profile)

        if native_worker_model is None:
            native_worker_profile = _first_profile(
                contract,
                "native_worker_model_profile",
                "worker_model_profile",
                "model_profile",
                "tier",
            )
            if native_worker_profile is None and settings.native_worker_model_profile:
                native_worker_profile = settings.native_worker_model_profile
            if native_worker_profile is not None:
                native_worker_model = settings.model_for_profile(provider_name, native_worker_profile)
                native_worker_profile = settings.normalize_model_profile(native_worker_profile)
    else:
        orchestrator_model = _contract_string(contract, "model")

    return ProviderModelRouting(
        orchestrator_model=orchestrator_model,
        orchestrator_profile=orchestrator_profile,
        native_worker_model=native_worker_model,
        native_worker_profile=native_worker_profile,
    )


def _model_routing_context(
    provider_name: str,
    routing: ProviderModelRouting,
) -> dict[str, Any]:
    return {
        "provider": provider_name,
        "orchestrator": {
            "model": routing.orchestrator_model,
            "model_profile": routing.orchestrator_profile,
        },
        "native_worker": {
            "model": routing.native_worker_model,
            "model_profile": routing.native_worker_profile,
            "preferred_agent_name": "tasque_native_worker",
            "use_provider_native_delegation": True,
        },
    }


def _work_memory_namespace(work_item: WorkItem) -> str:
    context = work_item.context or {}
    namespace = context.get("memory_namespace")
    if namespace:
        return str(namespace)
    namespaces = context.get("memory_namespaces")
    if isinstance(namespaces, list) and namespaces:
        return str(namespaces[0])
    return "global"


def _provider_context_limits(work_item: WorkItem) -> dict[str, int | None]:
    limits = dict(DEFAULT_PROVIDER_CONTEXT_LIMITS)
    for source in (work_item.runtime_contract or {}, work_item.context or {}):
        configured = source.get("context_limits")
        if not isinstance(configured, dict):
            continue
        for key in PROVIDER_CONTEXT_LIMIT_KEYS:
            if key not in configured:
                continue
            value = configured[key]
            if value is None:
                limits[key] = None
                continue
            try:
                limits[key] = max(0, int(value))
            except (TypeError, ValueError):
                raise ProviderExecutionError(f"context_limits.{key} must be an integer or null.") from None
    return limits


def _contract_string(contract: Mapping[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = contract.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return None


def _first_profile(contract: Mapping[str, Any], *keys: str) -> str | None:
    profile = _contract_string(contract, *keys)
    if profile is None:
        return None
    settings = get_settings()
    return settings.normalize_model_profile(profile)


def _normalize_submitted_result(payload: dict[str, Any]) -> tuple[str, str, str, dict[str, Any]]:
    report = payload.get("report")
    summary = payload.get("summary")
    produces = payload.get("produces") or {}
    if not isinstance(report, str):
        raise ProviderExecutionError("submit_worker_result payload is missing string field 'report'.")
    if not isinstance(summary, str):
        raise ProviderExecutionError("submit_worker_result payload is missing string field 'summary'.")
    if not isinstance(produces, dict):
        raise ProviderExecutionError("submit_worker_result payload field 'produces' must be an object.")
    status = str(payload.get("status") or "succeeded").strip().lower()
    error = payload.get("error")
    clean_error = error.strip() if isinstance(error, str) else ""
    normalized_produces = dict(produces)
    if clean_error and status in {"blocked", "awaiting_user", "deferred"}:
        normalized_produces.setdefault("blocker", clean_error)
    elif clean_error:
        status = "failed"
    return status, summary, report, normalized_produces


def _missing_result_error_message(response: ProviderResponse) -> str:
    failures = _mcp_tool_failures(response.stdout, response.raw_stream)
    submit_failures = [failure for failure in failures if failure["tool"] == "submit_worker_result"]
    if submit_failures:
        failure = submit_failures[-1]
        return (
            "Provider attempted submit_worker_result, but the MCP tool call failed: "
            f"{failure['error']}."
        )
    if failures:
        shown = "; ".join(
            f"{failure['tool']}: {failure['error']}"
            for failure in failures[-3:]
        )
        return (
            "Provider did not deposit a structured result. Recent MCP tool failures: "
            f"{shown}."
        )
    return "Provider did not call submit_worker_result; no structured result was deposited."


def _mcp_tool_failures(*texts: str) -> list[dict[str, str]]:
    failures: list[dict[str, str]] = []
    for text in texts:
        for obj in _iter_json_objects(text):
            item = obj.get("item")
            if not isinstance(item, dict):
                continue
            if item.get("type") != "mcp_tool_call" or item.get("status") != "failed":
                continue
            tool = item.get("tool")
            error = item.get("error")
            message = None
            if isinstance(error, dict):
                message = error.get("message")
            elif isinstance(error, str):
                message = error
            failures.append(
                {
                    "tool": str(tool or "unknown"),
                    "error": str(message or "unknown MCP failure"),
                }
            )
    return failures


def _provider_trace_markdown(response: ProviderResponse) -> str:
    text = response.raw_stream or response.stdout
    objects = _iter_json_objects(text)
    if not objects:
        return ""

    lines = [
        "# Provider Trace",
        "",
        f"- status: {response.status}",
        f"- summary: {response.summary}",
    ]
    if response.provider_session_id:
        lines.append(f"- provider_session_id: {response.provider_session_id}")
    if response.exit_code is not None:
        lines.append(f"- exit_code: {response.exit_code}")
    if response.usage:
        lines.append(f"- usage: {json.dumps(response.usage, sort_keys=True)}")
    lines.append("")
    lines.append("## Events")

    for obj in objects:
        event_type = obj.get("type")
        item = obj.get("item")
        if isinstance(item, dict):
            item_type = item.get("type")
            if item_type == "agent_message":
                message = _one_line(str(item.get("text") or ""))
                lines.append(f"- agent: {message}")
            elif item_type == "mcp_tool_call":
                tool = item.get("tool") or "unknown"
                status = item.get("status") or "unknown"
                error = item.get("error")
                arguments = item.get("arguments")
                detail = f"- mcp `{tool}` {status}"
                if arguments is not None:
                    detail += f" args={_compact_json(arguments)}"
                if error is not None:
                    detail += f" error={_compact_json(error)}"
                lines.append(detail)
            elif item_type == "command_execution":
                status = item.get("status") or "unknown"
                command = _one_line(str(item.get("command") or ""))
                exit_code = item.get("exit_code")
                detail = f"- command {status}"
                if exit_code is not None:
                    detail += f" exit={exit_code}"
                if command:
                    detail += f": `{command[:300]}`"
                output = _one_line(str(item.get("aggregated_output") or ""))
                if output:
                    detail += f" output={output[:300]}"
                lines.append(detail)
            elif item_type:
                lines.append(f"- {item_type}: {item.get('status') or event_type or ''}")
        elif event_type in {"turn.started", "turn.completed", "thread.started"}:
            detail = f"- {event_type}"
            if "usage" in obj:
                detail += f" usage={_compact_json(obj['usage'])}"
            if "thread_id" in obj:
                detail += f" thread={obj['thread_id']}"
            lines.append(detail)

    return "\n".join(lines).strip()


def _compact_json(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=True, sort_keys=True)[:500]
    except TypeError:
        return str(value)[:500]


def _one_line(value: str) -> str:
    return " ".join(compress_text(value, max_chars=1000, preserve_lines=20).split())


def _coerce_process_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def tasque_mcp_server_config() -> dict[str, Any]:
    settings = get_settings()
    env = {
        "TASQUE2_DATA_DIR": str(settings.resolved_data_dir),
        "TASQUE2_DB_PATH": str(settings.database_path),
        "TASQUE2_TIMEZONE": settings.timezone,
    }
    python_path = os.environ.get("PYTHONPATH")
    if python_path:
        env["PYTHONPATH"] = python_path
    return {
        "command": sys.executable,
        "args": ["-m", "tasque2.mcp"],
        "env": env,
    }


def _codex_tasque_mcp_args() -> list[str]:
    config = tasque_mcp_server_config()
    args = [
        ("mcp_servers.tasque2.command", config["command"]),
        ("mcp_servers.tasque2.args", config["args"]),
        ("mcp_servers.tasque2.tool_timeout_sec", CODEX_MCP_TOOL_TIMEOUT_SECONDS),
    ]
    for key, value in sorted(config["env"].items()):
        args.append((f"mcp_servers.tasque2.env.{key}", value))

    result: list[str] = []
    for key, value in args:
        result.extend(["-c", f"{key}={_toml_value(value)}"])
    return result


def _claude_tasque_mcp_config() -> str:
    return json.dumps({"mcpServers": {"tasque2": tasque_mcp_server_config()}})


def _toml_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int | float):
        return str(value)
    return json.dumps(value)


def extract_structured_output(*texts: str) -> Any | None:
    for text in texts:
        if not text:
            continue
        for candidate in _structured_candidates(text):
            parsed = _loads_json(candidate)
            if parsed is not None:
                return parsed
    return None


def _structured_candidates(text: str) -> list[str]:
    candidates = [text.strip()]
    stream_text = extract_text_from_stream(text)
    if stream_text and stream_text != text.strip():
        candidates.insert(0, stream_text)

    for obj in _iter_json_objects(text):
        for key in ("result", "output_text", "final_response", "final_answer", "text"):
            value = obj.get(key)
            if isinstance(value, str):
                candidates.append(value.strip())
            elif value is not None:
                candidates.append(json.dumps(value))
    return [candidate for candidate in candidates if candidate]


def extract_text_from_stream(text: str) -> str:
    objects = list(_iter_json_objects(text))
    if not objects:
        return text.strip()

    preferred: list[str] = []
    fallback: list[str] = []
    for obj in objects:
        for key in ("result", "final_response", "final_answer", "output_text"):
            value = obj.get(key)
            if isinstance(value, str) and value.strip():
                preferred.append(value.strip())
        fallback.extend(_extract_text_values(obj))

    if preferred:
        return preferred[-1]
    if fallback:
        return "\n".join(fallback).strip()
    return text.strip()


def extract_session_id_from_stream(text: str) -> str | None:
    session_id = None
    for obj in _iter_json_objects(text):
        session_id = _find_first_string(obj, {"session_id", "sessionId", "conversation_id"}) or session_id
    return session_id


def extract_usage_from_stream(text: str) -> dict[str, Any]:
    usage: dict[str, Any] = {}
    for obj in _iter_json_objects(text):
        if isinstance(obj.get("usage"), dict):
            usage.update(obj["usage"])
        for key in ("total_cost_usd", "duration_ms", "num_turns", "input_tokens", "output_tokens"):
            if key in obj:
                usage[key] = obj[key]
    return usage


def _normalize_stream_response(response: ProviderResponse, *, provider: str) -> ProviderResponse:
    output_text = extract_text_from_stream(response.stdout) or response.output_text
    session_id = response.provider_session_id or extract_session_id_from_stream(response.stdout)
    usage = dict(response.usage)
    usage.update(extract_usage_from_stream(response.stdout))
    if response.status != "succeeded":
        error_summary = _stream_error_summary(response.stdout, response.raw_stream, response.stderr)
        return ProviderResponse(
            status=response.status,
            summary=error_summary or response.summary,
            output_text=output_text,
            structured_output=response.structured_output,
            stdout=response.stdout,
            stderr=response.stderr,
            raw_stream=response.raw_stream,
            provider_session_id=session_id,
            usage=usage,
            exit_code=response.exit_code,
        )
    structured = response.structured_output or extract_structured_output(output_text)
    summary = _first_line(output_text) or response.summary or f"{provider} completed."
    return ProviderResponse(
        status=response.status,
        summary=summary,
        output_text=output_text,
        structured_output=structured if isinstance(structured, dict) else None,
        stdout=response.stdout,
        stderr=response.stderr,
        raw_stream=response.raw_stream,
        provider_session_id=session_id,
        usage=usage,
        exit_code=response.exit_code,
    )


def _stream_error_summary(*texts: str) -> str | None:
    for text in texts:
        for obj in reversed(_iter_json_objects(text)):
            message = obj.get("message")
            if isinstance(message, str) and message.strip():
                return message.strip()
            error = obj.get("error")
            if isinstance(error, dict):
                nested = error.get("message")
                if isinstance(nested, str) and nested.strip():
                    return nested.strip()
            elif isinstance(error, str) and error.strip():
                return error.strip()
    return None


def _iter_json_objects(text: str) -> list[dict[str, Any]]:
    parsed = _loads_json(text.strip())
    if isinstance(parsed, dict):
        return [parsed]
    if isinstance(parsed, list):
        return [item for item in parsed if isinstance(item, dict)]

    objects: list[dict[str, Any]] = []
    for line in text.splitlines():
        parsed_line = _loads_json(line.strip())
        if isinstance(parsed_line, dict):
            objects.append(parsed_line)
    return objects


def _loads_json(text: str) -> Any | None:
    if not text:
        return None
    stripped = _strip_json_fence(text)
    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        return None


def _strip_json_fence(text: str) -> str:
    stripped = text.strip()
    if stripped.startswith("```"):
        lines = stripped.splitlines()
        if len(lines) >= 3 and lines[-1].strip() == "```":
            return "\n".join(lines[1:-1]).strip()
    return stripped


def _extract_text_values(value: Any) -> list[str]:
    if isinstance(value, str):
        return []
    if isinstance(value, list):
        texts: list[str] = []
        for item in value:
            texts.extend(_extract_text_values(item))
        return texts
    if not isinstance(value, dict):
        return []

    texts = []
    for key in ("content", "text", "message"):
        child = value.get(key)
        if isinstance(child, str) and child.strip():
            texts.append(child.strip())
        elif isinstance(child, (dict, list)):
            texts.extend(_extract_text_values(child))
    for key in ("delta", "data", "payload"):
        child = value.get(key)
        if isinstance(child, (dict, list)):
            texts.extend(_extract_text_values(child))
    return texts


def _find_first_string(value: Any, keys: set[str]) -> str | None:
    if isinstance(value, dict):
        for key, child in value.items():
            if key in keys and isinstance(child, str):
                return child
            found = _find_first_string(child, keys)
            if found is not None:
                return found
    elif isinstance(value, list):
        for child in value:
            found = _find_first_string(child, keys)
            if found is not None:
                return found
    return None


def _first_line(text: str) -> str:
    return next((line.strip() for line in text.splitlines() if line.strip()), "")


def _declared_artifact_paths(value: Any) -> list[dict[str, str]]:
    if value is None:
        return []
    if isinstance(value, str):
        return [{"path": value}]
    if not isinstance(value, list):
        return []
    declarations: list[dict[str, str]] = []
    for item in value:
        if isinstance(item, str):
            declarations.append({"path": item})
        elif isinstance(item, dict) and isinstance(item.get("path"), str):
            declaration = {"path": str(item["path"])}
            for key in ("title", "content_type"):
                if item.get(key) is not None:
                    declaration[key] = str(item[key])
            declarations.append(declaration)
    return declarations


def _string_list(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return [str(item) for item in value if item]
    return []


def _required_write_string(write: dict[str, Any], key: str, index: int) -> str:
    value = write.get(key)
    if value is None or str(value).strip() == "":
        raise ProviderExecutionError(f"memory_writes[{index}].{key} is required.")
    return str(value).strip()


def _optional_string(value: Any) -> str | None:
    if value is None or str(value).strip() == "":
        return None
    return str(value).strip()


def _optional_int(value: Any) -> int | None:
    if value is None or str(value).strip() == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ProviderExecutionError(f"Invalid integer value for memory write: {value!r}.") from exc
