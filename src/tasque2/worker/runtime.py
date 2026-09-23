"""One provider run end to end: packet, prompt, subprocess, artifacts, telemetry, result."""

from __future__ import annotations

import json
import os
import re
from collections.abc import Mapping
from typing import Any

from opentelemetry.trace import SpanKind
from sqlalchemy.orm import Session

from tasque2.artifacts import ArtifactStore
from tasque2.config import ModelChoice, get_settings
from tasque2.models import ProviderRun, WorkAttempt, WorkItem, utc_now
from tasque2.providers import (
    ProviderExecutionError,
    ProviderRegistry,
    ProviderRequest,
    ProviderResponse,
    TransientProviderError,
    default_provider_registry,
    provider_name_for_worker_kind,
)
from tasque2.providers.claude import missing_result_message
from tasque2.providers.mcp_config import mcp_server_allowlist
from tasque2.providers.pricing import estimate_cost_usd
from tasque2.providers.stream import parse_stream, render_trace_markdown
from tasque2.scratch import scratch_dir_for_attempt, scratch_environment
from tasque2.telemetry import inject_trace_env, instruments, span, telemetry_active
from tasque2.work.runner import WorkerResult
from tasque2.worker import results
from tasque2.worker.context import WorkerContextBuilder
from tasque2.worker.finalize import finalize_payload
from tasque2.worker.prompt import contract_path, render_user_prompt

GEN_AI_PROVIDERS = {"claude": "anthropic", "codex": "openai"}
_RESOURCE_VALUE_RE = re.compile(r"[^A-Za-z0-9._-]+")


class ProviderRuntime:
    def __init__(
        self, *, registry: ProviderRegistry | None = None, artifact_store: ArtifactStore | None = None
    ) -> None:
        self.registry = registry or default_provider_registry()
        self.artifact_store = artifact_store or ArtifactStore()

    def can_run(self, worker_kind: str) -> bool:
        return worker_kind.startswith("provider.")

    def run(self, session: Session, work_item: WorkItem, attempt: WorkAttempt) -> WorkerResult:
        settings = get_settings()
        provider_name = provider_name_for_worker_kind(work_item.worker_kind)
        adapter = self.registry.get(provider_name)
        contract = work_item.runtime_contract or {}
        choice = model_choice_for(provider_name, contract)
        result_token = results.mint_token()
        scratch_dir = scratch_dir_for_attempt(attempt.id)

        with span("tasque.worker.context", attributes={"tasque.work.id": work_item.id}) as context_span:
            packet = WorkerContextBuilder(session).build_for_work(work_item)
            prompt = render_user_prompt(
                task_instruction=work_item.task_instruction,
                context_packet=packet,
                result_token=result_token,
                scratch_dir=scratch_dir,
            )
            context_span.set_attribute("tasque.prompt.chars", len(prompt))
            context_span.set_attribute("tasque.packet.memories", len(packet.get("memories") or []))

        env = {str(key): str(value) for key, value in _mapping(contract.get("env"), "env").items()}
        env.setdefault("TASQUE2_RESULT_TOKEN", result_token)
        env.setdefault("TASQUE2_WORK_ITEM_ID", work_item.id)
        for key, value in scratch_environment(scratch_dir).items():
            env.setdefault(key, value)
        cwd = str(contract.get("cwd") or (work_item.context or {}).get("cwd") or settings.resolved_project_dir)

        provider_run = ProviderRun(
            attempt_id=attempt.id,
            provider=provider_name,
            model=choice.model,
            cwd=cwd,
            argv=[],
            env_keys=sorted(env),
            status="running",
            started_at=utc_now(),
        )
        session.add(provider_run)
        session.flush()
        attempt.provider = provider_name
        attempt.provider_run_id = provider_run.id
        session.commit()

        agent_name = work_item.lane or work_item.title
        try:
            with span(
                f"invoke_agent {agent_name}",
                kind=SpanKind.CLIENT,
                attributes={
                    "gen_ai.operation.name": "invoke_agent",
                    "gen_ai.provider.name": GEN_AI_PROVIDERS.get(provider_name, provider_name),
                    "gen_ai.agent.name": agent_name,
                    "gen_ai.request.model": choice.model,
                    "tasque.model.profile": choice.profile,
                    "tasque.model.effort": choice.effort,
                    "tasque.work.id": work_item.id,
                    "tasque.provider.run.id": provider_run.id,
                },
            ) as agent_span:
                inject_trace_env(env)
                env.update(worker_telemetry_env(provider_name, work_item, attempt))
                request = ProviderRequest(
                    provider=provider_name,
                    prompt=prompt,
                    cwd=cwd,
                    model=choice.model,
                    effort=choice.effort,
                    system_prompt_path=contract_path(),
                    env=env,
                    mcp_servers=mcp_server_allowlist(contract),
                    disallowed_tools=_string_list(contract.get("disallowed_tools"), "disallowed_tools"),
                    max_turns=_optional_int(contract.get("max_turns")),
                    max_budget_usd=_optional_float(contract.get("max_budget_usd")),
                    result_token=result_token,
                    work_item_id=work_item.id,
                    argv=[str(item) for item in _list(contract.get("argv"), "argv")],
                )
                argv_record = _redacted_argv(adapter, request)
                response = adapter.run(request)
                summary = parse_stream(response.stdout)
                usage = {**response.usage, **summary.usage_record()}
                cost = estimate_cost_usd(summary.model or choice.model, summary.usage)
                if cost is not None:
                    usage["estimated_cost_usd"] = round(cost, 4)
                self._observe(agent_span, provider_name, work_item, choice, summary, cost)
                agent_span.set_attribute("tasque.provider.status", response.status)
                if response.exit_code is not None:
                    agent_span.set_attribute("tasque.provider.exit_code", response.exit_code)
        except Exception:
            provider_run.status = "failed"
            provider_run.ended_at = utc_now()
            raise

        provider_run.argv = argv_record
        provider_run.status = response.status
        provider_run.provider_session_id = response.provider_session_id
        provider_run.usage = usage
        provider_run.ended_at = utc_now()
        attempt.exit_code = response.exit_code
        self._write_run_artifacts(session, work_item, attempt, provider_run, response, summary)
        session.commit()

        payload = results.read_and_consume(result_token)
        if payload is None:
            if response.status != "succeeded":
                raise TransientProviderError(response.summary)
            raise TransientProviderError(missing_result_message(response))
        return self.finalize(session, work_item=work_item, attempt=attempt, provider_run=provider_run, payload=payload)

    def finalize(
        self,
        session: Session,
        *,
        work_item: WorkItem,
        attempt: WorkAttempt,
        provider_run: ProviderRun,
        payload: dict[str, Any],
    ) -> WorkerResult:
        return finalize_payload(
            session,
            work_item=work_item,
            attempt=attempt,
            provider_run=provider_run,
            payload=payload,
            store=self.artifact_store,
        )

    def _write_run_artifacts(
        self,
        session: Session,
        work_item: WorkItem,
        attempt: WorkAttempt,
        provider_run: ProviderRun,
        response: ProviderResponse,
        summary,
    ) -> None:
        def write(tag: str, title: str, content: str, suffix: str = ".txt") -> str:
            return self.artifact_store.write_text(
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
            ).id

        if response.stdout:
            provider_run.stdout_artifact_id = write("stream", f"{provider_run.provider} stream", response.stdout)
        if response.stderr.strip():
            provider_run.stderr_artifact_id = write("stderr", f"{provider_run.provider} stderr", response.stderr)
        trace = render_trace_markdown(summary, status=response.status, exit_code=response.exit_code)
        if trace:
            write("trace", f"{provider_run.provider} trace", trace, suffix=".md")

    def _observe(self, agent_span, provider_name: str, work_item: WorkItem, choice: ModelChoice, summary, cost) -> None:
        usage = summary.usage
        model = summary.model or choice.model or "unknown"
        lane = work_item.lane or "unassigned"
        agent_span.set_attribute("gen_ai.response.model", model)
        agent_span.set_attribute("gen_ai.usage.input_tokens", usage.total_input)
        agent_span.set_attribute("gen_ai.usage.output_tokens", usage.output_tokens)
        agent_span.set_attribute("gen_ai.usage.cache_read.input_tokens", usage.cache_read_tokens)
        agent_span.set_attribute("gen_ai.usage.cache_creation.input_tokens", usage.cache_write_tokens)
        agent_span.set_attribute("tasque.provider.messages", summary.messages)
        agent_span.set_attribute("tasque.provider.subagent_messages", summary.subagent_messages)
        if summary.session_id:
            agent_span.set_attribute("gen_ai.conversation.id", summary.session_id)
        if cost is not None:
            agent_span.set_attribute("tasque.provider.estimated_cost_usd", round(cost, 4))

        metrics = instruments()
        base = {
            "gen_ai.provider.name": GEN_AI_PROVIDERS.get(provider_name, provider_name),
            "gen_ai.request.model": choice.model or model,
            "tasque.work.lane": lane,
        }
        metrics.token_usage.record(usage.total_input, {**base, "gen_ai.token.type": "input"})
        metrics.token_usage.record(usage.output_tokens, {**base, "gen_ai.token.type": "output"})
        for token_type, count in (
            ("input", usage.input_tokens),
            ("output", usage.output_tokens),
            ("cache_read", usage.cache_read_tokens),
            ("cache_write", usage.cache_write_tokens),
        ):
            if count:
                metrics.tokens.add(count, {**base, "tasque.token.type": token_type})
        if cost is not None:
            metrics.cost.add(cost, base)
        metrics.turns.record(summary.messages, base)
        for tool, count in summary.tool_calls.items():
            metrics.worker_tool_calls.add(
                count, {"gen_ai.tool.name": tool, "tasque.tool.server": _tool_server(tool), "tasque.work.lane": lane}
            )


def model_choice_for(provider_name: str, contract: Mapping[str, Any]) -> ModelChoice:
    return get_settings().model_choice(
        provider_name,
        _optional_str(contract.get("model_profile")),
        model=_optional_str(contract.get("model")),
        effort=_optional_str(contract.get("effort")),
    )


def worker_telemetry_env(provider_name: str, work_item: WorkItem, attempt: WorkAttempt) -> dict[str, str]:
    """Environment that makes the agent CLI export its own telemetry, tagged with this run."""
    settings = get_settings()
    if provider_name != "claude" or not telemetry_active() or not settings.telemetry_worker_export:
        return {}
    attributes = {
        "service.name": "claude-code",
        "tasque.work.id": work_item.id,
        "tasque.work.lane": work_item.lane or "unassigned",
        "tasque.work.attempt": str(attempt.attempt_number),
    }
    env = {
        "CLAUDE_CODE_ENABLE_TELEMETRY": "1",
        "OTEL_METRICS_EXPORTER": "otlp",
        "OTEL_LOGS_EXPORTER": "otlp",
        # The agent CLI exports nothing until told the protocol; Tasque's own exporters speak OTLP over HTTP.
        "OTEL_EXPORTER_OTLP_PROTOCOL": os.environ.get("OTEL_EXPORTER_OTLP_PROTOCOL", "").strip() or "http/protobuf",
        "OTEL_METRIC_EXPORT_INTERVAL": "10000",
        "OTEL_LOGS_EXPORT_INTERVAL": "2000",
        "OTEL_RESOURCE_ATTRIBUTES": ",".join(
            f"{key}={_RESOURCE_VALUE_RE.sub('_', value)}" for key, value in attributes.items()
        ),
    }
    if settings.telemetry_worker_traces:
        env["CLAUDE_CODE_ENHANCED_TELEMETRY_BETA"] = "1"
        env["OTEL_TRACES_EXPORTER"] = "otlp"
    return env


def _redacted_argv(adapter, request: ProviderRequest) -> list[str]:
    """The command line for the record, with MCP server configs reduced to their names."""
    build = getattr(adapter, "build_argv", None)
    if build is None:
        return list(request.argv)
    try:
        argv = build(request)
    except ProviderExecutionError:
        return []
    redacted: list[str] = []
    skip_next = False
    for index, part in enumerate(argv):
        if skip_next:
            skip_next = False
            continue
        if part == "--mcp-config" and index + 1 < len(argv):
            try:
                servers = sorted(json.loads(argv[index + 1]).get("mcpServers", {}))
            except (ValueError, AttributeError):
                servers = []
            redacted.extend([part, f"<servers: {', '.join(servers)}>"])
            skip_next = True
        elif part.startswith("mcp_servers.") and ".env." in part:
            redacted.append(part.split("=", 1)[0] + "=<redacted>")
        else:
            redacted.append(part)
    return redacted


def _tool_server(tool: str) -> str:
    if tool.startswith("mcp__"):
        parts = tool.split("__")
        return parts[1] if len(parts) > 2 else "mcp"
    return "builtin"


def _mapping(value: Any, name: str) -> Mapping[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ProviderExecutionError(f"runtime_contract.{name} must be an object.")
    return value


def _list(value: Any, name: str) -> list[Any]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ProviderExecutionError(f"runtime_contract.{name} must be a list.")
    return value


def _string_list(value: Any, name: str) -> list[str]:
    items = _list(value, name)
    if not all(isinstance(item, str) for item in items):
        raise ProviderExecutionError(f"runtime_contract.{name} must be a list of strings.")
    return [item.strip() for item in items if item.strip()]


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _optional_int(value: Any) -> int | None:
    if value is None or str(value).strip() == "":
        return None
    return int(value)


def _optional_float(value: Any) -> float | None:
    if value is None or str(value).strip() == "":
        return None
    return float(value)
