"""The Tasque stdio MCP server: core tools plus extension tools, every call traced.

A provider run starts this server as a child of the agent CLI. Each tool call becomes an
OpenTelemetry span (``tools/call {name}``) parented to the run's ``invoke_agent`` span
through ``TRACEPARENT``, and is exported before the call returns because the process tree
is shut down as soon as the worker submits its result.
"""

from __future__ import annotations

import functools
import json
import os
import time
from collections.abc import Callable
from typing import Any

from opentelemetry.trace import SpanKind, Status, StatusCode

from tasque2.extensions import registry as extension_registry
from tasque2.mcp.tools import CORE_TOOLS
from tasque2.telemetry import (
    clean_attributes,
    configure_telemetry,
    context_from_env,
    flush_telemetry,
    get_tracer,
    instruments,
)

INSTRUCTIONS = (
    "Tasque tools for workers: durable memory, artifacts, images, work items, schedules and "
    "workflows. Read tools take an optional intent string describing why you are reading. "
    "Every tool returns JSON: {ok: true, ...} or {ok: false, error, error_type}. "
    "End every run by calling submit_worker_result exactly once with the result_token from the prompt."
)


def build_server():
    from mcp.server.fastmcp import FastMCP

    server = FastMCP("tasque2", instructions=INSTRUCTIONS)
    for tool in (*CORE_TOOLS, *extension_registry().mcp_tools):
        server.tool()(traced(tool))
    return server


def traced(tool: Callable[..., str]) -> Callable[..., str]:
    """Wrap a tool so each call records an MCP server span and duration metric."""
    name = tool.__name__

    @functools.wraps(tool)
    def call(*args: Any, **kwargs: Any) -> str:
        attributes = clean_attributes(
            {
                "mcp.method.name": "tools/call",
                "gen_ai.operation.name": "execute_tool",
                "gen_ai.tool.name": name,
                "network.transport": "pipe",
                "tasque.work_item.id": os.environ.get("TASQUE2_WORK_ITEM_ID") or None,
            }
        )
        started = time.perf_counter()
        error_type: str | None = None
        try:
            with get_tracer().start_as_current_span(
                f"tools/call {name}",
                context=context_from_env(),
                kind=SpanKind.SERVER,
                attributes=attributes,
                record_exception=False,
                set_status_on_exception=False,
            ) as span:
                try:
                    result = tool(*args, **kwargs)
                    error_type = _tool_error(result)
                except Exception as exc:
                    error_type = type(exc).__name__
                    span.record_exception(exc)
                    raise
                finally:
                    if error_type:
                        span.set_attribute("error.type", error_type)
                        span.set_status(Status(StatusCode.ERROR))
                    _record_duration(started, attributes, error_type)
            return result
        finally:
            flush_telemetry(timeout_millis=2000)

    return call


def _record_duration(started: float, attributes: dict[str, Any], error_type: str | None) -> None:
    metric_attributes = {key: value for key, value in attributes.items() if key != "tasque.work_item.id"}
    if error_type:
        metric_attributes["error.type"] = error_type
    instruments().mcp_operation_duration.record(time.perf_counter() - started, metric_attributes)


def _tool_error(result: Any) -> str | None:
    """The error type a tool reported in its JSON envelope, if it reported one."""
    if not isinstance(result, str) or '"ok": false' not in result:
        return None
    try:
        payload = json.loads(result)
    except ValueError:
        return None
    if isinstance(payload, dict) and payload.get("ok") is False:
        return str(payload.get("error_type") or "tool_error")
    return None


def run_stdio() -> None:
    from tasque2.logs import configure_logging
    from tasque2.migrations import upgrade_database

    configure_logging()
    configure_telemetry("mcp")
    upgrade_database()
    build_server().run("stdio")


__all__ = ["INSTRUCTIONS", "build_server", "run_stdio", "traced"]
