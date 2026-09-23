from __future__ import annotations

import asyncio
import inspect
import json
from pathlib import Path

import pytest
from opentelemetry.trace import SpanKind, StatusCode

from tasque2.extensions import registry as extension_registry
from tasque2.mcp.server import INSTRUCTIONS, build_server, traced
from tasque2.mcp.toolkit import run_json
from tasque2.mcp.tools import CORE_TOOLS, memory_recall, submit_worker_result

TRACE_ID = "0af7651916cd43dd8448eb211c80319c"
PARENT_SPAN_ID = "b7ad6b7169203331"
TRACEPARENT = f"00-{TRACE_ID}-{PARENT_SPAN_ID}-01"
BASE_ATTRIBUTES = {
    "mcp.method.name": "tools/call",
    "gen_ai.operation.name": "execute_tool",
    "network.transport": "pipe",
}


def _tools() -> dict:
    return {tool.name: tool for tool in asyncio.run(build_server().list_tools())}


def _call_span(spans, tool: str):
    return next(item for item in spans.get_finished_spans() if item.name == f"tools/call {tool}")


def _durations(metric_points, tool: str) -> list:
    return [
        point
        for point in metric_points("mcp.server.operation.duration")
        if point.attributes.get("gen_ai.tool.name") == tool
    ]


def test_traced_tool_keeps_its_name_signature_and_docstring() -> None:
    wrapped = traced(memory_recall)

    assert wrapped.__name__ == "memory_recall"
    assert wrapped.__doc__ == memory_recall.__doc__
    assert inspect.signature(wrapped) == inspect.signature(memory_recall)
    assert wrapped.__wrapped__ is memory_recall


def test_server_serves_every_core_tool_with_its_parameters() -> None:
    tools = _tools()

    assert set(tools) == {tool.__name__ for tool in CORE_TOOLS}
    recall = tools["memory_recall"]
    assert recall.description == memory_recall.__doc__
    assert recall.inputSchema["required"] == ["query"]
    assert list(recall.inputSchema["properties"]) == ["query", "namespace", "tags", "limit", "intent"]
    assert recall.inputSchema["properties"]["limit"]["default"] == 8
    submit = tools["submit_worker_result"]
    assert submit.description == submit_worker_result.__doc__
    assert submit.inputSchema["required"] == ["result_token", "summary", "report"]
    assert submit.inputSchema["properties"]["status"]["default"] == "succeeded"


def test_server_serves_extension_tools_beside_the_core() -> None:
    def ledger_balance(account: str, intent: str = "") -> str:
        """Current balance of one ledger account."""
        return run_json(lambda: {"ok": True, "account": account})

    extension_registry().add_mcp_tools(ledger_balance)
    tools = _tools()

    assert len(tools) == len(CORE_TOOLS) + 1
    assert tools["ledger_balance"].description == "Current balance of one ledger account."
    assert tools["ledger_balance"].inputSchema["required"] == ["account"]


def test_server_instructions_state_the_result_contract() -> None:
    assert build_server().instructions == INSTRUCTIONS
    assert "submit_worker_result exactly once" in INSTRUCTIONS


def test_tool_call_records_a_server_span_under_the_runs_traceparent(spans, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRACEPARENT", TRACEPARENT)
    monkeypatch.setenv("TASQUE2_WORK_ITEM_ID", "work-7")

    def ledger_balance(account: str) -> str:
        """Balance."""
        return json.dumps({"ok": True, "account": account})

    result = traced(ledger_balance)(account="checking")

    call = _call_span(spans, "ledger_balance")
    assert result == '{"ok": true, "account": "checking"}'
    assert call.kind is SpanKind.SERVER
    assert f"{call.context.trace_id:032x}" == TRACE_ID
    assert f"{call.parent.span_id:016x}" == PARENT_SPAN_ID
    assert call.parent.is_remote is True
    assert dict(call.attributes) == {
        **BASE_ATTRIBUTES,
        "gen_ai.tool.name": "ledger_balance",
        "tasque.work_item.id": "work-7",
    }
    assert call.status.status_code is StatusCode.UNSET


def test_tool_called_through_the_server_is_traced(fresh_db: Path, spans, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRACEPARENT", TRACEPARENT)

    _content, structured = asyncio.run(build_server().call_tool("system_status", {}))

    call = _call_span(spans, "system_status")
    assert json.loads(structured["result"])["ok"] is True
    assert f"{call.parent.span_id:016x}" == PARENT_SPAN_ID
    assert "error.type" not in call.attributes


def test_tool_call_without_a_traceparent_starts_its_own_trace(spans) -> None:
    def lonely(x: str) -> str:
        """Lonely."""
        return json.dumps({"ok": True})

    traced(lonely)(x="1")

    call = _call_span(spans, "lonely")
    assert call.parent is None
    assert "tasque.work_item.id" not in call.attributes


def test_successful_tool_call_records_its_duration(metric_points) -> None:
    def quick_tool(x: str) -> str:
        """Quick."""
        return json.dumps({"ok": True})

    traced(quick_tool)(x="1")

    points = _durations(metric_points, "quick_tool")
    assert len(points) == 1
    assert points[0].count == 1
    assert dict(points[0].attributes) == {**BASE_ATTRIBUTES, "gen_ai.tool.name": "quick_tool"}


def test_tool_reported_error_marks_the_span_and_the_duration(
    spans, metric_points, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("TASQUE2_WORK_ITEM_ID", "work-8")

    def failing_lookup(key: str) -> str:
        """Look something up."""

        def body() -> dict:
            raise KeyError(key)

        return run_json(body)

    result = traced(failing_lookup)(key="x")

    call = _call_span(spans, "failing_lookup")
    points = _durations(metric_points, "failing_lookup")
    assert json.loads(result)["ok"] is False
    assert call.status.status_code is StatusCode.ERROR
    assert call.attributes["error.type"] == "KeyError"
    assert [point.attributes.get("error.type") for point in points] == ["KeyError"]
    assert "tasque.work_item.id" not in points[0].attributes


def test_tool_error_without_a_type_is_a_generic_tool_error(spans) -> None:
    def vague(x: str) -> str:
        """Vague."""
        return json.dumps({"ok": False, "error": "nope"})

    traced(vague)(x="1")

    assert _call_span(spans, "vague").attributes["error.type"] == "tool_error"


def test_raising_tool_marks_the_span_and_reraises(spans, metric_points) -> None:
    def exploding(x: str) -> str:
        """Explodes."""
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        traced(exploding)(x="1")

    call = _call_span(spans, "exploding")
    assert call.status.status_code is StatusCode.ERROR
    assert call.attributes["error.type"] == "RuntimeError"
    assert [event.name for event in call.events] == ["exception"]
    assert [point.attributes.get("error.type") for point in _durations(metric_points, "exploding")] == ["RuntimeError"]
