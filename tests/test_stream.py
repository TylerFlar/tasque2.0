from __future__ import annotations

import json

import pytest

from tasque2.providers.pricing import estimate_cost_usd, list_price
from tasque2.providers.stream import (
    TokenUsage,
    iter_json_objects,
    parse_stream,
    render_trace_markdown,
    strip_tool_call_xml,
)

NS = "antml" + ":"


def _stream(*events: dict) -> str:
    return "\n".join(json.dumps(event) for event in events)


def _init(*servers: tuple[str, str], model: str = "claude-sonnet-5") -> dict:
    return {
        "type": "system",
        "subtype": "init",
        "session_id": "sess-1",
        "model": model,
        "mcp_servers": [{"name": name, "status": status} for name, status in servers],
    }


def _assistant(
    message_id: str,
    *blocks: dict,
    usage: dict | None = None,
    model: str = "claude-sonnet-5",
    parent: str | None = None,
) -> dict:
    message: dict = {"id": message_id, "model": model, "content": list(blocks)}
    if usage is not None:
        message["usage"] = usage
    event: dict = {"type": "assistant", "message": message, "session_id": "sess-1"}
    if parent:
        event["parent_tool_use_id"] = parent
    return event


def _text(text: str) -> dict:
    return {"type": "text", "text": text}


def _tool_use(tool_id: str, name: str, **arguments) -> dict:
    return {"type": "tool_use", "id": tool_id, "name": name, "input": arguments}


def _tool_error(tool_id: str, content) -> dict:
    block = {"type": "tool_result", "tool_use_id": tool_id, "is_error": True, "content": content}
    return {"type": "user", "message": {"content": [block]}}


def _usage(input_tokens: int = 0, output_tokens: int = 0, cache_read: int = 0, cache_write: int = 0) -> dict:
    return {
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "cache_read_input_tokens": cache_read,
        "cache_creation_input_tokens": cache_write,
    }


def _result(text: str = "Done.", **fields) -> dict:
    return {"type": "result", "subtype": "success", "is_error": False, "result": text, "session_id": "sess-1", **fields}


def test_parse_stream_reads_session_model_answer_and_result_fields() -> None:
    summary = parse_stream(
        _stream(
            _init(("tasque2", "connected")),
            _assistant("msg_1", _text("Working on it.")),
            _result('{"ok": true, "message": "done"}', total_cost_usd=0.05, num_turns=3, duration_ms=1234),
        )
    )

    assert summary.session_id == "sess-1"
    assert summary.model == "claude-sonnet-5"
    assert summary.final_text == '{"ok": true, "message": "done"}'
    assert summary.has_result_event is True
    assert summary.error is None
    assert summary.reported_cost_usd == 0.05
    assert summary.num_turns == 3
    assert summary.duration_ms == 1234
    assert len(summary.events) == 3


def test_usage_is_counted_once_per_message_id() -> None:
    summary = parse_stream(
        _stream(
            _assistant("msg_1", _text("Looking."), usage=_usage(10, 5, 100, 20)),
            _assistant("msg_1", _tool_use("toolu_1", "Read"), usage=_usage(10, 30, 100, 20)),
        )
    )

    assert summary.messages == 1
    assert summary.usage.as_dict() == {
        "input_tokens": 10,
        "output_tokens": 30,
        "cache_read_tokens": 100,
        "cache_write_tokens": 20,
    }


def test_messages_are_counted_once_per_id_without_usage() -> None:
    summary = parse_stream(
        _stream(
            _assistant("msg_1", _text("One.")),
            _assistant("msg_1", _tool_use("toolu_1", "Bash")),
            _assistant("msg_2", _text("Sub."), parent="toolu_1"),
            _assistant("msg_2", _text("More."), parent="toolu_1"),
        )
    )

    assert (summary.messages, summary.subagent_messages) == (2, 1)
    assert summary.usage == TokenUsage()


def test_usage_sums_distinct_messages_including_subagents() -> None:
    summary = parse_stream(
        _stream(
            _assistant("msg_1", _text("One."), usage=_usage(10, 30, 100, 20)),
            _assistant("msg_2", _text("Two."), usage=_usage(3, 7, 130, 0)),
            _assistant("msg_3", _text("Sub."), usage=_usage(1, 2, 0, 50), model="claude-haiku-4-5", parent="toolu_9"),
        )
    )

    assert summary.messages == 3
    assert summary.subagent_messages == 1
    assert summary.usage == TokenUsage(input_tokens=14, output_tokens=39, cache_read_tokens=230, cache_write_tokens=70)
    assert summary.usage.total_input == 314


def test_subagent_text_and_model_never_become_the_answer() -> None:
    summary = parse_stream(
        _stream(
            _assistant("msg_0", _text("Synthetic."), model="<synthetic>"),
            _assistant("msg_1", _text("Main answer.")),
            _assistant("msg_2", _text("Subagent notes."), model="claude-haiku-4-5", parent="toolu_1"),
        )
    )

    assert summary.model == "claude-sonnet-5"
    assert summary.final_text == "Main answer."


def test_final_text_prefers_the_result_event_over_assistant_text() -> None:
    summary = parse_stream(_stream(_assistant("msg_1", _text("Draft.")), _result("Final.")))

    assert summary.final_text == "Final."


def test_tool_calls_are_counted_once_per_tool_use_id() -> None:
    call = _tool_use("toolu_1", "mcp__tasque2__memory_recall", query="budget")
    summary = parse_stream(
        _stream(
            _assistant("msg_1", call),
            _assistant("msg_1", call),
            _assistant("msg_2", _tool_use("toolu_2", "mcp__tasque2__memory_recall"), _tool_use("toolu_3", "Bash")),
        )
    )

    assert summary.tool_calls == {"mcp__tasque2__memory_recall": 2, "Bash": 1}
    assert summary.usage_record()["tool_calls"] == {"mcp__tasque2__memory_recall": 2, "Bash": 1}


def test_mcp_servers_that_did_not_connect_are_reported() -> None:
    summary = parse_stream(
        _stream(
            _init(
                ("tasque2", "connected"),
                ("autopilot", "failed"),
                ("openart", "pending"),
                ("google-workspace", "needs-auth"),
            )
        )
    )

    assert summary.failed_mcp_servers == ["autopilot", "google-workspace"]


def test_tool_errors_are_recorded_under_the_calling_tool() -> None:
    summary = parse_stream(
        _stream(
            _assistant("msg_1", _tool_use("toolu_1", "mcp__tasque2__submit_worker_result")),
            _tool_error("toolu_1", "result_token is required.\nTry again."),
            _tool_error("toolu_unknown", [{"type": "text", "text": "boom"}]),
            _tool_error("toolu_1", "x" * 500),
        )
    )

    failures = summary.mcp_tool_failures
    assert failures[0] == {
        "tool": "mcp__tasque2__submit_worker_result",
        "error": "result_token is required. Try again.",
    }
    assert failures[1]["tool"] == "unknown"
    assert "boom" in failures[1]["error"]
    assert len(failures[2]["error"]) <= 300


def test_error_result_sets_the_error() -> None:
    limited = parse_stream(
        _stream({"type": "result", "subtype": "success", "is_error": True, "result": "Claude AI usage limit reached"})
    )
    no_text = parse_stream(_stream({"type": "result", "subtype": "error_max_turns", "is_error": True}))

    assert limited.error == "Claude AI usage limit reached"
    assert no_text.error == "error_max_turns"


def test_api_error_message_sets_the_error() -> None:
    event = _assistant("msg_1", _text("API Error: 529 overloaded"), model="<synthetic>")
    event.update({"error": "overloaded", "is_api_error_message": True})

    assert parse_stream(_stream(event)).error == "API Error: 529 overloaded"


def test_usage_record_carries_counts_and_reported_fields() -> None:
    summary = parse_stream(
        _stream(
            _assistant("msg_1", _tool_use("toolu_1", "Bash"), usage=_usage(5, 6, 7, 8)),
            _assistant("msg_2", _text("Sub."), usage=_usage(1, 1), parent="toolu_1"),
            _result(total_cost_usd=0.25, num_turns=2, duration_ms=900),
        )
    )

    assert summary.usage_record() == {
        "input_tokens": 6,
        "output_tokens": 7,
        "cache_read_tokens": 7,
        "cache_write_tokens": 8,
        "messages": 2,
        "subagent_messages": 1,
        "total_cost_usd": 0.25,
        "num_turns": 2,
        "duration_ms": 900,
        "tool_calls": {"Bash": 1},
    }


def test_usage_record_omits_what_the_stream_did_not_report() -> None:
    assert parse_stream(_stream(_assistant("msg_1", _text("Hi.")))).usage_record() == {
        "input_tokens": 0,
        "output_tokens": 0,
        "cache_read_tokens": 0,
        "cache_write_tokens": 0,
        "messages": 1,
    }


def test_plain_text_output_is_the_final_text() -> None:
    summary = parse_stream("  just some words\n")

    assert summary.events == []
    assert summary.final_text == "just some words"


def test_iter_json_objects_reads_lines_arrays_and_fenced_json() -> None:
    assert iter_json_objects('{"a": 1}\nnot json\n[1, 2]\n{"b": 2}\n') == [{"a": 1}, {"b": 2}]
    assert iter_json_objects('[{"a": 1}, 2, {"b": 2}]') == [{"a": 1}, {"b": 2}]
    assert iter_json_objects('```json\n{"a": 1}\n```') == [{"a": 1}]
    assert iter_json_objects("") == []


def test_codex_events_are_parsed() -> None:
    summary = parse_stream(
        _stream(
            {"type": "thread.started", "thread_id": "thread-1"},
            {"type": "turn.started"},
            {"type": "item.completed", "item": {"type": "command_execution", "command": "ls", "exit_code": 0}},
            {
                "type": "item.completed",
                "item": {"type": "mcp_tool_call", "server": "tasque2", "tool": "memory_recall", "status": "completed"},
            },
            {
                "type": "item.completed",
                "item": {
                    "type": "mcp_tool_call",
                    "server": "tasque2",
                    "tool": "submit_worker_result",
                    "status": "failed",
                    "error": {"message": "user cancelled MCP tool call"},
                },
            },
            {"type": "item.completed", "item": {"type": "agent_message", "text": "All done."}},
            {
                "type": "turn.completed",
                "usage": {"input_tokens": 1000, "cached_input_tokens": 800, "output_tokens": 50},
            },
        )
    )

    assert summary.session_id == "thread-1"
    assert summary.final_text == "All done."
    assert summary.tool_calls == {
        "command_execution": 1,
        "mcp__tasque2__memory_recall": 1,
        "mcp__tasque2__submit_worker_result": 1,
    }
    assert summary.mcp_tool_failures == [{"tool": "submit_worker_result", "error": "user cancelled MCP tool call"}]
    assert summary.usage == TokenUsage(input_tokens=200, output_tokens=50, cache_read_tokens=800)
    assert summary.messages == 1


def test_codex_errors_use_the_event_message() -> None:
    message = "Codex ran out of room in the model's context window. Start a new thread."

    assert parse_stream(_stream({"type": "error", "message": message})).error == message
    assert parse_stream(_stream({"type": "turn.failed", "error": {"message": message}})).error == message
    assert parse_stream(_stream({"type": "error", "error": "unknown", "message": message})).error == message


def test_render_trace_markdown_summarizes_a_claude_run() -> None:
    summary = parse_stream(
        _stream(
            _init(("tasque2", "connected"), ("autopilot", "failed")),
            _assistant(
                "msg_1", _text("Checking memory."), _tool_use("toolu_1", "mcp__tasque2__memory_recall", query="x")
            ),
            _tool_error("toolu_1", "database is locked"),
            _assistant("msg_2", _text("Subagent says hi."), parent="toolu_1"),
            {"type": "result", "subtype": "error_during_execution", "is_error": True, "result": "Gave up."},
        )
    )

    trace = render_trace_markdown(summary, status="failed", exit_code=1)

    assert trace.startswith("# Provider trace\n\n- status: failed\n- session: sess-1\n- model: claude-sonnet-5")
    assert "- exit_code: 1" in trace
    assert '"messages": 2' in trace
    assert "- mcp servers not connected: autopilot" in trace
    assert "- says: Checking memory." in trace
    assert '- calls `mcp__tasque2__memory_recall` {"query": "x"}' in trace
    assert "- tool error: database is locked" in trace
    assert "  - subagent says: Subagent says hi." in trace
    assert "- result (error_during_execution): Gave up." in trace


def test_render_trace_markdown_summarizes_a_codex_run() -> None:
    summary = parse_stream(
        _stream(
            {"type": "item.completed", "item": {"type": "agent_message", "text": "Looking."}},
            {
                "type": "item.completed",
                "item": {
                    "type": "mcp_tool_call",
                    "tool": "memory_recall",
                    "status": "failed",
                    "arguments": {"query": "x"},
                    "error": {"message": "nope"},
                },
            },
            {
                "type": "item.completed",
                "item": {"type": "command_execution", "status": "completed", "exit_code": 0, "command": "ls"},
            },
            {"type": "turn.failed", "error": {"message": "context window exceeded"}},
        )
    )

    trace = render_trace_markdown(summary, status="failed", exit_code=None)

    assert "exit_code" not in trace
    assert "- says: Looking." in trace
    assert '- mcp `memory_recall` failed {"query": "x"} error={"message": "nope"}' in trace
    assert "- command completed exit=0: `ls`" in trace
    assert "- error: context window exceeded" in trace


def test_render_trace_markdown_is_empty_without_events() -> None:
    assert render_trace_markdown(parse_stream("plain output"), status="succeeded", exit_code=0) == ""


def test_strip_tool_call_xml_drops_a_truncated_parameter() -> None:
    leaked = (
        "Say yes and I'll do it, or no.\n"
        '<parameter name="produces">{"run": "run-20", "device_touched": false, '
        '"hinge_likes_remaining_today": 0'
    )

    cleaned = parse_stream(leaked).final_text

    assert cleaned == "Say yes and I'll do it, or no."


def test_strip_tool_call_xml_drops_a_complete_tool_call_block() -> None:
    leaked = (
        "Here is the reply.\n"
        '<function_calls><invoke name="submit_worker_result">'
        '<parameter name="report">x</parameter></invoke></function_calls>'
    )

    assert strip_tool_call_xml(leaked) == "Here is the reply."


def test_strip_tool_call_xml_drops_namespaced_markup() -> None:
    truncated = f'Say yes or no.\n<{NS}parameter name="produces">{{"run": "run-21"'
    complete = f'Reply.\n<{NS}function_calls><{NS}invoke name="x"></{NS}invoke></{NS}function_calls>'

    assert strip_tool_call_xml(truncated) == "Say yes or no."
    assert strip_tool_call_xml(complete) == "Reply."


def test_strip_tool_call_xml_leaves_clean_text_untouched() -> None:
    text = "A normal reply with < less-than but no tags."

    assert strip_tool_call_xml(text) == text
    assert parse_stream(text).final_text == text


def test_estimate_cost_prices_input_output_and_cache_tokens() -> None:
    usage = TokenUsage(
        input_tokens=1_000_000,
        output_tokens=1_000_000,
        cache_read_tokens=1_000_000,
        cache_write_tokens=1_000_000,
    )

    assert estimate_cost_usd("claude-sonnet-5", usage) == pytest.approx(2.0 + 10.0 + 0.2 + 4.0)


def test_list_price_matches_the_longest_model_prefix() -> None:
    assert list_price("claude-opus-5-5-20260901") == (4.0, 20.0)
    assert list_price("claude-opus-5-20260101") == (5.0, 25.0)
    assert list_price(" Claude-Sonnet-5 ") == (2.0, 10.0)


def test_unknown_or_missing_model_has_no_cost() -> None:
    usage = TokenUsage(input_tokens=10)

    assert estimate_cost_usd("gpt-9", usage) is None
    assert estimate_cost_usd(None, usage) is None
    assert list_price("") is None
