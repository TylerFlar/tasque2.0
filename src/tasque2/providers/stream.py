"""Parse provider JSON event streams (Claude Code ``stream-json`` and Codex ``--json``).

The parser extracts the session id, final text, errors, MCP failures, tool calls, and
token usage. Claude Code reports usage per model message and repeats it for every content
block of that message, so usage is aggregated per message id; subagent messages carry a
``parent_tool_use_id`` and are counted separately.
"""

from __future__ import annotations

import json
import re
from collections import Counter
from dataclasses import dataclass, field
from typing import Any

from tasque2.text import one_line

_OK_SERVER_STATUSES = {"connected", "pending"}
_TOOL_CALL_BLOCK_RE = re.compile(r"<(?:antml:)?function_calls>.*?</(?:antml:)?function_calls>", re.DOTALL)
_TOOL_CALL_INVOKE_RE = re.compile(r"<(?:antml:)?invoke\b.*?</(?:antml:)?invoke>", re.DOTALL)
_TOOL_CALL_PARAM_RE = re.compile(r"<(?:antml:)?parameter\b.*?</(?:antml:)?parameter>", re.DOTALL)
_TOOL_CALL_NS = "antml" + ":"
_TOOL_CALL_OPENERS = tuple(
    f"<{prefix}{name}" for name in ("function_calls>", "invoke", "parameter") for prefix in ("", _TOOL_CALL_NS)
)


@dataclass
class TokenUsage:
    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_tokens: int = 0
    cache_write_tokens: int = 0

    @property
    def total_input(self) -> int:
        return self.input_tokens + self.cache_read_tokens + self.cache_write_tokens

    def as_dict(self) -> dict[str, int]:
        return {
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "cache_read_tokens": self.cache_read_tokens,
            "cache_write_tokens": self.cache_write_tokens,
        }


@dataclass
class StreamSummary:
    events: list[dict[str, Any]] = field(default_factory=list)
    session_id: str | None = None
    model: str | None = None
    final_text: str = ""
    error: str | None = None
    usage: TokenUsage = field(default_factory=TokenUsage)
    reported_cost_usd: float | None = None
    num_turns: int | None = None
    duration_ms: int | None = None
    messages: int = 0
    subagent_messages: int = 0
    tool_calls: Counter[str] = field(default_factory=Counter)
    mcp_tool_failures: list[dict[str, str]] = field(default_factory=list)
    failed_mcp_servers: list[str] = field(default_factory=list)
    has_result_event: bool = False
    tool_names: dict[str, str] = field(default_factory=dict)

    def usage_record(self) -> dict[str, Any]:
        record: dict[str, Any] = {**self.usage.as_dict(), "messages": self.messages}
        if self.subagent_messages:
            record["subagent_messages"] = self.subagent_messages
        if self.reported_cost_usd is not None:
            record["total_cost_usd"] = self.reported_cost_usd
        if self.num_turns is not None:
            record["num_turns"] = self.num_turns
        if self.duration_ms is not None:
            record["duration_ms"] = self.duration_ms
        if self.tool_calls:
            record["tool_calls"] = dict(self.tool_calls.most_common())
        return record


def parse_stream(text: str) -> StreamSummary:
    summary = StreamSummary(events=iter_json_objects(text))
    message_usage: dict[str, dict[str, int]] = {}
    seen_tool_ids: set[str] = set()
    preferred_text: list[str] = []
    fallback_text: list[str] = []

    for event in summary.events:
        event_type = event.get("type")
        summary.session_id = _first_string(event, ("session_id", "sessionId", "thread_id")) or summary.session_id

        if event_type == "system" and event.get("subtype") == "init":
            summary.model = event.get("model") or summary.model
            servers = event.get("mcp_servers")
            if isinstance(servers, list):
                summary.failed_mcp_servers = [
                    str(server.get("name"))
                    for server in servers
                    if isinstance(server, dict)
                    and server.get("name")
                    and str(server.get("status") or "connected") not in _OK_SERVER_STATUSES
                ]
        elif event_type == "assistant":
            _read_assistant(event, summary, message_usage, seen_tool_ids, fallback_text)
        elif event_type == "user":
            _read_tool_results(event, summary)
        elif event_type == "result":
            summary.has_result_event = True
            result_text = event.get("result")
            if isinstance(result_text, str) and result_text.strip():
                preferred_text.append(result_text.strip())
                if event.get("is_error"):
                    summary.error = result_text.strip()
            elif event.get("is_error"):
                summary.error = str(event.get("subtype") or "error")
            if isinstance(event.get("total_cost_usd"), int | float):
                summary.reported_cost_usd = float(event["total_cost_usd"])
            if isinstance(event.get("num_turns"), int):
                summary.num_turns = event["num_turns"]
            if isinstance(event.get("duration_ms"), int):
                summary.duration_ms = event["duration_ms"]
        elif isinstance(event.get("item"), dict):
            _read_codex_item(event["item"], summary, preferred_text)
        elif event_type == "turn.completed" and isinstance(event.get("usage"), dict):
            usage = event["usage"]
            cached = int(usage.get("cached_input_tokens") or 0)
            summary.usage.input_tokens += max(0, int(usage.get("input_tokens") or 0) - cached)
            summary.usage.cache_read_tokens += cached
            summary.usage.output_tokens += int(usage.get("output_tokens") or 0)
            summary.messages += 1
        elif event_type in {"turn.failed", "error"}:
            summary.error = _error_message(event) or summary.error

    for usage in message_usage.values():
        summary.usage.input_tokens += usage.get("input_tokens", 0)
        summary.usage.output_tokens += usage.get("output_tokens", 0)
        summary.usage.cache_read_tokens += usage.get("cache_read_input_tokens", 0)
        summary.usage.cache_write_tokens += usage.get("cache_creation_input_tokens", 0)

    if preferred_text:
        summary.final_text = strip_tool_call_xml(preferred_text[-1])
    elif fallback_text:
        summary.final_text = strip_tool_call_xml(fallback_text[-1])
    elif not summary.events:
        summary.final_text = strip_tool_call_xml(text.strip())
    return summary


def iter_json_objects(text: str) -> list[dict[str, Any]]:
    stripped = (text or "").strip()
    if not stripped:
        return []
    whole = _loads(stripped)
    if isinstance(whole, dict):
        return [whole]
    if isinstance(whole, list):
        return [item for item in whole if isinstance(item, dict)]
    objects: list[dict[str, Any]] = []
    for line in stripped.splitlines():
        parsed = _loads(line.strip())
        if isinstance(parsed, dict):
            objects.append(parsed)
    return objects


def strip_tool_call_xml(text: str) -> str:
    """Remove tool-call markup a model echoed as text, including a truncated trailing call."""
    if not text or "<" not in text:
        return text
    text = _TOOL_CALL_BLOCK_RE.sub("", text)
    text = _TOOL_CALL_INVOKE_RE.sub("", text)
    text = _TOOL_CALL_PARAM_RE.sub("", text)
    cuts = [index for index in (text.find(marker) for marker in _TOOL_CALL_OPENERS) if index != -1]
    if cuts:
        text = text[: min(cuts)]
    return text.strip()


def render_trace_markdown(summary: StreamSummary, *, status: str, exit_code: int | None) -> str:
    """A compact, readable trace of one provider run for debugging from Discord or disk."""
    if not summary.events:
        return ""
    lines = ["# Provider trace", "", f"- status: {status}"]
    if summary.session_id:
        lines.append(f"- session: {summary.session_id}")
    if summary.model:
        lines.append(f"- model: {summary.model}")
    if exit_code is not None:
        lines.append(f"- exit_code: {exit_code}")
    lines.append(f"- usage: {json.dumps(summary.usage_record(), sort_keys=True)}")
    if summary.failed_mcp_servers:
        lines.append(f"- mcp servers not connected: {', '.join(summary.failed_mcp_servers)}")
    lines += ["", "## Events"]
    for event in summary.events:
        line = _trace_line(event)
        if line:
            lines.append(line)
    return "\n".join(lines).strip()


def _read_assistant(
    event: dict[str, Any],
    summary: StreamSummary,
    message_usage: dict[str, dict[str, int]],
    seen_tool_ids: set[str],
    fallback_text: list[str],
) -> None:
    message = event.get("message") if isinstance(event.get("message"), dict) else {}
    message_id = str(message.get("id") or "")
    usage = message.get("usage") if isinstance(message.get("usage"), dict) else None
    if message_id and message_id not in message_usage:
        message_usage[message_id] = {}
        summary.messages += 1
        if event.get("parent_tool_use_id"):
            summary.subagent_messages += 1
    if message_id and usage:
        current = message_usage[message_id]
        for key in ("input_tokens", "output_tokens", "cache_read_input_tokens", "cache_creation_input_tokens"):
            value = usage.get(key)
            if isinstance(value, int):
                current[key] = max(current.get(key, 0), value)
    if message.get("model") and not event.get("parent_tool_use_id") and message.get("model") != "<synthetic>":
        summary.model = summary.model or message.get("model")
    for block in message.get("content") or []:
        if not isinstance(block, dict):
            continue
        if block.get("type") == "tool_use":
            block_id = str(block.get("id") or "")
            if block_id and block_id in seen_tool_ids:
                continue
            seen_tool_ids.add(block_id)
            name = str(block.get("name") or "unknown")
            summary.tool_calls[name] += 1
            if block_id:
                summary.tool_names[block_id] = name
        elif block.get("type") == "text" and not event.get("parent_tool_use_id"):
            text = str(block.get("text") or "").strip()
            if text:
                fallback_text.append(text)
    if event.get("error") and event.get("is_api_error_message"):
        texts = [str(b.get("text") or "") for b in message.get("content") or [] if isinstance(b, dict)]
        summary.error = " ".join(texts).strip() or str(event.get("error"))


def _read_tool_results(event: dict[str, Any], summary: StreamSummary) -> None:
    message = event.get("message") if isinstance(event.get("message"), dict) else {}
    for block in message.get("content") or []:
        if isinstance(block, dict) and block.get("type") == "tool_result" and block.get("is_error"):
            content = block.get("content")
            text = content if isinstance(content, str) else json.dumps(content)
            tool = summary.tool_names.get(str(block.get("tool_use_id") or ""), "unknown")
            summary.mcp_tool_failures.append({"tool": tool, "error": one_line(text, limit=300)})


def _read_codex_item(item: dict[str, Any], summary: StreamSummary, preferred_text: list[str]) -> None:
    item_type = item.get("type")
    if item_type == "agent_message" and isinstance(item.get("text"), str):
        preferred_text.append(item["text"].strip())
    elif item_type == "mcp_tool_call":
        tool = str(item.get("tool") or "unknown")
        summary.tool_calls[f"mcp__{item.get('server') or 'unknown'}__{tool}"] += 1
        if item.get("status") == "failed":
            error = item.get("error")
            message = error.get("message") if isinstance(error, dict) else error
            summary.mcp_tool_failures.append({"tool": tool, "error": str(message or "unknown MCP failure")})
    elif item_type == "command_execution":
        summary.tool_calls["command_execution"] += 1


def _trace_line(event: dict[str, Any]) -> str | None:
    event_type = event.get("type")
    if event_type == "assistant":
        message = event.get("message") if isinstance(event.get("message"), dict) else {}
        prefix = "  - subagent" if event.get("parent_tool_use_id") else "-"
        parts = []
        for block in message.get("content") or []:
            if not isinstance(block, dict):
                continue
            if block.get("type") == "text" and str(block.get("text") or "").strip():
                parts.append(f"{prefix} says: {one_line(str(block['text']), limit=400)[:400]}")
            elif block.get("type") == "tool_use":
                parts.append(f"{prefix} calls `{block.get('name')}` {_compact(block.get('input'))}")
        return "\n".join(parts) or None
    if event_type == "user":
        message = event.get("message") if isinstance(event.get("message"), dict) else {}
        errors = [block for block in message.get("content") or [] if isinstance(block, dict) and block.get("is_error")]
        if errors:
            content = errors[0].get("content")
            return f"- tool error: {one_line(content if isinstance(content, str) else json.dumps(content), limit=300)}"
        return None
    if event_type == "result":
        return f"- result ({event.get('subtype')}): {one_line(str(event.get('result') or ''), limit=400)}"
    item = event.get("item")
    if isinstance(item, dict):
        item_type = item.get("type")
        if item_type == "agent_message":
            return f"- says: {one_line(str(item.get('text') or ''), limit=400)}"
        if item_type == "mcp_tool_call":
            detail = f"- mcp `{item.get('tool')}` {item.get('status')} {_compact(item.get('arguments'))}"
            return detail + (f" error={_compact(item.get('error'))}" if item.get("error") else "")
        if item_type == "command_execution":
            command = one_line(str(item.get("command") or ""), limit=300)
            return f"- command {item.get('status')} exit={item.get('exit_code')}: `{command}`"
    if event_type in {"turn.failed", "error"}:
        return f"- error: {_error_message(event)}"
    return None


def _error_message(event: dict[str, Any]) -> str | None:
    error = event.get("error")
    if isinstance(error, dict) and isinstance(error.get("message"), str):
        return error["message"].strip()
    if isinstance(error, str) and error.strip() and error.strip() != "unknown":
        return error.strip()
    message = event.get("message")
    return message.strip() if isinstance(message, str) and message.strip() else None


def _first_string(event: dict[str, Any], keys: tuple[str, ...]) -> str | None:
    for key in keys:
        value = event.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def _compact(value: Any) -> str:
    if value is None:
        return ""
    try:
        text = json.dumps(value, ensure_ascii=True, sort_keys=True)
    except TypeError:
        text = str(value)
    return text[:300]


def _loads(text: str) -> Any | None:
    if not text:
        return None
    stripped = text.strip()
    if stripped.startswith("```"):
        lines = stripped.splitlines()
        if len(lines) >= 3 and lines[-1].strip() == "```":
            stripped = "\n".join(lines[1:-1]).strip()
    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        return None
