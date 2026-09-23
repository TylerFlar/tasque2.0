"""MCP server configuration for worker runs.

Every run gets the Tasque MCP server. A work item's ``runtime_contract.mcp_servers`` (or
``TASQUE2_DEFAULT_MCP_SERVERS``) names the user-scope servers from ``~/.claude.json`` it
may also load; with an allowlist the CLI loads nothing else.
"""

from __future__ import annotations

import json
import os
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from tasque2.config import get_settings
from tasque2.providers.base import ProviderExecutionError

TASQUE_MCP_SERVER_NAME = "tasque2"
_PASSTHROUGH_ENV = ("PYTHONPATH", "TASQUE2_TELEMETRY", "TASQUE2_LOG_LEVEL")


def tasque_mcp_server_config(
    *, work_item_id: str | None = None, extra_env: Mapping[str, str] | None = None
) -> dict[str, Any]:
    settings = get_settings()
    env = {
        "TASQUE2_DATA_DIR": str(settings.resolved_data_dir),
        "TASQUE2_DB_PATH": str(settings.database_path),
        "TASQUE2_EXTENSIONS_DIR": str(settings.resolved_extensions_dir),
        "TASQUE2_TIMEZONE": settings.timezone,
        "TASQUE2_PROJECT_DIR": str(settings.resolved_project_dir),
    }
    if work_item_id:
        env["TASQUE2_WORK_ITEM_ID"] = work_item_id
    for name in _PASSTHROUGH_ENV:
        if os.environ.get(name):
            env[name] = os.environ[name]
    for name, value in os.environ.items():
        if name.startswith("OTEL_") and name not in {"OTEL_SERVICE_NAME", "OTEL_RESOURCE_ATTRIBUTES"}:
            env[name] = value
    env.update(extra_env or {})
    return {"command": sys.executable, "args": ["-m", "tasque2.mcp"], "env": env}


def claude_mcp_config(
    *,
    work_item_id: str | None,
    servers: Sequence[str] | None,
    extra_env: Mapping[str, str] | None = None,
) -> str:
    """The ``--mcp-config`` JSON: Tasque plus the allowlisted user-scope servers."""
    configured: dict[str, Any] = {
        TASQUE_MCP_SERVER_NAME: tasque_mcp_server_config(work_item_id=work_item_id, extra_env=extra_env)
    }
    if servers is not None:
        available = user_scope_mcp_servers()
        for name in servers:
            if name == TASQUE_MCP_SERVER_NAME:
                continue
            server = available.get(name)
            if server is None:
                known = ", ".join(sorted(available)) or "(none)"
                raise ProviderExecutionError(
                    f"runtime_contract.mcp_servers names {name!r}, which is not a user-scope MCP server. "
                    f"Known: {known}."
                )
            configured[name] = server
    return json.dumps({"mcpServers": configured})


def user_scope_mcp_servers() -> dict[str, Any]:
    try:
        data = json.loads((Path.home() / ".claude.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    servers = data.get("mcpServers")
    return dict(servers) if isinstance(servers, dict) else {}


def mcp_server_allowlist(contract: Mapping[str, Any]) -> list[str] | None:
    """The contract's ``mcp_servers`` list, else the configured default, else None (inherit all)."""
    declared = contract.get("mcp_servers")
    if declared is None:
        return get_settings().default_mcp_server_list
    if not isinstance(declared, list) or not all(isinstance(item, str) for item in declared):
        raise ProviderExecutionError("runtime_contract.mcp_servers must be a list of strings.")
    return [item.strip() for item in declared if item.strip()]


def codex_mcp_args(*, work_item_id: str | None, extra_env: Mapping[str, str] | None = None) -> list[str]:
    config = tasque_mcp_server_config(work_item_id=work_item_id, extra_env=extra_env)
    pairs: list[tuple[str, Any]] = [
        (f"mcp_servers.{TASQUE_MCP_SERVER_NAME}.command", config["command"]),
        (f"mcp_servers.{TASQUE_MCP_SERVER_NAME}.args", config["args"]),
        (f"mcp_servers.{TASQUE_MCP_SERVER_NAME}.tool_timeout_sec", 24 * 60 * 60),
    ]
    pairs.extend(
        (f"mcp_servers.{TASQUE_MCP_SERVER_NAME}.env.{key}", value) for key, value in sorted(config["env"].items())
    )
    args: list[str] = []
    for key, value in pairs:
        args.extend(["-c", f"{key}={_toml_value(value)}"])
    return args


def _toml_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int | float):
        return str(value)
    return json.dumps(value)
