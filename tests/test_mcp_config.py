from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from tasque2.config import get_settings, reset_settings
from tasque2.providers import ProviderExecutionError, mcp_config
from tasque2.providers.mcp_config import (
    claude_mcp_config,
    codex_mcp_args,
    mcp_server_allowlist,
    tasque_mcp_server_config,
    user_scope_mcp_servers,
)

TRACEPARENT = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"


@pytest.fixture()
def user_servers(monkeypatch: pytest.MonkeyPatch) -> dict:
    servers = {"autopilot": {"command": "ap"}, "blender": {"command": "bl"}, "tasque2": {"command": "stale"}}
    monkeypatch.setattr(mcp_config, "user_scope_mcp_servers", lambda: servers)
    return servers


def test_tasque_server_runs_this_interpreter_against_this_install(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PYTHONPATH", raising=False)
    settings = get_settings()

    config = tasque_mcp_server_config(work_item_id="work-1")

    assert config["command"] == sys.executable
    assert config["args"] == ["-m", "tasque2.mcp"]
    assert config["env"] == {
        "TASQUE2_DATA_DIR": str(settings.resolved_data_dir),
        "TASQUE2_DB_PATH": str(settings.database_path),
        "TASQUE2_EXTENSIONS_DIR": str(settings.resolved_extensions_dir),
        "TASQUE2_TIMEZONE": "America/Los_Angeles",
        "TASQUE2_PROJECT_DIR": str(settings.resolved_project_dir),
        "TASQUE2_WORK_ITEM_ID": "work-1",
    }


def test_tasque_server_env_passes_through_python_path_logging_telemetry_and_trace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PYTHONPATH", "C:/extra")
    monkeypatch.setenv("TASQUE2_TELEMETRY", "otlp")
    monkeypatch.setenv("TASQUE2_LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318")
    monkeypatch.setenv("OTEL_SERVICE_NAME", "tasque2")
    monkeypatch.setenv("OTEL_RESOURCE_ATTRIBUTES", "deployment.environment=home")

    env = tasque_mcp_server_config(extra_env={"TRACEPARENT": TRACEPARENT})["env"]

    assert env["PYTHONPATH"] == "C:/extra"
    assert env["TASQUE2_TELEMETRY"] == "otlp"
    assert env["TASQUE2_LOG_LEVEL"] == "DEBUG"
    assert env["OTEL_EXPORTER_OTLP_ENDPOINT"] == "http://localhost:4318"
    assert env["TRACEPARENT"] == TRACEPARENT
    assert "OTEL_SERVICE_NAME" not in env
    assert "OTEL_RESOURCE_ATTRIBUTES" not in env
    assert "TASQUE2_WORK_ITEM_ID" not in env


def test_claude_config_without_an_allowlist_holds_only_tasque() -> None:
    servers = json.loads(claude_mcp_config(work_item_id=None, servers=None))["mcpServers"]

    assert list(servers) == ["tasque2"]


def test_claude_config_adds_the_allowlisted_user_servers(user_servers: dict) -> None:
    servers = json.loads(claude_mcp_config(work_item_id="work-1", servers=["autopilot", "tasque2"]))["mcpServers"]

    assert set(servers) == {"tasque2", "autopilot"}
    assert servers["autopilot"] == {"command": "ap"}
    assert servers["tasque2"]["args"] == ["-m", "tasque2.mcp"]
    assert servers["tasque2"]["env"]["TASQUE2_WORK_ITEM_ID"] == "work-1"


def test_claude_config_rejects_an_unknown_server_and_names_the_known_ones(
    user_servers: dict, monkeypatch: pytest.MonkeyPatch
) -> None:
    with pytest.raises(ProviderExecutionError, match=r"names 'autopilto'.*Known: autopilot, blender, tasque2\."):
        claude_mcp_config(work_item_id=None, servers=["autopilto"])

    monkeypatch.setattr(mcp_config, "user_scope_mcp_servers", dict)
    with pytest.raises(ProviderExecutionError, match=r"Known: \(none\)\."):
        claude_mcp_config(work_item_id=None, servers=["autopilot"])


def test_user_scope_servers_come_from_the_home_claude_json(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(Path, "home", lambda: tmp_path)

    assert user_scope_mcp_servers() == {}
    (tmp_path / ".claude.json").write_text(
        json.dumps({"mcpServers": {"autopilot": {"command": "ap"}}}), encoding="utf-8"
    )
    assert user_scope_mcp_servers() == {"autopilot": {"command": "ap"}}


@pytest.mark.parametrize("content", ["not json", '{"mcpServers": ["autopilot"]}', "{}"])
def test_unusable_user_config_means_no_user_servers(
    content: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    (tmp_path / ".claude.json").write_text(content, encoding="utf-8")

    assert user_scope_mcp_servers() == {}


def test_allowlist_comes_from_the_contract_then_the_configured_default(monkeypatch: pytest.MonkeyPatch) -> None:
    assert mcp_server_allowlist({}) is None
    assert mcp_server_allowlist({"mcp_servers": []}) == []
    assert mcp_server_allowlist({"mcp_servers": ["autopilot", " openart ", " "]}) == ["autopilot", "openart"]

    monkeypatch.setenv("TASQUE2_DEFAULT_MCP_SERVERS", "autopilot, google-workspace")
    reset_settings()

    assert mcp_server_allowlist({}) == ["autopilot", "google-workspace"]
    assert mcp_server_allowlist({"mcp_servers": ["openart"]}) == ["openart"]
    assert mcp_server_allowlist({"mcp_servers": []}) == []


def test_blank_default_allowlist_inherits_every_server(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TASQUE2_DEFAULT_MCP_SERVERS", " ")
    reset_settings()

    assert mcp_server_allowlist({}) is None


@pytest.mark.parametrize("declared", ["autopilot", ["autopilot", 3], {"autopilot": True}])
def test_malformed_allowlist_is_rejected(declared: object) -> None:
    with pytest.raises(ProviderExecutionError, match="runtime_contract.mcp_servers must be a list of strings."):
        mcp_server_allowlist({"mcp_servers": declared})


def test_codex_args_encode_the_tasque_server_as_config_overrides() -> None:
    args = codex_mcp_args(work_item_id="work-1", extra_env={"TRACEPARENT": TRACEPARENT})

    assert args[0::2] == ["-c"] * (len(args) // 2)
    pairs = dict(item.split("=", 1) for item in args[1::2])
    assert pairs["mcp_servers.tasque2.command"] == json.dumps(sys.executable)
    assert pairs["mcp_servers.tasque2.args"] == '["-m", "tasque2.mcp"]'
    assert pairs["mcp_servers.tasque2.tool_timeout_sec"] == "86400"
    assert pairs["mcp_servers.tasque2.env.TASQUE2_WORK_ITEM_ID"] == '"work-1"'
    assert pairs["mcp_servers.tasque2.env.TRACEPARENT"] == json.dumps(TRACEPARENT)
