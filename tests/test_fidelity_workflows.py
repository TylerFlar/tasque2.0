from __future__ import annotations

import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def _read_custom_workflow_file(relative_path: str) -> str:
    path = ROOT / relative_path
    if not path.exists():
        pytest.skip(f"local custom workflow file is not present: {relative_path}")
    return path.read_text(encoding="utf-8")


def test_fidelity_workflows_do_not_split_auth_preflight_from_snapshot() -> None:
    workflow = json.loads(_read_custom_workflow_file("data/workflows/fidelity-daily-autotrading.workflow.json"))
    nodes = workflow["definition"]["nodes"]
    node_keys = {node["key"] for node in nodes}

    assert "fidelity_auth_preflight" not in node_keys
    for node in nodes:
        assert "fidelity_auth_preflight" not in node.get("depends_on", [])
        assert node.get("task_template_path") != "../work-templates/fidelity-auth-preflight.template.md"


def test_fidelity_snapshot_templates_own_login_repair() -> None:
    content = _read_custom_workflow_file(
        "data/work-templates/fidelity-daily-autotrading/account-snapshot.template.md"
    )

    assert "fidelity_login" in content
    assert "same worker" in content
    assert "auth_recovery" in content
    assert "auth_preflight" not in content


def test_fidelity_daily_autotrading_uses_analysis_debate_manager_shape() -> None:
    workflow = json.loads(_read_custom_workflow_file("data/workflows/fidelity-daily-autotrading.workflow.json"))
    nodes = workflow["definition"]["nodes"]
    assert [node["key"] for node in nodes] == [
        "account_snapshot",
        "analytics",
        "researchers",
        "trader",
        "risk_management",
        "manager",
        "execute_orders",
        "daily_audit",
    ]
    assert all("pre_trade_risk_gate" not in node.get("depends_on", []) for node in nodes)
    manager = next(node for node in nodes if node["key"] == "manager")
    assert set(manager["depends_on"]) == {"account_snapshot", "risk_management"}
