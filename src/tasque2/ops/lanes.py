"""Lane settings applied from a manifest: model tiers, lane-file bindings, and schedules on or off.

A lane's tier lives where its runs come from: a schedule's runtime contract, a workflow
node's contract (a fan-out node's contract applies to its children), and, for replies in a
Discord thread, the reply configuration of the work item replies there go to (the thread's
owner, or a workflow thread's reply owner). The manifest names each of these and the profile
it runs at::

    {
      "schedules": {"finance-daily": {"profile": "high", "context_file": "data/work-templates/finance/context.json"}},
      "workflows": {"daily-gmail-cleanup": {"report": "low"}},
      "threads": {"1520335407013826716": {"context_file": "data/work-templates/finance/context.json"}}
    }

Schedules are named by name or id, workflows by definition name (every version), nodes by
key, threads by Discord thread id. ``context_file`` binds a schedule or thread to its lane file
(see ``tasque2.lane_context``); a lane file then owns the reply configuration, including its
tier. ``keep_stored`` alongside it lists the only stored context keys to keep (``[]`` leaves the
lane file as the whole context). ``enabled`` switches a schedule on or off. ``mcp_servers`` and
``disallowed_tools`` set the lists a schedule's runs load and deny. ``reply_profile`` sets the
reply tier of a schedule or thread that has no lane file.
"""

from __future__ import annotations

import copy
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from tasque2.config import MODEL_PROFILES
from tasque2.discord.routing import DiscordService
from tasque2.lane_context import CONTEXT_FILE_KEY, load_context_file
from tasque2.models import DiscordThread, Schedule, WorkflowDefinition, WorkItem
from tasque2.schedules import ScheduleService


@dataclass(frozen=True)
class LaneChange:
    target: str
    field: str
    old: str | None
    new: str | None

    @property
    def changed(self) -> bool:
        return self.old != self.new


CONTRACT_LISTS = ("mcp_servers", "disallowed_tools")


class LaneManifestError(ValueError):
    pass


def load_manifest(path: Path) -> dict[str, Any]:
    manifest = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(manifest, dict):
        raise LaneManifestError("The manifest must be a JSON object.")
    return manifest


def apply_lane_tiers(session: Session, manifest: dict[str, Any], *, dry_run: bool = False) -> list[LaneChange]:
    """Apply every setting the manifest names; returns one row per setting, changed or not."""
    changes: list[LaneChange] = []
    for name, spec in (manifest.get("schedules") or {}).items():
        changes.extend(_schedule(session, name, spec, dry_run=dry_run))
    for name, nodes in (manifest.get("workflows") or {}).items():
        changes.extend(_workflow(session, name, nodes, dry_run=dry_run))
    for thread_id, spec in (manifest.get("threads") or {}).items():
        changes.extend(_thread(session, str(thread_id), spec, dry_run=dry_run))
    if not dry_run:
        session.flush()
    return changes


def _schedule(session: Session, name: str, spec: dict[str, Any], *, dry_run: bool) -> list[LaneChange]:
    schedule = session.scalar(select(Schedule).where(or_(Schedule.id == name, Schedule.name == name)))
    if schedule is None:
        raise LaneManifestError(f"Unknown schedule: {name}")
    target = f"schedule {schedule.name}"
    changes: list[LaneChange] = []
    if "enabled" in spec:
        enabled = bool(spec["enabled"])
        changes.append(LaneChange(target, "enabled", str(schedule.enabled).lower(), str(enabled).lower()))
        if not dry_run and enabled and not schedule.enabled:
            ScheduleService(session).enable_schedule(schedule.id)
        elif not dry_run and not enabled and schedule.enabled:
            ScheduleService(session).disable_schedule(schedule.id)
    contract = copy.deepcopy(schedule.runtime_contract or {})
    if "profile" in spec:
        changes.append(_set_profile(target, "run", contract, spec["profile"]))
    for key in CONTRACT_LISTS:
        if key in spec:
            changes.append(_set_list(target, key, contract, spec[key]))
    if not dry_run and contract != (schedule.runtime_contract or {}):
        schedule.runtime_contract = contract
    payload = copy.deepcopy(schedule.payload or {})
    context = payload.setdefault("context", {})
    if "context_file" in spec:
        changes.append(_bind_context_file(target, context, spec["context_file"], spec.get("keep_stored")))
    if "reply_profile" in spec:
        if context.get(CONTEXT_FILE_KEY):
            raise LaneManifestError(f"{target}: its lane file owns the reply tier; set it there.")
        reply = context.get("reply_followup_work")
        if not isinstance(reply, dict):
            raise LaneManifestError(f"Schedule {schedule.name} has no reply configuration to set.")
        changes.append(_set_profile(target, "reply", reply.setdefault("runtime_contract", {}), spec["reply_profile"]))
    if not dry_run and ("context_file" in spec or "reply_profile" in spec):
        schedule.payload = payload
    return changes


def _workflow(session: Session, name: str, nodes: dict[str, str], *, dry_run: bool) -> list[LaneChange]:
    definitions = session.scalars(select(WorkflowDefinition).where(WorkflowDefinition.name == name)).all()
    if not definitions:
        raise LaneManifestError(f"Unknown workflow: {name}")
    changes: list[LaneChange] = []
    for definition in definitions:
        body = copy.deepcopy(definition.definition or {})
        by_key = {node.get("key"): node for node in body.get("nodes", []) if isinstance(node, dict)}
        for key, profile in nodes.items():
            node = by_key.get(key)
            if node is None:
                raise LaneManifestError(f"Workflow {name} has no node {key!r}.")
            contract = node.setdefault("runtime_contract", {})
            changes.append(_set_profile(f"workflow {name}@{definition.version}", key, contract, profile))
        if not dry_run:
            definition.definition = body
    return changes


def _thread(session: Session, thread_id: str, spec: dict[str, Any], *, dry_run: bool) -> list[LaneChange]:
    binding = session.scalar(select(DiscordThread).where(DiscordThread.discord_thread_id == thread_id))
    owner = None
    if binding is not None and binding.work_item_id:
        owner = session.get(WorkItem, binding.work_item_id)
    elif binding is not None and binding.workflow_run_id:
        owner = DiscordService(session).workflow_reply_owner(binding.workflow_run_id)
    if owner is None:
        raise LaneManifestError(f"Thread {thread_id} has no work item that replies there go to.")
    target = f"thread {thread_id} ({owner.title})"
    context = copy.deepcopy(owner.context or {})
    changes: list[LaneChange] = []
    if "context_file" in spec:
        changes.append(_bind_context_file(target, context, spec["context_file"], spec.get("keep_stored")))
    if "reply_profile" in spec:
        if context.get(CONTEXT_FILE_KEY):
            raise LaneManifestError(f"{target}: its lane file owns the reply tier; set it there.")
        reply = context.get("reply_followup_work")
        if not isinstance(reply, dict):
            raise LaneManifestError(f"{target} has no reply configuration to set.")
        changes.append(_set_profile(target, "reply", reply.setdefault("runtime_contract", {}), spec["reply_profile"]))
    if not dry_run:
        owner.context = context
    return changes


def _bind_context_file(target: str, context: dict[str, Any], reference: str, keep: Any = None) -> LaneChange:
    """Point a stored context at a lane file, dropping the stored keys the file now owns.

    With ``keep`` (a list of key names), every other stored key is dropped too, so the lane
    file is the context's only source apart from the keys kept.
    """
    try:
        lane = load_context_file(reference)
    except (OSError, ValueError) as exc:
        raise LaneManifestError(f"{target}: lane file {reference} is unusable ({exc}).") from None
    if keep is not None and not (isinstance(keep, list) and all(isinstance(key, str) for key in keep)):
        raise LaneManifestError(f"{target}: keep_stored must be a list of key names.")
    old = context.get(CONTEXT_FILE_KEY)
    for key in list(context):
        if key in lane or (keep is not None and key not in keep):
            context.pop(key)
    context[CONTEXT_FILE_KEY] = reference
    return LaneChange(target=target, field="context", old=old, new=reference)


def _set_list(target: str, key: str, contract: dict[str, Any], value: Any) -> LaneChange:
    if not isinstance(value, list) or not all(isinstance(item, str) and item.strip() for item in value):
        raise LaneManifestError(f"{target}: {key} must be a list of names.")
    old = contract.get(key)
    contract[key] = list(value)
    return LaneChange(
        target=target,
        field=key,
        old=(", ".join(old) or "none") if isinstance(old, list) else None,
        new=", ".join(value) or "none",
    )


def _set_profile(target: str, field: str, contract: dict[str, Any], profile: str | None) -> LaneChange:
    if profile is not None and profile not in MODEL_PROFILES:
        raise LaneManifestError(f"{target}: profile must be one of {', '.join(MODEL_PROFILES)}.")
    old = contract.get("model_profile")
    if profile is None:
        contract.pop("model_profile", None)
    else:
        contract["model_profile"] = profile
    return LaneChange(target=target, field=field, old=old, new=profile)
