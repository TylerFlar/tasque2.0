"""The context packet: the state a worker starts from, assembled per run.

The packet carries the work item, its task context, pinned and relevant memories,
related artifacts, the workflow neighborhood, the parent work for replies, and any
code-computed domain digests an extension contributes. It is a starting map: workers
fetch anything else through the Tasque MCP tools.
"""

from __future__ import annotations

import inspect
import logging
from typing import Any

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from tasque2.extensions import registry as extension_registry
from tasque2.memory import MemoryService, canonical_budget
from tasque2.memory.excerpt import select_relevant_excerpt
from tasque2.models import Artifact, Memory, WorkAttempt, WorkflowEdge, WorkflowNode, WorkflowRun, WorkItem

logger = logging.getLogger(__name__)

MEMORY_CONTENT_CHARS = 2000
MEMORY_PINNED_CHARS = 12000
MEMORY_DECLARED_MAX_CHARS = 32000
MEMORY_SEARCH_HEADROOM = 5
PARENT_INSTRUCTION_CHARS = 6000
DEFAULT_LIMITS: dict[str, int | None] = {"memories": 24, "artifacts": 12}
LIMIT_KEYS = frozenset(DEFAULT_LIMITS)
HIDDEN_ARTIFACT_KINDS = ("provider_stream", "provider_bundle")
ROUTING_CONTEXT_KEYS = frozenset({"reply_followup_work", "reply_memory", "context_limits", "context_file"})
PACKET_CONFIG_KEYS = frozenset(
    {"memory_canonical_keys", "memory_queries", "memory_tags", "memory_query_limit", "memory_kinds"}
)


class WorkerContextBuilder:
    def __init__(self, session: Session) -> None:
        self.session = session

    def build_for_work(self, work_item: WorkItem, *, limits: dict[str, int | None] | None = None) -> dict[str, Any]:
        limits = limits if limits is not None else packet_limits(work_item)
        context = work_item.context or {}
        parent = self._parent_work(work_item)
        run = self.session.get(WorkflowRun, work_item.workflow_run_id) if work_item.workflow_run_id else None
        query = memory_query(work_item)
        packet: dict[str, Any] = {
            "work_item": _work_item_data(work_item),
            "task_context": {
                key: value
                for key, value in context.items()
                if key not in ROUTING_CONTEXT_KEYS and key not in PACKET_CONFIG_KEYS
            },
            "memories": [
                memory_data(memory, query=query, pinned=pinned)
                for memory, pinned in self._memories(work_item, query=query, limit=limits.get("memories"))
            ],
            "artifacts": [
                artifact_data(artifact)
                for artifact in self._artifacts(work_item, parent=parent, run=run, limit=limits.get("artifacts"))
            ],
        }
        if parent is not None:
            packet["parent_work"] = self._parent_data(
                parent, include_instruction=bool(context.get("reply_default_processor"))
            )
        if run is not None:
            packet["workflow"] = self._workflow_data(run, work_item)
        for key, wants, build in extension_registry().context_digests:
            if not wants(context):
                continue
            try:
                packet[key] = call_digest(build, self.session, context)
            except Exception:  # noqa: BLE001 - one failed digest must not fail the run
                logger.exception("Failed to compute digest %s for %s", key, work_item.id)
        return packet

    def _memories(self, work_item: WorkItem, *, query: str, limit: int | None) -> list[tuple[Memory, bool]]:
        """The lane's pinned documents (delivered whole) and then relevant memories (excerpted)."""
        if limit is not None and limit <= 0:
            return []
        service = MemoryService(self.session)
        context = work_item.context or {}
        namespaces = context_namespaces(context)
        default_namespace = namespaces[0] if namespaces else "global"
        collected: list[tuple[Memory, bool]] = []
        seen: set[str] = set()

        def add(memory: Memory | None, *, pinned: bool = True) -> bool:
            if memory is not None and memory.id not in seen:
                seen.add(memory.id)
                collected.append((memory, pinned))
            return limit is not None and len(collected) >= limit

        for spec in canonical_specs(context, namespaces):
            if add(service.get_canonical(namespace=spec["namespace"], canonical_key=spec["canonical_key"])):
                return collected
        for wants, resolve in extension_registry().canonical_key_resolvers:
            if not wants(context):
                continue
            try:
                keys = resolve(self.session, context) or []
            except Exception:  # noqa: BLE001 - a failed resolver skips its keys only
                logger.exception("Canonical key resolver failed")
                continue
            for key in keys:
                if add(service.get_canonical(namespace=default_namespace, canonical_key=str(key))):
                    return collected
        for namespace in namespaces or ["global"]:
            for kind in _string_list(context.get("memory_kinds")):
                for memory in service.list_active_by_kind(namespace=namespace, kind=kind):
                    if add(memory):
                        return collected
        for spec in query_specs(context, namespaces):
            remaining = None if limit is None else limit - len(collected)
            spec_limit = spec["limit"] if remaining is None else min(remaining, spec["limit"] or remaining)
            for memory in service.search(
                query=spec["query"], namespace=spec["namespace"], tags=spec["tags"], limit=spec_limit
            ):
                if add(memory, pinned=False):
                    return collected
        for namespace in [*namespaces, "global"]:
            remaining = None if limit is None else limit - len(collected)
            for memory in service.search(query=query, namespace=namespace, limit=remaining):
                if add(memory, pinned=False):
                    return collected
        return collected

    def _artifacts(
        self,
        work_item: WorkItem,
        *,
        parent: WorkItem | None,
        run: WorkflowRun | None,
        limit: int | None,
    ) -> list[Artifact]:
        explicit = context_artifact_ids(work_item.context or {})
        if parent is not None:
            parent_attempt = self._latest_attempt(parent.id)
            if parent_attempt is not None and parent_attempt.report_artifact_id:
                explicit.insert(0, parent_attempt.report_artifact_id)
        clauses = [Artifact.work_item_id == work_item.id]
        if parent is not None:
            clauses.append(Artifact.work_item_id == parent.id)
        if run is not None:
            clauses.append(Artifact.workflow_run_id == run.id)
        statement = (
            select(Artifact)
            .where(Artifact.archived_at.is_(None), Artifact.kind.not_in(HIDDEN_ARTIFACT_KINDS), or_(*clauses))
            .order_by(Artifact.created_at.desc())
        )
        if limit is not None:
            statement = statement.limit(max(0, limit))
        ordered: list[Artifact] = []
        seen: set[str] = set()
        for artifact in [self.session.get(Artifact, artifact_id) for artifact_id in explicit] + list(
            self.session.scalars(statement).all()
        ):
            if artifact is None or artifact.archived_at is not None or artifact.id in seen:
                continue
            seen.add(artifact.id)
            ordered.append(artifact)
        return ordered if limit is None else ordered[: max(limit, len(explicit))]

    def _parent_work(self, work_item: WorkItem) -> WorkItem | None:
        parent_id = parent_work_item_id(work_item.context or {})
        if not parent_id or parent_id == work_item.id:
            return None
        return self.session.get(WorkItem, parent_id)

    def _parent_data(self, parent: WorkItem, *, include_instruction: bool) -> dict[str, Any]:
        attempt = self._latest_attempt(parent.id)
        report = (
            self.session.get(Artifact, attempt.report_artifact_id) if attempt and attempt.report_artifact_id else None
        )
        data: dict[str, Any] = {
            "work_item": _work_item_data(parent),
            "latest_attempt": {
                "status": attempt.status,
                "summary": attempt.summary,
                "produces": attempt.produces or {},
                "error_message": attempt.error_message,
            }
            if attempt is not None
            else None,
            "report_artifact": artifact_data(report) if report is not None else None,
        }
        if include_instruction:
            instruction = parent.task_instruction or ""
            data["task_instruction"] = instruction[:PARENT_INSTRUCTION_CHARS]
            data["task_instruction_truncated"] = len(instruction) > PARENT_INSTRUCTION_CHARS
        return data

    def _workflow_data(self, run: WorkflowRun, work_item: WorkItem) -> dict[str, Any]:
        """The run, the current node, and the nodes it depends on (with their outputs)."""
        current = self.session.get(WorkflowNode, work_item.workflow_node_id) if work_item.workflow_node_id else None
        nodes: list[WorkflowNode] = []
        if current is not None:
            upstream_ids = [
                edge.from_node_id
                for edge in self.session.scalars(
                    select(WorkflowEdge).where(WorkflowEdge.to_node_id == current.id)
                ).all()
            ]
            if upstream_ids:
                nodes = list(
                    self.session.scalars(
                        select(WorkflowNode).where(WorkflowNode.id.in_(upstream_ids)).order_by(WorkflowNode.created_at)
                    ).all()
                )
        return {
            "id": run.id,
            "name": run.name,
            "status": run.status,
            "input": run.input or {},
            "current_node": _node_data(current, include_output=False) if current is not None else None,
            "nodes": [_node_data(node, include_output=True) for node in nodes],
        }

    def _latest_attempt(self, work_item_id: str) -> WorkAttempt | None:
        return self.session.scalar(
            select(WorkAttempt)
            .where(WorkAttempt.work_item_id == work_item_id)
            .order_by(WorkAttempt.attempt_number.desc(), WorkAttempt.created_at.desc())
        )


def packet_limits(work_item: WorkItem) -> dict[str, int | None]:
    """Default packet limits, sized to what the work item pins, then explicit overrides."""
    limits = dict(DEFAULT_LIMITS)
    adaptive = adaptive_memory_limit(work_item.context)
    if adaptive is not None:
        limits["memories"] = adaptive
    for source in (work_item.runtime_contract or {}, work_item.context or {}):
        configured = source.get("context_limits")
        if not isinstance(configured, dict):
            continue
        for key in LIMIT_KEYS & configured.keys():
            value = configured[key]
            if value is None:
                limits[key] = None
                continue
            try:
                limits[key] = max(0, int(value))
            except (TypeError, ValueError):
                raise ValueError(f"context_limits.{key} must be an integer or null.") from None
    return limits


def adaptive_memory_limit(context: dict[str, Any] | None) -> int | None:
    """The pinned canonical set plus a few search hits; None keeps the default.

    Contexts that force-load whole memory kinds keep the default so registers are never cut.
    """
    if not isinstance(context, dict) or context.get("memory_kinds"):
        return None
    specs = canonical_specs(context, context_namespaces(context))
    return len(specs) + MEMORY_SEARCH_HEADROOM if specs else None


def memory_data(memory: Memory, *, query: str = "", pinned: bool = True) -> dict[str, Any]:
    """A memory as the packet carries it: pinned documents whole (up to their budget), others excerpted."""
    budget = MEMORY_PINNED_CHARS if pinned else MEMORY_CONTENT_CHARS
    declared = canonical_budget(memory.content) if pinned else None
    if declared is not None and declared <= MEMORY_DECLARED_MAX_CHARS:
        budget = max(budget, declared)
    content, trimmed = select_relevant_excerpt(
        memory.content, query, budget_chars=budget, position_bias=0.5 if pinned else 0.0
    )
    data: dict[str, Any] = {
        "id": memory.id,
        "namespace": memory.namespace,
        "kind": memory.kind,
        "canonical_key": memory.canonical_key,
        "content": content,
        "tags": memory.tags or [],
        "pinned": memory.pinned,
        "updated_at": memory.updated_at.isoformat(timespec="minutes") if memory.updated_at else None,
    }
    if trimmed:
        data["content_chars"] = len(memory.content)
        data["content_compacted"] = True
    if declared is not None:
        data["max_chars"] = declared
        if len(memory.content) > declared:
            data["over_budget"] = True
            data["compact_this_run"] = (
                f"{memory.canonical_key} is {len(memory.content)} characters against its {declared} budget. "
                "Rewrite it inside the budget this run: drop what is stale, move detail into the ledger that "
                "owns it, keep the marker line."
            )
    return data


def call_digest(build: Any, session: Session, context: dict[str, Any]) -> Any:
    """Run a digest builder; a builder with a ``context`` parameter receives the work item's context."""
    try:
        parameters = inspect.signature(build).parameters
    except (TypeError, ValueError):
        return build(session)
    return build(session, context=context) if "context" in parameters else build(session)


def artifact_data(artifact: Artifact) -> dict[str, Any]:
    return {
        "id": artifact.id,
        "kind": artifact.kind,
        "title": artifact.title,
        "local_path": artifact.local_path,
        "content_type": artifact.content_type,
        "size_bytes": artifact.size_bytes,
        "tags": artifact.tags or [],
    }


def memory_query(work_item: WorkItem) -> str:
    """An OR query of the distinctive words in the title and instruction."""
    words = f"{work_item.title} {work_item.task_instruction}".split()
    tokens: list[str] = []
    for word in words:
        token = "".join(ch for ch in word.lower() if ch.isalnum() or ch == "_")
        if len(token) >= 4 and token not in tokens:
            tokens.append(token)
        if len(tokens) >= 8:
            break
    return " OR ".join(tokens) or work_item.title


def context_namespaces(context: dict[str, Any]) -> list[str]:
    raw: list[Any] = []
    for key in ("memory_namespace", "memory_namespaces"):
        value = context.get(key)
        if isinstance(value, list):
            raw.extend(value)
        elif value:
            raw.append(value)
    namespaces: list[str] = []
    for value in raw:
        namespace = str(value).strip()
        if namespace and namespace not in namespaces:
            namespaces.append(namespace)
    return namespaces


def canonical_specs(context: dict[str, Any], namespaces: list[str]) -> list[dict[str, str]]:
    default_namespace = namespaces[0] if namespaces else "global"
    specs: list[dict[str, str]] = []
    for item in _as_list(context.get("memory_canonical_keys")):
        if isinstance(item, str) and item.strip():
            specs.append({"namespace": default_namespace, "canonical_key": item.strip()})
        elif isinstance(item, dict):
            key = str(item.get("canonical_key") or item.get("key") or "").strip()
            namespace = str(item.get("namespace") or default_namespace).strip()
            if key and namespace:
                specs.append({"namespace": namespace, "canonical_key": key})
    return specs


def query_specs(context: dict[str, Any], namespaces: list[str]) -> list[dict[str, Any]]:
    namespaces = namespaces or ["global"]
    context_tags = _string_list(context.get("memory_tags"))
    context_limit = _optional_limit(context.get("memory_query_limit"))
    specs: list[dict[str, Any]] = []
    for item in _as_list(context.get("memory_queries")):
        if isinstance(item, str) and item.strip():
            specs.extend(
                {"query": item.strip(), "namespace": ns, "tags": context_tags, "limit": context_limit}
                for ns in namespaces
            )
        elif isinstance(item, dict) and str(item.get("query") or "").strip():
            item_namespaces = _string_list(item.get("namespaces"))
            if item.get("namespace"):
                item_namespaces.insert(0, str(item["namespace"]))
            for namespace in dict.fromkeys(item_namespaces or namespaces):
                specs.append(
                    {
                        "query": str(item["query"]).strip(),
                        "namespace": namespace,
                        "tags": _string_list(item.get("tags")) or context_tags,
                        "limit": _optional_limit(item.get("limit", context_limit)),
                    }
                )
    return specs


def context_artifact_ids(context: dict[str, Any]) -> list[str]:
    ids: list[str] = []
    for key in ("attachments", "input_artifacts", "artifact_ids", "related_artifacts"):
        value = context.get(key)
        for item in value if isinstance(value, list) else [value]:
            if isinstance(item, str):
                ids.append(item)
            elif isinstance(item, dict) and isinstance(item.get("artifact_id"), str):
                ids.append(item["artifact_id"])
    for holder in (context, context.get("source_reply") if isinstance(context.get("source_reply"), dict) else {}):
        report_id = holder.get("parent_report_artifact_id")
        if isinstance(report_id, str) and report_id not in ids:
            ids.insert(0, report_id)
    return ids


def parent_work_item_id(context: dict[str, Any]) -> str | None:
    value = context.get("parent_work_item_id")
    if isinstance(value, str) and value.strip():
        return value.strip()
    source_reply = context.get("source_reply")
    if isinstance(source_reply, dict):
        value = source_reply.get("parent_work_item_id")
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _work_item_data(work_item: WorkItem) -> dict[str, Any]:
    return {
        "id": work_item.id,
        "title": work_item.title,
        "lane": work_item.lane,
        "status": work_item.status,
        "attempt": work_item.attempt_count,
        "max_attempts": work_item.max_attempts,
        "source_kind": work_item.source_kind,
        "schedule_id": work_item.schedule_id,
        "workflow_run_id": work_item.workflow_run_id,
        "created_at": work_item.created_at.isoformat() if work_item.created_at else None,
    }


def _node_data(node: WorkflowNode, *, include_output: bool) -> dict[str, Any]:
    data: dict[str, Any] = {
        "node_key": node.node_key,
        "kind": node.kind,
        "status": node.status,
        "input": node.input or {},
    }
    if node.failure_reason:
        data["failure_reason"] = node.failure_reason
    if include_output:
        data["output"] = node.output or {}
    return data


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def _string_list(value: Any) -> list[str]:
    if isinstance(value, str):
        value = [value]
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _optional_limit(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return None
