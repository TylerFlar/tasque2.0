from __future__ import annotations

import json
import logging
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from tasque2.extensions import registry as extension_registry
from tasque2.memory import MemoryService
from tasque2.models import (
    Artifact,
    Memory,
    WorkAttempt,
    WorkEvent,
    WorkflowNode,
    WorkflowRun,
    WorkItem,
)
from tasque2.retrieval import select_relevant_excerpt

logger = logging.getLogger(__name__)

# Per-memory delivery budgets. Pinned/canonical ledgers (the durable source of
# truth a worker ranks against) get a far larger budget than the old 1600 so
# they no longer arrive as a head+tail fragment; the genuinely huge ones are
# relevance-excerpted (see tasque2.retrieval) rather than sliced.
MEMORY_CONTEXT_CONTENT_CHARS = 2000
MEMORY_CONTEXT_PINNED_CHARS = 12000


class WorkerContextBuilder:
    def __init__(self, session: Session) -> None:
        self.session = session

    def build_for_work(
        self,
        work_item: WorkItem,
        *,
        limits: dict[str, int | None] | None = None,
    ) -> dict[str, Any]:
        limits = limits or {}
        parent_work = self._parent_work(work_item)
        workflow_run = (
            self.session.get(WorkflowRun, work_item.workflow_run_id) if work_item.workflow_run_id else None
        )

        query = _memory_query(work_item)
        memories = self._memories(
            query=query,
            work_item=work_item,
            limit=limits.get("memories"),
        )
        artifacts = self._artifacts_for_work(
            work_item,
            parent_work=parent_work,
            workflow_run=workflow_run,
            limit=limits.get("artifacts"),
        )

        packet = {
            "version": 1,
            "work_item": _work_item_data(work_item),
            "task_context": work_item.context or {},
            "parent_work": self._parent_work_data(
                parent_work,
                event_limit=limits.get("events"),
                artifact_limit=limits.get("artifacts"),
            ),
            "workflow": self._workflow_data(
                workflow_run,
                work_item=work_item,
                limit=limits.get("workflow_nodes"),
            ),
            "memories": [_memory_data(memory, query=query) for memory in memories],
            "artifacts": [_artifact_data(artifact) for artifact in artifacts],
            "recent_events": [
                _event_data(event)
                for event in self._events_for_work(
                    work_item,
                    limit=limits.get("events"),
                )
            ],
        }
        # Code-computed domain digests (extension-registered) injected into
        # matching context packets. Each is derived from an append-only ledger
        # table the worker writes through a validated MCP tool — ground truth
        # the worker reads but cannot rewrite, unlike its canonical memories.
        for digest_key, wants, build in extension_registry().context_digests:
            if not wants(work_item.context or {}):
                continue
            try:
                packet[digest_key] = build(self.session)
            except Exception:  # noqa: BLE001 - context assembly must not fail the run
                logger.exception("Failed to compute %s for %s", digest_key, work_item.id)
        return packet

    def _memories(
        self,
        *,
        query: str,
        work_item: WorkItem,
        limit: int | None,
    ) -> list[Memory]:
        if limit is not None and limit <= 0:
            return []

        service = MemoryService(self.session)
        context = work_item.context or {}
        memories: list[Memory] = []

        def add(memory: Memory | None) -> None:
            if memory is None:
                return
            if memory.id in {existing.id for existing in memories}:
                return
            memories.append(memory)

        explicit_namespaces = _context_memory_namespaces(context)
        default_namespaces = list(explicit_namespaces)

        for spec in _memory_canonical_specs(context, default_namespaces):
            add(
                service.get_canonical(
                    namespace=spec["namespace"],
                    canonical_key=spec["canonical_key"],
                )
            )
            if _limit_reached(memories, limit):
                return _trim_to_limit(memories, limit)

        # Force-load complete structured registers (e.g. every `interest` record) so the
        # worker always checks the full set, not whatever a fuzzy search happens to return.
        for namespace in default_namespaces or ["global"]:
            for kind in _context_memory_kinds(context):
                for memory in service.list_active_by_kind(namespace=namespace, kind=kind):
                    add(memory)
                    if _limit_reached(memories, limit):
                        return _trim_to_limit(memories, limit)

        for spec in _memory_query_specs(context, default_namespaces):
            search_limit = _smaller_limit(spec["limit"], _remaining_limit(limit, len(memories)))
            for memory in service.search(
                query=spec["query"],
                namespace=spec["namespace"],
                tags=spec["tags"],
                limit=search_limit,
            ):
                add(memory)
                if _limit_reached(memories, limit):
                    return _trim_to_limit(memories, limit)

        for namespace in explicit_namespaces:
            for memory in service.search(
                query=query,
                namespace=namespace,
                limit=_remaining_limit(limit, len(memories)),
            ):
                add(memory)
                if _limit_reached(memories, limit):
                    return _trim_to_limit(memories, limit)

        if not _limit_reached(memories, limit):
            for memory in service.search(
                query=query,
                namespace="global",
                limit=_remaining_limit(limit, len(memories)),
            ):
                add(memory)
        return _trim_to_limit(memories, limit)

    def _artifacts_for_work(
        self,
        work_item: WorkItem,
        *,
        parent_work: WorkItem | None,
        workflow_run: WorkflowRun | None,
        limit: int | None,
    ) -> list[Artifact]:
        clauses = [Artifact.work_item_id == work_item.id]
        if parent_work is not None:
            clauses.append(Artifact.work_item_id == parent_work.id)
        if workflow_run is not None:
            clauses.append(Artifact.workflow_run_id == workflow_run.id)
        artifact_statement = (
            select(Artifact)
            .where(Artifact.archived_at.is_(None))
            .where(_or_many(clauses))
            .order_by(Artifact.created_at.desc())
        )
        queried_artifacts = self.session.scalars(_apply_limit(artifact_statement, limit)).all()

        explicit_artifact_ids = _context_artifact_ids(work_item.context or {})
        if parent_work is not None:
            parent_attempt = self._latest_attempt(parent_work.id)
            if parent_attempt is not None and parent_attempt.report_artifact_id:
                explicit_artifact_ids.insert(0, parent_attempt.report_artifact_id)

        by_id: dict[str, Artifact] = {}
        ordered: list[Artifact] = []

        def add_artifact(artifact: Artifact | None) -> None:
            if artifact is None or artifact.archived_at is not None or artifact.id in by_id:
                return
            by_id[artifact.id] = artifact
            ordered.append(artifact)

        for artifact_id in explicit_artifact_ids:
            add_artifact(self.session.get(Artifact, artifact_id))

        for artifact in queried_artifacts:
            add_artifact(artifact)

        return _trim_to_limit(ordered, limit)

    def _parent_work(self, work_item: WorkItem) -> WorkItem | None:
        parent_work_item_id = _parent_work_item_id(work_item.context or {})
        if not parent_work_item_id or parent_work_item_id == work_item.id:
            return None
        return self.session.get(WorkItem, parent_work_item_id)

    def _parent_work_data(
        self,
        parent_work: WorkItem | None,
        *,
        event_limit: int | None,
        artifact_limit: int | None,
    ) -> dict[str, Any] | None:
        if parent_work is None:
            return None
        latest_attempt = self._latest_attempt(parent_work.id)
        artifact_statement = (
            select(Artifact)
            .where(Artifact.work_item_id == parent_work.id, Artifact.archived_at.is_(None))
            .order_by(Artifact.created_at.desc())
        )
        artifacts = self.session.scalars(_apply_limit(artifact_statement, artifact_limit)).all()
        event_statement = (
            select(WorkEvent)
            .where(WorkEvent.work_item_id == parent_work.id)
            .order_by(WorkEvent.created_at.desc(), WorkEvent.id.desc())
        )
        events = self.session.scalars(_apply_limit(event_statement, event_limit)).all()
        return {
            "work_item": _work_item_data(parent_work, full=True),
            "latest_attempt": _attempt_data(latest_attempt),
            "artifacts": [_artifact_data(artifact) for artifact in artifacts],
            "recent_events": [_event_data(event) for event in events],
        }

    def _latest_attempt(self, work_item_id: str) -> WorkAttempt | None:
        return self.session.scalar(
            select(WorkAttempt)
            .where(WorkAttempt.work_item_id == work_item_id)
            .order_by(WorkAttempt.attempt_number.desc(), WorkAttempt.created_at.desc())
        )

    def _events_for_work(
        self,
        work_item: WorkItem,
        *,
        limit: int | None,
    ) -> list[WorkEvent]:
        clauses = [WorkEvent.work_item_id == work_item.id]
        event_statement = (
            select(WorkEvent)
            .where(_or_many(clauses))
            .order_by(WorkEvent.created_at.desc(), WorkEvent.id.desc())
        )
        return list(
            self.session.scalars(
                _apply_limit(event_statement, limit)
            ).all()
        )

    def _workflow_data(
        self,
        workflow_run: WorkflowRun | None,
        *,
        work_item: WorkItem,
        limit: int | None,
    ) -> dict[str, Any] | None:
        if workflow_run is None:
            return None
        node_statement = (
            select(WorkflowNode)
            .where(WorkflowNode.workflow_run_id == workflow_run.id)
            .order_by(WorkflowNode.created_at)
        )
        nodes = self.session.scalars(_apply_limit(node_statement, limit)).all()
        current_node = next(
            (node for node in nodes if node.id == work_item.workflow_node_id),
            None,
        )
        return {
            "id": workflow_run.id,
            "name": workflow_run.name,
            "status": workflow_run.status,
            "current_node": _workflow_node_data(current_node) if current_node else None,
            "nodes": [_workflow_node_data(node) for node in nodes],
        }


def render_worker_context_packet(packet: dict[str, Any]) -> str:
    return json.dumps(packet, indent=2, sort_keys=True)


def render_provider_prompt(
    *,
    task_instruction: str,
    context_packet: dict[str, Any],
    result_token: str,
) -> str:
    return "\n\n".join(
        [
            (
                "# Tasque WorkItem Coordinator\n\n"
                "## Purpose\n"
                "You are the top-level coordinator for one Tasque WorkItem. The WorkItem task "
                "instruction is a reusable work template. It may be a short direct prompt, a structured "
                "template, or an older Tasque-style queued job/chain directive. Read it for the domain "
                "goal, relevant context, instructions, output expectations, durable state updates, and "
                "checks. "
                "The context packet is supporting state and a starting map."
            ),
            (
                "## Result Submission Contract\n"
                f"- result_token: {result_token}\n"
                "- You MUST call the Tasque MCP tool `submit_worker_result` exactly once near the end "
                "of this coordinator turn.\n"
                "- Submit `summary`, `report`, optional `produces`, optional `status`, and optional "
                "`error`. Use status `succeeded` for normal completion, `blocked` or `awaiting_user` "
                "when user input is needed, and `failed` only for a real task failure.\n"
                "- Tasque ignores provider stdout as a result fallback; if you do not call "
                "`submit_worker_result`, this WorkItem fails even if you print a good answer.\n"
                "- Do not ask delegated/native subagents to submit the result. The coordinator owns "
                "Tasque MCP result submission and durable state writes.\n"
                "- Treat `submit_worker_result` as terminal for this WorkItem. After the result is "
                "submitted, Tasque may terminate the provider process tree. Do not leave dev servers, "
                "watchers, browsers, or other long-running foreground children attached to this worker; "
                "stop temporary helpers before submitting, or record the restart command/URL in the "
                "report."
            ),
            (
                "## Standard Work Template Shape\n"
                "There is no required schema. Prefer this compact shape when authoring new templates:\n"
                "1. `# <Template Name>` optional.\n"
                "2. `## Goal` - outcome, role, cadence, and success condition.\n"
                "3. `## Context` - memories, artifacts, files, replies, work/run history, or tools to "
                "consult.\n"
                "4. `## Instructions` - the domain work one or more native subagents should perform.\n"
                "5. `## Output` - user-facing report, summary, files, or structured produces.\n"
                "6. `## Save` optional - memories, artifacts, child work, or workflow updates to persist.\n"
                "7. `## Checks` optional - validation, blocking criteria, or self-checks.\n"
                "Treat older labels as aliases: `Role`, `Purpose`, or `Objective` map to Goal; "
                "`Inputs`, `Resources`, or `Context To Gather` map to Context; `Directive`, "
                "`Run steps`, `Steps`, or `Native Worker Task` map to Instructions; `Produce`, "
                "`Output contract`, `Structured produces guidance`, or `Output Format` map to Output; "
                "`Durable State Updates` or `Save as...` map to Save; `Self-check` or "
                "`Acceptance Checks` map to Checks."
            ),
            (
                "## Template Interpretation\n"
                "- The task instruction is a reusable work template, not the final prompt and not a "
                "Tasque API contract. It may be fully structured, lightly structured, or plain prose.\n"
                "- Read the template for its context-gathering instructions, prompt skeleton, domain "
                "constraints, acceptance checks, and requested state/artifact updates.\n"
                "- Treat explicit placeholders, implicit references, missing background, and ambiguous "
                "template slots as context to resolve from relevant memory, artifacts, work "
                "history, task context, durable Discord transcript, and available MCP tools.\n"
                "- Do not require templates or WorkItem context to enumerate every memory query. Derive "
                "reasonable searches from the template's purpose, gather list, state keys, and domain "
                "terms. Treat explicit `memory_queries` and `memory_canonical_keys` as helpful hints, "
                "not as the only context you may retrieve.\n"
                "- Assemble one or more complete, self-contained native-subagent prompts or requests "
                "from the template and the context you gathered. Preserve the user's domain intent; "
                "do not add Tasque mechanics to native-subagent prompts unless needed to explain "
                "available local files or domain constraints.\n"
                "- Use `context_packet.model_routing.native_worker` as the requested model/profile "
                "for each launched native subagent when it is populated. The current session is the "
                "orchestrator; delegated native subagent(s) are where domain task work should run "
                "when the template calls for delegation, parallel research, critique, or specialized "
                "roles.\n"
                "- If this WorkItem is processing a user reply, use `source_reply`, `parent_work_item_id`, "
                "`parent_work`, `conversation.recent_messages`, and related artifacts to recover what "
                "the user is responding to before delegating. Each relevant native subagent should see "
                "the original domain output, the recent conversation, the user's reply, and any attached "
                "files, not Tasque routing internals.\n"
                "- After native subagent result(s) return, translate their ordinary answers into Tasque "
                "state updates and a `submit_worker_result` payload."
            ),
            (
                "## Coordinator Process\n"
                "1. Treat the work template as the domain workflow for this WorkItem. It should not need "
                "to name Tasque MCP tools, result tokens, JSON schemas, database tables, or "
                "provider-specific mechanics. If it does include stale Tasque mechanics, this coordinator "
                "contract wins.\n"
                "2. Extract the template's gather list and state-update instructions, then gather relevant "
                "context before acting. Prefer Tasque MCP read tools for memory, artifact, work, workflow, "
                "and status lookups because they support natural search and precise retrieval. Use the "
                "context packet as a starting map, not as the whole available state. Treat local artifact "
                "paths as readable source material when useful. When a parent report artifact is present, "
                "read it before interpreting a reply. Use the durable Discord transcript from context for "
                "channel/thread conversation, especially when the reply is short or refers to earlier "
                "messages.\n"
                "3. Construct the needed native-subagent prompt(s) from the template and gathered "
                "context. This may require synthesizing memories, notes, artifacts, user replies, "
                "recent history, and domain assumptions, not just simple variable substitution. Resolve "
                "contradictions explicitly. Do not treat prior proposed or prescribed work as completed "
                "unless the context contains completion evidence.\n"
                "4. If the WorkItem is straightforward Tasque routing or operations, such as starting an "
                "existing workflow, creating a schedule, saving a memory, reporting status, or queuing "
                "follow-up work, perform the needed MCP actions directly in this coordinator turn. "
                "Delegate only when there is actual domain reasoning or content work to do. Treat "
                "'run / trigger / fire my <job> now' as routing, not as the work itself: fire it as a "
                "separate run (schedule_fire_now / workflow_start) and acknowledge — do not perform that "
                "job's work inline in this turn, even when it is a single-worker job. The separate run "
                "reports its own result.\n"
                "5. For domain work, launch one or more provider-native workers/subagents/delegated "
                "agents by natural-language request when the template benefits from parallel research, "
                "critique, specialist roles, or a separate drafting pass. If a native-worker model/profile "
                "is present in `model_routing`, use provider-native controls or clear delegation "
                "instructions to run delegated workers at the requested level. When a provider's native "
                "delegation tool inherits model/profile from a full-context fork, do not also pass "
                "explicit model/profile/reasoning overrides that the tool rejects; the routed WorkItem "
                "profile is already the requested level. Ask delegated workers to do the domain reasoning "
                "and return draft answers plus recommended state/file changes. Keep Tasque state mutation "
                "and final result submission in this coordinator turn.\n"
                "6. Consolidate delegated result(s). Apply any needed durable Tasque state changes with "
                "MCP mutation tools, then submit the final worker result through `submit_worker_result`. "
                "If native delegation is unavailable or fails, submit a blocked or failed result rather "
                "than silently doing the domain work directly."
            ),
            (
                "## Durable State Rules\n"
                "- Use memory_recall (hybrid semantic + keyword search) to pull the most relevant "
                "memories for this run before reasoning; prefer it over scanning whole documents. It "
                "ranks by relevance, recency, and importance and returns small focused items. Memory "
                "in the context packet may be excerpted (content_compacted=true) — recall or "
                "memory_get_canonical for the full item when you need it.\n"
                "- Use memory_create for a NEW discrete fact, observation, or log entry. Prefer many "
                "small, atomic memories over one growing document.\n"
                "- Use memory_update to change ONE existing fact in place, and memory_delete to forget "
                "a stale or contradicted one. Do NOT rewrite a whole canonical document to edit a "
                "single line — update or delete the specific item instead. Set a 1-5 importance on "
                "facts that should rank highly in future recall.\n"
                "- Use memory_upsert_canonical only for genuinely single-state summaries that stay "
                "small (a short pointer or profile), never as an ever-growing ledger.\n"
                "- Use memory_ingest_text or memory_ingest_artifact when a text source, report, "
                "uploaded file, or useful local document should become searchable context.\n"
                "- Use todo_write for lightweight multi-step coordination state when a worker needs "
                "a durable checklist.\n"
                "- Use ask_user only when genuinely blocked on user input; submit an awaiting_user "
                "or blocked result after recording the question.\n"
                "- Use artifact_capture_file for files created by the worker; tag captured artifacts with "
                "`discord_upload` when they should be uploaded back to Discord.\n"
                "- Use schedule_create_work when the user asks for recurring or future work.\n"
                "- Use workflow_start when the user asks to run an existing workflow or chain once. "
                "Use workflow_list first when the exact workflow name is unclear.\n"
                "- Use schedule_fire_now to run an EXISTING scheduled job/watch once now (use "
                "schedule_list to find its id). When the user asks to trigger / run / fire an existing "
                "job, watch, schedule, or workflow immediately, enqueue it as its own SEPARATE run "
                "(schedule_fire_now for a schedule/watch, workflow_start for a workflow) and just "
                "confirm you queued it — do NOT reproduce that job's work inline in this reply, even "
                "when the job is a single worker. The queued run executes on its own and posts its own "
                "result, so the user gets two messages: your acknowledgement now, then the run's output "
                "when it finishes. Only inline the work when the user asks you directly to do a one-off "
                "task that is not an existing job.\n"
                "- Use work_enqueue for follow-up or child work that should run as a normal Tasque "
                "WorkItem.\n"
                "- Persist memory with the memory MCP tools (memory_create / memory_update / "
                "memory_delete / memory_upsert_canonical), not by returning a `memory_writes` array or "
                "hand-shaped database JSON in produces. `memory_writes` is a legacy path; a malformed one "
                "is skipped, not applied, so use the tools to be sure your writes land."
            ),
            (
                "## Working With Images\n"
                "- You can SEE images: Read an image file's local_path (uploaded attachments and "
                "stored artifacts expose one) and you view the actual pixels -- do this before "
                "reasoning about a photo.\n"
                "- `image_crop` crops a region of an image (a path or artifact id) to a new artifact; "
                "`image_fetch` downloads an image URL to an artifact so you can Read it (e.g. to see "
                "what a product looks like).\n"
                "- `image_save` stores/labels an image as a durable artifact you can recall later; "
                "group related images with tags you choose (there is no fixed scheme). `image_find` "
                "retrieves them by tag or free-text query.\n"
                "- `image_send` -- or adding artifact ids to `produces.discord_upload_artifact_ids` -- "
                "delivers stored images back to the user in Discord.\n"
                "- These are general-purpose; a work template may define a convention (which tags to "
                "use, when to catalog or send) for its own domain."
            ),
            (
                "## Result Payload Rules\n"
                "- `summary`: one short sentence for status surfaces.\n"
                "- `report`: the complete user-facing Markdown answer. Empty string is allowed only when "
                "there is truly nothing user-facing to say.\n"
                "- `produces`: compact machine-readable outputs such as ids, status flags, selected focus, "
                "artifact ids, child work ids, or domain result metadata.\n"
                "- For Discord uploads, include captured artifact ids in "
                "`produces.discord_upload_artifact_ids` or capture files with the `discord_upload` tag "
                "before submission."
            ),
            "## Work Template\n" + task_instruction.strip(),
            "## Context Packet JSON\n" + render_worker_context_packet(context_packet),
        ]
    )


def _context_memory_kinds(context: dict[str, Any]) -> list[str]:
    """Memory kinds to force-load in full (e.g. ``["interest"]`` for the register)."""
    return _string_list(context.get("memory_kinds"))


def _context_memory_namespaces(context: dict[str, Any]) -> list[str]:
    raw_values: list[Any] = []
    for key in ("memory_namespace", "memory_namespaces"):
        value = context.get(key)
        if isinstance(value, list):
            raw_values.extend(value)
        elif value:
            raw_values.append(value)
    namespaces: list[str] = []
    for value in raw_values:
        namespace = str(value).strip()
        if namespace and namespace not in namespaces:
            namespaces.append(namespace)
    return namespaces


def _memory_canonical_specs(
    context: dict[str, Any],
    default_namespaces: list[str],
) -> list[dict[str, str]]:
    specs: list[dict[str, str]] = []
    default_namespace = default_namespaces[0] if default_namespaces else "global"
    for item in _as_list(context.get("memory_canonical_keys")):
        if isinstance(item, str):
            key = item.strip()
            if key:
                specs.append({"namespace": default_namespace, "canonical_key": key})
            continue
        if not isinstance(item, dict):
            continue
        raw_key = item.get("canonical_key") or item.get("key")
        if raw_key is None:
            continue
        key = str(raw_key).strip()
        namespace = str(item.get("namespace") or default_namespace).strip()
        if key and namespace:
            specs.append({"namespace": namespace, "canonical_key": key})
    return specs


def _memory_query_specs(
    context: dict[str, Any],
    default_namespaces: list[str],
) -> list[dict[str, Any]]:
    specs: list[dict[str, Any]] = []
    default_namespaces = default_namespaces or ["global"]
    context_tags = _string_list(context.get("memory_tags"))
    context_limit = _optional_limit(context.get("memory_query_limit"))
    for item in [*_as_list(context.get("memory_queries")), *_as_list(context.get("memory_searches"))]:
        if isinstance(item, str):
            query = item.strip()
            if query:
                specs.extend(
                    {
                        "query": query,
                        "namespace": namespace,
                        "tags": context_tags,
                        "limit": context_limit,
                    }
                    for namespace in default_namespaces
                )
            continue
        if not isinstance(item, dict):
            continue
        raw_query = item.get("query")
        if raw_query is None:
            continue
        query = str(raw_query).strip()
        if not query:
            continue
        namespaces = _query_namespaces(item, default_namespaces)
        tags = _string_list(item.get("tags")) or context_tags
        spec_limit = _optional_limit(item.get("limit", context_limit))
        for namespace in namespaces:
            specs.append(
                {
                    "query": query,
                    "namespace": namespace,
                    "tags": tags,
                    "limit": spec_limit,
                }
            )
    return specs


def _query_namespaces(item: dict[str, Any], default_namespaces: list[str]) -> list[str]:
    namespaces = _string_list(item.get("namespaces"))
    namespace = item.get("namespace")
    if namespace:
        namespaces.insert(0, str(namespace))
    if not namespaces:
        namespaces = list(default_namespaces)
    result: list[str] = []
    for namespace_value in namespaces:
        clean = str(namespace_value).strip()
        if clean and clean not in result:
            result.append(clean)
    return result or ["global"]


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        values = [value]
    elif isinstance(value, list):
        values = value
    else:
        return []
    result: list[str] = []
    for item in values:
        text = str(item).strip()
        if text:
            result.append(text)
    return result


def _optional_limit(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return None


def _apply_limit(statement: Any, limit: int | None) -> Any:
    if limit is None:
        return statement
    return statement.limit(max(0, limit))


def _trim_to_limit[T](items: list[T], limit: int | None) -> list[T]:
    if limit is None:
        return items
    return items[: max(0, limit)]


def _limit_reached(items: list[Any], limit: int | None) -> bool:
    return limit is not None and len(items) >= max(0, limit)


def _remaining_limit(limit: int | None, count: int) -> int | None:
    if limit is None:
        return None
    return max(0, limit - count)


def _smaller_limit(left: int | None, right: int | None) -> int | None:
    if left is None:
        return right
    if right is None:
        return left
    return min(left, right)


def _work_item_data(work_item: WorkItem, *, full: bool = False) -> dict[str, Any]:
    data = {
        "id": work_item.id,
        "title": work_item.title,
        "status": work_item.status,
        "worker_kind": work_item.worker_kind,
        "priority": work_item.priority,
        "attempt_count": work_item.attempt_count,
        "max_attempts": work_item.max_attempts,
        "source_kind": work_item.source_kind,
        "source_id": work_item.source_id,
        "workflow_run_id": work_item.workflow_run_id,
        "workflow_node_id": work_item.workflow_node_id,
    }
    if full:
        data.update(
            {
                "task_instruction": work_item.task_instruction,
                "runtime_contract": work_item.runtime_contract or {},
                "context": work_item.context or {},
                "retry_policy": work_item.retry_policy or {},
            }
        )
    return data


def _attempt_data(attempt: WorkAttempt | None) -> dict[str, Any] | None:
    if attempt is None:
        return None
    return {
        "id": attempt.id,
        "attempt_number": attempt.attempt_number,
        "status": attempt.status,
        "summary": attempt.summary,
        "error_type": attempt.error_type,
        "error_message": attempt.error_message,
        "report_artifact_id": attempt.report_artifact_id,
        "produces": attempt.produces or {},
        "provider": attempt.provider,
    }


def _artifact_data(artifact: Artifact) -> dict[str, Any]:
    return {
        "id": artifact.id,
        "kind": artifact.kind,
        "title": artifact.title,
        "local_path": artifact.local_path,
        "content_type": artifact.content_type,
        "size_bytes": artifact.size_bytes,
        "sha256": artifact.sha256,
        "tags": artifact.tags or [],
        "source_kind": artifact.source_kind,
        "source_id": artifact.source_id,
    }


def _memory_data(memory: Memory, *, query: str = "") -> dict[str, Any]:
    is_durable = memory.pinned or bool(memory.canonical_key)
    budget = MEMORY_CONTEXT_PINNED_CHARS if is_durable else MEMORY_CONTEXT_CONTENT_CHARS
    # Logs/state accrete newest-last, so bias durable docs toward recent sections.
    position_bias = 0.5 if is_durable else 0.0
    content, trimmed = select_relevant_excerpt(
        memory.content,
        query,
        budget_chars=budget,
        position_bias=position_bias,
    )
    return {
        "id": memory.id,
        "namespace": memory.namespace,
        "kind": memory.kind,
        "content": content,
        "content_chars": len(memory.content),
        "content_compacted": trimmed,
        "tags": memory.tags or [],
        "work_item_id": memory.work_item_id,
        "pinned": memory.pinned,
    }


def _event_data(event: WorkEvent) -> dict[str, Any]:
    return {
        "id": event.id,
        "event_type": event.event_type,
        "entity_kind": event.entity_kind,
        "entity_id": event.entity_id,
        "summary": event.summary,
        "source": event.source,
        "created_at": event.created_at.isoformat(),
    }

def _workflow_node_data(node: WorkflowNode | None) -> dict[str, Any] | None:
    if node is None:
        return None
    return {
        "id": node.id,
        "node_key": node.node_key,
        "kind": node.kind,
        "status": node.status,
        "work_item_id": node.work_item_id,
        "failure_reason": node.failure_reason,
        "input": node.input,
        "output": node.output,
    }


def _memory_query(work_item: WorkItem) -> str:
    words = " ".join([work_item.title, work_item.task_instruction])
    tokens = [_fts_token(token) for token in words.split()]
    useful = []
    for token in tokens:
        if len(token) >= 4 and token not in useful:
            useful.append(token)
    useful = useful[:8]
    return " OR ".join(useful) or work_item.title


def _fts_token(value: str) -> str:
    return "".join(character for character in value.lower() if character.isalnum() or character == "_")


def _context_artifact_ids(context: dict[str, Any]) -> list[str]:
    ids: list[str] = []
    for key in ("attachments", "input_artifacts", "artifact_ids", "related_artifacts"):
        value = context.get(key)
        if isinstance(value, list):
            for item in value:
                if isinstance(item, str):
                    ids.append(item)
                elif isinstance(item, dict) and isinstance(item.get("artifact_id"), str):
                    ids.append(item["artifact_id"])
        elif isinstance(value, dict) and isinstance(value.get("artifact_id"), str):
            ids.append(value["artifact_id"])
    parent_report_artifact_id = context.get("parent_report_artifact_id")
    if isinstance(parent_report_artifact_id, str):
        ids.insert(0, parent_report_artifact_id)
    source_reply = context.get("source_reply")
    if isinstance(source_reply, dict):
        reply_parent_report_artifact_id = source_reply.get("parent_report_artifact_id")
        if isinstance(reply_parent_report_artifact_id, str):
            ids.insert(0, reply_parent_report_artifact_id)
    return ids


def _parent_work_item_id(context: dict[str, Any]) -> str | None:
    value = context.get("parent_work_item_id")
    if isinstance(value, str) and value.strip():
        return value.strip()
    source_reply = context.get("source_reply")
    if isinstance(source_reply, dict):
        reply_parent = source_reply.get("parent_work_item_id")
        if isinstance(reply_parent, str) and reply_parent.strip():
            return reply_parent.strip()
    return None


def _or_many(clauses: list[Any]) -> Any:
    from sqlalchemy import or_

    return or_(*clauses)
