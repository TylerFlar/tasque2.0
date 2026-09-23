from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path

import pytest
from sqlalchemy.orm import Session

from tasque2.artifacts import ArtifactStore
from tasque2.config import get_settings
from tasque2.db import session_scope
from tasque2.extensions import registry as extension_registry
from tasque2.memory import MemoryService
from tasque2.models import Memory, WorkflowDefinition, WorkflowEdge, WorkflowNode, WorkflowRun, WorkItem
from tasque2.providers import FakeProvider, ProviderRegistry, ProviderRequest
from tasque2.work.queue import WorkQueue
from tasque2.work.repository import WorkRepository
from tasque2.work.runner import WorkRunner
from tasque2.worker.context import (
    DEFAULT_LIMITS,
    MEMORY_SEARCH_HEADROOM,
    PARENT_INSTRUCTION_CHARS,
    WorkerContextBuilder,
    context_artifact_ids,
    memory_data,
    memory_query,
    packet_limits,
    parent_work_item_id,
)
from tasque2.worker.prompt import WORKER_CONTRACT, contract_path, render_user_prompt
from tasque2.worker.runtime import ProviderRuntime


def _work(session: Session, **fields) -> WorkItem:
    return WorkRepository(session).create_work_item(
        **{"title": "Context work", "task_instruction": "Do the work.", "worker_kind": "provider.fake", **fields}
    )


def _transient(**fields) -> WorkItem:
    return WorkItem(
        **{"title": "Context work", "task_instruction": "Do the work.", "worker_kind": "provider.fake", **fields}
    )


def _memory(content: str, *, canonical_key: str | None = None, pinned: bool = False) -> Memory:
    return Memory(
        id="memory-1",
        namespace="global",
        kind="note",
        content=content,
        tags=[],
        canonical_key=canonical_key,
        pinned=pinned,
    )


def _contents(packet: dict) -> list[str]:
    return [memory["content"] for memory in packet["memories"]]


def test_packet_describes_the_work_item_and_its_task_context(fresh_db: Path) -> None:
    with session_scope() as session:
        work = _work(
            session,
            lane="finance",
            max_attempts=3,
            context={
                "account": "checking",
                "reply_followup_work": {"worker_kind": "provider.claude"},
                "reply_memory": {"namespace": "finance"},
                "context_limits": {"memories": 3},
            },
        )
        packet = WorkerContextBuilder(session).build_for_work(work)

    assert packet == {
        "work_item": {
            "id": work.id,
            "title": "Context work",
            "lane": "finance",
            "status": "ready",
            "attempt": 0,
            "max_attempts": 3,
            "source_kind": None,
            "schedule_id": None,
            "workflow_run_id": None,
            "created_at": work.created_at.isoformat(),
        },
        "task_context": {"account": "checking"},
        "memories": [],
        "artifacts": [],
    }


def test_packet_holds_24_memories_and_12_artifacts_by_default(fresh_db: Path) -> None:
    with session_scope() as session:
        work = _work(session, title="Packet limits", task_instruction="Check packet limits.")
        memory = MemoryService(session)
        for index in range(30):
            memory.create_memory(namespace="global", kind="note", content=f"Packet memory {index}.")
        store = ArtifactStore()
        for index in range(13):
            store.write_text(session, kind="note", title=f"Artifact {index}", content="x", work_item_id=work.id)
        builder = WorkerContextBuilder(session)
        default = builder.build_for_work(work)
        unlimited = builder.build_for_work(work, limits={"memories": None, "artifacts": None})

    assert DEFAULT_LIMITS == {"memories": 24, "artifacts": 12}
    assert (len(default["memories"]), len(default["artifacts"])) == (24, 12)
    assert (len(unlimited["memories"]), len(unlimited["artifacts"])) == (30, 13)


def test_context_limits_from_the_contract_and_context_override_the_defaults() -> None:
    work = _transient(
        runtime_contract={"context_limits": {"memories": 2, "artifacts": 5}},
        context={"context_limits": {"artifacts": "1", "ignored": 9}},
    )

    unlimited = _transient(context={"context_limits": {"memories": None}})
    negative = _transient(context={"context_limits": {"artifacts": -4}})

    assert packet_limits(work) == {"memories": 2, "artifacts": 1}
    assert packet_limits(unlimited) == {"memories": None, "artifacts": 12}
    assert packet_limits(negative)["artifacts"] == 0


def test_invalid_context_limit_is_rejected() -> None:
    with pytest.raises(ValueError, match="context_limits.memories must be an integer or null."):
        packet_limits(_transient(context={"context_limits": {"memories": "many"}}))


def test_pinned_canonical_keys_size_the_memory_limit() -> None:
    pinned = _transient(context={"memory_namespace": "cooking", "memory_canonical_keys": ["a", "b", "c"]})
    unpinned = _transient(context={"memory_namespace": "cooking"})
    register = _transient(
        context={"memory_namespace": "local", "memory_canonical_keys": ["a", "b"], "memory_kinds": ["interest"]}
    )
    explicit = _transient(
        context={
            "memory_namespace": "cooking",
            "memory_canonical_keys": ["a", "b", "c"],
            "context_limits": {"memories": 30},
        }
    )

    assert packet_limits(pinned)["memories"] == 3 + MEMORY_SEARCH_HEADROOM
    assert packet_limits(unpinned)["memories"] == DEFAULT_LIMITS["memories"]
    assert packet_limits(register)["memories"] == DEFAULT_LIMITS["memories"]
    assert packet_limits(explicit)["memories"] == 30


def test_canonical_documents_and_memory_queries_reach_the_packet(fresh_db: Path) -> None:
    with session_scope() as session:
        memory = MemoryService(session)
        memory.upsert_canonical(
            namespace="health",
            canonical_key="current_workout_state",
            kind="summary",
            content="Current workout state: last confirmed pull session.",
        )
        memory.upsert_canonical(
            namespace="health",
            canonical_key="workout_exercise_ledger",
            kind="summary",
            content="Leg press [Tier A, 6-10, +5]: 2026-06-01 3x10 @ 95 lb clean.",
        )
        memory.upsert_canonical(namespace="finance", canonical_key="budget", kind="summary", content="Budget doctrine.")
        memory.create_memory(
            namespace="health",
            kind="working",
            content="Completed workout actual loads: bench 95x10 RPE 8.",
            tags=["workout", "completion"],
        )
        work = _work(
            session,
            title="Workout generator",
            task_instruction="Generate today's workout.",
            context={
                "memory_namespace": "health",
                "memory_canonical_keys": [
                    "current_workout_state",
                    "workout_exercise_ledger",
                    {"namespace": "finance", "key": "budget"},
                ],
                "memory_queries": [{"query": "completed workout actual loads", "tags": ["workout", "completion"]}],
            },
        )
        packet = WorkerContextBuilder(session).build_for_work(work)

    assert _contents(packet)[:4] == [
        "Current workout state: last confirmed pull session.",
        "Leg press [Tier A, 6-10, +5]: 2026-06-01 3x10 @ 95 lb clean.",
        "Budget doctrine.",
        "Completed workout actual loads: bench 95x10 RPE 8.",
    ]


def test_memory_kinds_force_load_whole_registers(fresh_db: Path) -> None:
    with session_scope() as session:
        memory = MemoryService(session)
        for name in ("hiking", "climbing", "chess"):
            memory.create_memory(namespace="local", kind="interest", content=f"Interest: {name}.")
        memory.create_memory(namespace="local", kind="note", content="Unrelated note.")
        work = _work(
            session,
            title="Scout",
            task_instruction="Find events.",
            context={"memory_namespace": "local", "memory_kinds": ["interest"]},
        )
        packet = WorkerContextBuilder(session).build_for_work(work)

    assert set(_contents(packet)) == {"Interest: hiking.", "Interest: climbing.", "Interest: chess."}


def test_zero_memory_limit_loads_no_memories(fresh_db: Path) -> None:
    with session_scope() as session:
        MemoryService(session).upsert_canonical(
            namespace="global", canonical_key="doctrine", kind="summary", content="Doctrine."
        )
        work = _work(session, context={"memory_canonical_keys": ["doctrine"], "context_limits": {"memories": 0}})
        packet = WorkerContextBuilder(session).build_for_work(work)

    assert packet["memories"] == []


def test_documents_the_run_pins_arrive_whole_and_recalled_ones_as_excerpts() -> None:
    content = "Fact. " * 600

    recalled_note = memory_data(_memory(content), pinned=False)
    recalled_doctrine = memory_data(_memory(content, canonical_key="doctrine", pinned=True), pinned=False)
    pinned_doctrine = memory_data(_memory(content, canonical_key="doctrine"), pinned=True)

    for recalled in (recalled_note, recalled_doctrine):
        assert recalled["content_compacted"] is True
        assert recalled["content_chars"] == len(content)
        assert len(recalled["content"]) < len(content)
    assert pinned_doctrine["content"] == content
    assert "content_compacted" not in pinned_doctrine


def test_packet_delivers_pinned_documents_whole_and_recall_hits_excerpted(fresh_db: Path) -> None:
    long_rules = "Rule line.\n" * 400
    with session_scope() as session:
        service = MemoryService(session)
        service.upsert_canonical(
            namespace="finance", canonical_key="finance_direction", kind="doctrine", content=long_rules
        )
        service.upsert_canonical(
            namespace="finance", canonical_key="finance_archive", kind="summary", content="Rule archive line.\n" * 400
        )
        work = WorkRepository(session).create_work_item(
            title="Rule review",
            task_instruction="Review the rule archive and the rule lines.",
            worker_kind="manual",
            context={
                "memory_namespace": "finance",
                "memory_canonical_keys": ["finance_direction"],
                "memory_queries": ["rule archive"],
            },
        )
        packet = WorkerContextBuilder(session).build_for_work(work)

    by_key = {memory["canonical_key"]: memory for memory in packet["memories"]}
    assert by_key["finance_direction"]["content"] == long_rules
    assert by_key["finance_archive"]["content_compacted"] is True
    assert len(by_key["finance_archive"]["content"]) < 2200
    assert packet["task_context"] == {"memory_namespace": "finance"}


def test_declared_budget_flags_documents_over_it() -> None:
    marker = "<!-- tasque:max_chars=200 -->"
    over_content = f"{marker}\n" + "x" * 300

    over = memory_data(_memory(over_content, canonical_key="ledger"))
    within = memory_data(_memory(f"{marker}\nshort", canonical_key="ledger"))

    assert over["max_chars"] == 200
    assert over["over_budget"] is True
    assert over["compact_this_run"].startswith(f"ledger is {len(over_content)} characters against its 200 budget.")
    assert within["max_chars"] == 200
    assert "over_budget" not in within


def test_declared_budget_raises_the_delivery_budget_up_to_a_ceiling() -> None:
    large = "<!-- tasque:max_chars=20000 -->\n" + "y" * 15000
    oversized = "<!-- tasque:max_chars=40000 -->\n" + "z" * 15000

    assert memory_data(_memory(large, canonical_key="big"))["content"] == large
    assert memory_data(_memory(oversized, canonical_key="huge"))["content_compacted"] is True


def test_reply_packet_carries_the_parent_work_and_its_report(fresh_db: Path) -> None:
    with session_scope() as session:
        parent = _work(session, title="Workout generator", task_instruction="Generate today's workout.")
        claimed = WorkQueue(session).claim_next_ready_work(lease_owner="test")
        report = ArtifactStore().write_text(
            session,
            kind="worker_report",
            title="Workout report",
            content="**Focus**: push\nBench press - 3x10 @ 95 lb",
            work_item_id=parent.id,
            attempt_id=claimed.attempt.id,
            tags=["provider", "report"],
        )
        WorkQueue(session).complete_attempt(
            claimed.attempt.id,
            summary="Prescribed push workout.",
            produces={"focus": "push"},
            report_artifact_id=report.id,
        )
        reply = _work(
            session,
            title="Process workout reply",
            task_instruction="Process the user's workout reply.",
            context={
                "parent_work_item_id": parent.id,
                "source_reply": {
                    "discord_message_id": "reply-1",
                    "content": "I did the workout and bench was 95x10 RPE 8.",
                    "parent_work_item_id": parent.id,
                    "parent_report_artifact_id": report.id,
                },
            },
        )
        packet = WorkerContextBuilder(session).build_for_work(reply)

    parent_work = packet["parent_work"]
    assert parent_work["work_item"]["id"] == parent.id
    assert parent_work["latest_attempt"] == {
        "status": "succeeded",
        "summary": "Prescribed push workout.",
        "produces": {"focus": "push"},
        "error_message": None,
    }
    assert parent_work["report_artifact"]["id"] == report.id
    assert "task_instruction" not in parent_work
    assert [artifact["id"] for artifact in packet["artifacts"]] == [report.id]
    assert packet["task_context"]["source_reply"]["content"] == "I did the workout and bench was 95x10 RPE 8."


def test_default_reply_processor_sees_the_parent_instruction_truncated(fresh_db: Path) -> None:
    with session_scope() as session:
        parent = _work(session, task_instruction="p" * (PARENT_INSTRUCTION_CHARS + 50))
        reply = _work(session, context={"parent_work_item_id": parent.id, "reply_default_processor": True})
        parent_work = WorkerContextBuilder(session).build_for_work(reply)["parent_work"]

    assert parent_work["task_instruction"] == "p" * PARENT_INSTRUCTION_CHARS
    assert parent_work["task_instruction_truncated"] is True
    assert parent_work["latest_attempt"] is None
    assert parent_work["report_artifact"] is None


def test_workflow_packet_lists_the_current_node_and_its_upstream_outputs(fresh_db: Path) -> None:
    with session_scope() as session:
        definition = WorkflowDefinition(name="Morning", version="1", definition={})
        session.add(definition)
        session.flush()
        run = WorkflowRun(workflow_definition_id=definition.id, name="Morning", status="active", input={"day": "tue"})
        session.add(run)
        session.flush()
        specs = {
            "fetch": ("succeeded", {"rows": 3}, None),
            "weather": ("failed", {}, "timeout"),
            "write": ("running", {}, None),
            "post": ("pending", {}, None),
            "unrelated": ("succeeded", {"x": 1}, None),
        }
        nodes = {
            key: WorkflowNode(
                workflow_run_id=run.id,
                node_key=key,
                kind="work",
                status=status,
                input={"key": key},
                output=output,
                failure_reason=reason,
            )
            for key, (status, output, reason) in specs.items()
        }
        session.add_all(nodes.values())
        session.flush()
        for upstream, downstream in (("fetch", "write"), ("weather", "write"), ("write", "post")):
            session.add(
                WorkflowEdge(workflow_run_id=run.id, from_node_id=nodes[upstream].id, to_node_id=nodes[downstream].id)
            )
        run_artifact = ArtifactStore().write_text(
            session, kind="note", title="Run input", content="x", workflow_run_id=run.id
        )
        work = _work(session, workflow_run_id=run.id, workflow_node_id=nodes["write"].id)
        packet = WorkerContextBuilder(session).build_for_work(work)

    workflow = packet["workflow"]
    assert {key: workflow[key] for key in ("id", "name", "status", "input")} == {
        "id": run.id,
        "name": "Morning",
        "status": "active",
        "input": {"day": "tue"},
    }
    assert workflow["current_node"] == {
        "node_key": "write",
        "kind": "work",
        "status": "running",
        "input": {"key": "write"},
    }
    assert sorted(workflow["nodes"], key=lambda node: node["node_key"]) == [
        {"node_key": "fetch", "kind": "work", "status": "succeeded", "input": {"key": "fetch"}, "output": {"rows": 3}},
        {
            "node_key": "weather",
            "kind": "work",
            "status": "failed",
            "input": {"key": "weather"},
            "failure_reason": "timeout",
            "output": {},
        },
    ]
    assert [artifact["id"] for artifact in packet["artifacts"]] == [run_artifact.id]


def test_extension_digests_join_the_packets_that_want_them(fresh_db: Path, caplog: pytest.LogCaptureFixture) -> None:
    registry = extension_registry()
    registry.add_context_digest(
        "finance_ledger", lambda context: bool(context.get("finance")), lambda _: {"balance": 120}
    )
    registry.add_context_digest("broken_ledger", lambda context: True, lambda _: 1 / 0)

    with session_scope() as session, caplog.at_level(logging.ERROR, logger="tasque2.worker.context"):
        finance = WorkerContextBuilder(session).build_for_work(_work(session, context={"finance": True}))
        other = WorkerContextBuilder(session).build_for_work(_work(session))

    assert finance["finance_ledger"] == {"balance": 120}
    assert "finance_ledger" not in other
    assert "broken_ledger" not in finance
    assert "Failed to compute digest broken_ledger" in caplog.text


def test_extension_canonical_key_resolvers_pin_docs_per_run(fresh_db: Path) -> None:
    seen_contexts: list[dict] = []

    def resolve(_session: Session, context: dict) -> list[str]:
        seen_contexts.append(context)
        return ["ledger_b"]

    extension_registry().add_canonical_keys(lambda context: bool(context.get("ledger_lane")), resolve)
    with session_scope() as session:
        memory = MemoryService(session)
        for key in ("ledger_a", "ledger_b"):
            memory.upsert_canonical(namespace="health", canonical_key=key, kind="canonical", content=f"{key} body")
        lane_work = _work(
            session,
            title="Lane run",
            task_instruction="Program today.",
            context={"memory_namespace": "health", "ledger_lane": True},
        )
        other_work = _work(
            session, title="Other run", task_instruction="Unrelated.", context={"memory_namespace": "health"}
        )
        builder = WorkerContextBuilder(session)
        lane_contents = _contents(builder.build_for_work(lane_work))
        other_contents = _contents(builder.build_for_work(other_work))

    assert "ledger_b body" in lane_contents
    assert "ledger_a body" not in lane_contents
    assert "ledger_b body" not in other_contents
    assert seen_contexts == [{"memory_namespace": "health", "ledger_lane": True}]


def test_canonical_key_resolver_failure_does_not_cost_other_memories(fresh_db: Path) -> None:
    def resolve(_session: Session, _context: dict) -> list[str]:
        raise RuntimeError("board unavailable")

    extension_registry().add_canonical_keys(lambda context: True, resolve)
    with session_scope() as session:
        MemoryService(session).upsert_canonical(
            namespace="health", canonical_key="doctrine", kind="canonical", content="doctrine body"
        )
        work = _work(
            session,
            title="Run",
            task_instruction="Go.",
            context={"memory_namespace": "health", "memory_canonical_keys": ["doctrine"]},
        )
        packet = WorkerContextBuilder(session).build_for_work(work)

    assert _contents(packet) == ["doctrine body"]


def test_context_artifact_ids_collect_attachments_with_the_parent_report_first() -> None:
    context = {
        "attachments": ["a1", {"artifact_id": "a2"}, {"name": "no id"}, 3],
        "input_artifacts": "a3",
        "artifact_ids": ["a4"],
        "related_artifacts": [{"artifact_id": "a5"}],
        "source_reply": {"parent_report_artifact_id": "r1"},
    }

    assert context_artifact_ids(context) == ["r1", "a1", "a2", "a3", "a4", "a5"]
    assert context_artifact_ids(
        {"parent_report_artifact_id": "r2", "source_reply": {"parent_report_artifact_id": "r1"}}
    ) == ["r1", "r2"]


def test_parent_work_item_id_reads_the_context_or_the_source_reply() -> None:
    assert parent_work_item_id({"parent_work_item_id": " w1 "}) == "w1"
    assert parent_work_item_id({"source_reply": {"parent_work_item_id": "w2"}}) == "w2"
    assert parent_work_item_id({"parent_work_item_id": " ", "source_reply": "w3"}) is None


def test_memory_query_joins_the_distinctive_words() -> None:
    assert memory_query(
        _transient(title="Workout generator", task_instruction="Generate today's workout: bench, bench press.")
    ) == ("workout OR generator OR generate OR todays OR bench OR press")
    assert memory_query(
        _transient(title="alpha bravo charlie delta", task_instruction="echo foxtrot golf hotel india")
    ) == ("alpha OR bravo OR charlie OR delta OR echo OR foxtrot OR golf OR hotel")
    assert memory_query(_transient(title="Go", task_instruction="do it")) == "Go"


def test_worker_context_span_times_packet_and_prompt_assembly(fresh_db: Path, spans) -> None:
    captured: list[ProviderRequest] = []
    registry = ProviderRegistry()
    registry.register(FakeProvider(capture_requests=captured))
    with session_scope() as session:
        MemoryService(session).create_memory(namespace="global", kind="note", content="Context span memory.")
        work = _work(session, title="Context span", task_instruction="Check the context span memory.")
        WorkRunner(session, provider_runtime=ProviderRuntime(registry=registry)).run_next()

    finished = spans.get_finished_spans()
    context_span = next(item for item in finished if item.name == "tasque.worker.context")
    work_run = next(item for item in finished if item.name == "tasque.work.run")
    assert context_span.parent.span_id == work_run.context.span_id
    assert dict(context_span.attributes) == {
        "tasque.work.id": work.id,
        "tasque.prompt.chars": len(captured[0].prompt),
        "tasque.packet.memories": 1,
    }


def test_user_prompt_holds_the_run_header_template_and_compact_packet() -> None:
    scratch = Path("scratch") / "attempt-1"

    prompt = render_user_prompt(
        task_instruction="  Summarize the day.  ",
        context_packet={"work_item": {"id": "work-1"}, "note": "café", "when": datetime(2026, 9, 22, tzinfo=UTC)},
        result_token="tok-1",
        scratch_dir=scratch,
        now=datetime(2026, 9, 22, 17, 30, tzinfo=UTC),
    )

    assert prompt == (
        "# Run\n"
        "- result_token: tok-1\n"
        "- work_item_id: work-1\n"
        "- local time: Tuesday 2026-09-22 10:30 (America/Los_Angeles)\n"
        f"- scratch directory: {scratch}\n\n"
        "# Work template\n\nSummarize the day.\n\n"
        '# Context packet\n\n{"work_item":{"id":"work-1"},"note":"café","when":"2026-09-22 00:00:00+00:00"}'
    )


def test_user_prompt_without_a_scratch_directory_omits_it() -> None:
    prompt = render_user_prompt(task_instruction="Go.", context_packet={}, result_token="tok", scratch_dir=None)

    assert "scratch directory" not in prompt
    assert "- work_item_id: \n" in prompt


def test_worker_contract_is_written_once_under_the_data_dir() -> None:
    path = contract_path()

    assert path.parent == get_settings().resolved_data_dir / "runtime"
    assert path.name.startswith("worker-contract-")
    assert path.suffix == ".md"
    assert path.read_text(encoding="utf-8") == WORKER_CONTRACT
    path.write_text("kept", encoding="utf-8")
    assert contract_path() == path
    assert path.read_text(encoding="utf-8") == "kept"


def test_worker_contract_keeps_the_run_one_shot_and_in_the_foreground() -> None:
    assert "The run is headless and one-shot." in WORKER_CONTRACT
    assert "Run everything in the foreground and wait for it." in WORKER_CONTRACT
    assert "Call `submit_worker_result` exactly once, as your last action" in WORKER_CONTRACT
