from __future__ import annotations

from pathlib import Path

from tasque2.artifacts import ArtifactStore
from tasque2.db import session_scope
from tasque2.memory import MemoryService
from tasque2.models import WorkEvent, WorkflowDefinition, WorkflowNode, WorkflowRun
from tasque2.repo import WorkRepository
from tasque2.worker_context import WorkerContextBuilder


def test_worker_context_has_no_default_limits(fresh_db: Path) -> None:
    with session_scope() as session:
        definition = WorkflowDefinition(
            name="Unlimited Context",
            version="1",
            definition={},
        )
        session.add(definition)
        session.flush()

        run = WorkflowRun(
            workflow_definition_id=definition.id,
            name="Unlimited Context",
        )
        session.add(run)
        session.flush()

        nodes = [
            WorkflowNode(
                workflow_run_id=run.id,
                node_key=f"node-{index}",
                kind="task",
            )
            for index in range(21)
        ]
        session.add_all(nodes)
        session.flush()

        work = WorkRepository(session).create_work_item(
            title="Unlimited context",
            task_instruction="Use unlimited context memories.",
            worker_kind="provider.fake",
            workflow_run_id=run.id,
            workflow_node_id=nodes[-1].id,
        )
        nodes[-1].work_item_id = work.id

        memory = MemoryService(session)
        for index in range(9):
            memory.create_memory(
                namespace="global",
                kind="note",
                content=f"Unlimited context memory {index}.",
            )

        artifacts = ArtifactStore()
        for index in range(13):
            artifacts.write_text(
                session,
                kind="note",
                title=f"Artifact {index}",
                content=f"Artifact {index}",
                work_item_id=work.id,
            )

        for index in range(21):
            session.add(
                WorkEvent(
                    event_type="test.event",
                    entity_kind="work_item",
                    entity_id=work.id,
                    work_item_id=work.id,
                    source="test",
                    summary=f"Event {index}",
                )
            )
        session.flush()

        packet = WorkerContextBuilder(session).build_for_work(work)

        assert len(packet["memories"]) == 9
        assert len(packet["artifacts"]) == 13
        assert len(packet["recent_events"]) >= 22
        assert len(packet["workflow"]["nodes"]) == 21


def test_extension_canonical_key_resolvers_pin_docs_per_run(fresh_db: Path) -> None:
    # A resolver picks canonical docs PER RUN for pinned sets where only one
    # member matters to a given run (a per-focus ledger, say), so the packet
    # carries the one that matters instead of the whole family.
    from tasque2.extensions import registry as extension_registry

    registry = extension_registry()
    seen_contexts: list[dict] = []

    def wants(context: dict) -> bool:
        return bool(context.get("ledger_lane"))

    def resolve(session, context: dict) -> list[str]:
        seen_contexts.append(context)
        return ["ledger_b"]

    registry.add_canonical_keys(wants, resolve)
    try:
        with session_scope() as session:
            memory = MemoryService(session)
            for key in ("ledger_a", "ledger_b"):
                memory.upsert_canonical(
                    namespace="health",
                    canonical_key=key,
                    kind="canonical",
                    content=f"{key} body",
                )
            repo = WorkRepository(session)
            lane_work = repo.create_work_item(
                title="Lane run",
                task_instruction="Program today.",
                worker_kind="provider.fake",
                context={"memory_namespace": "health", "ledger_lane": True},
            )
            other_work = repo.create_work_item(
                title="Other run",
                task_instruction="Unrelated.",
                worker_kind="provider.fake",
                context={"memory_namespace": "health"},
            )

            builder = WorkerContextBuilder(session)
            lane_contents = {m["content"] for m in builder.build_for_work(lane_work)["memories"]}
            assert "ledger_b body" in lane_contents
            assert "ledger_a body" not in lane_contents
            assert seen_contexts == [{"memory_namespace": "health", "ledger_lane": True}]

            other_contents = {m["content"] for m in builder.build_for_work(other_work)["memories"]}
            assert "ledger_b body" not in other_contents
            assert len(seen_contexts) == 1  # `wants` gated the resolver off
    finally:
        registry.canonical_key_resolvers.remove((wants, resolve))


def test_canonical_key_resolver_failure_does_not_cost_other_memories(fresh_db: Path) -> None:
    from tasque2.extensions import registry as extension_registry

    registry = extension_registry()

    def wants(context: dict) -> bool:
        return True

    def resolve(session, context: dict) -> list[str]:
        raise RuntimeError("board unavailable")

    registry.add_canonical_keys(wants, resolve)
    try:
        with session_scope() as session:
            MemoryService(session).upsert_canonical(
                namespace="health",
                canonical_key="doctrine",
                kind="canonical",
                content="doctrine body",
            )
            work = WorkRepository(session).create_work_item(
                title="Run",
                task_instruction="Go.",
                worker_kind="provider.fake",
                context={"memory_namespace": "health", "memory_canonical_keys": ["doctrine"]},
            )
            packet = WorkerContextBuilder(session).build_for_work(work)
            assert {m["content"] for m in packet["memories"]} == {"doctrine body"}
    finally:
        registry.canonical_key_resolvers.remove((wants, resolve))
