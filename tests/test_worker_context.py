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
