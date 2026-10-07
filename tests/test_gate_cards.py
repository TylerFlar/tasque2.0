from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import select

from tasque2.db import session_scope
from tasque2.discord.gateway import FakeDiscordGateway
from tasque2.discord.output import DiscordOutputService, OutputChannels
from tasque2.discord.ui import DiscordUIAction, DiscordUIService, parse_custom_id
from tasque2.models import WorkflowNode, WorkflowRun
from tasque2.work.runner import WorkRunner
from tasque2.workflows import WorkflowService, validate_definition

CHANNELS = OutputChannels(ops="ops", jobs="jobs", chains="chains", dlq="dlq")
DEFINITION = {
    "nodes": [
        {
            "key": "verify",
            "kind": "work",
            "title": "Verify",
            "task_instruction": "Fix ready: tests pass.",
            "worker_kind": "function.echo",
        },
        {
            "key": "approve",
            "kind": "gate",
            "prompt": "Merge?",
            "choices": ["Merge and restart", "Discard"],
            "card_from": "verify.task_instruction",
            "depends_on": ["verify"],
        },
        {
            "key": "merge",
            "kind": "work",
            "title": "Merge",
            "task_instruction": "Merge.",
            "worker_kind": "function.echo",
            "depends_on": ["approve"],
        },
    ]
}


def _started(session, *, thread: str | None = "thread-accounts") -> WorkflowRun:
    service = WorkflowService(session)
    definition = service.create_definition(name="repair", version="1", definition=DEFINITION)
    run = service.start_run(workflow_definition_id=definition.id, discord_thread_id=thread)
    service.tick_runs()
    WorkRunner(session).run_next()
    service.tick_runs()
    return run


def _gate(session, run: WorkflowRun) -> WorkflowNode:
    return session.scalar(
        select(WorkflowNode).where(WorkflowNode.workflow_run_id == run.id, WorkflowNode.node_key == "approve")
    )


def test_a_waiting_gate_takes_its_card_text_from_the_upstream_output(fresh_db: Path) -> None:
    with session_scope() as session:
        run = _started(session)
        gate = _gate(session, run)
        assert gate.status == "awaiting_input"
        assert gate.input["card"] == "Fix ready: tests pass."


def test_the_card_posts_once_into_the_runs_thread_with_a_button_per_choice(fresh_db: Path) -> None:
    gateway = FakeDiscordGateway()
    with session_scope() as session:
        run = _started(session)
        service = DiscordOutputService(session)
        assert service.post_gate_cards(gateway=gateway) == 1
        assert service.post_gate_cards(gateway=gateway) == 0

        assert gateway.sent_messages == [("thread-accounts", "Fix ready: tests pass.")]
        buttons = [(child.label, child.custom_id) for child in gateway.sent_views[-1].children]
        assert buttons == [("Merge and restart", f"t2:gate:0:{run.id}"), ("Discard", f"t2:gate:1:{run.id}")]


def test_quiet_hours_hold_the_card(fresh_db: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("tasque2.discord.output.in_quiet_hours", lambda *args, **kwargs: True)
    gateway = FakeDiscordGateway()
    with session_scope() as session:
        _started(session)
        DiscordOutputService(session).post_pending_updates(gateway=gateway, channels=CHANNELS)
    assert not any(content == "Fix ready: tests pass." for _channel, content in gateway.sent_messages)


def test_a_button_answers_the_gate_and_the_run_moves_on(fresh_db: Path) -> None:
    with session_scope() as session:
        run = _started(session)
        action = parse_custom_id(f"t2:gate:0:{run.id}")
        assert action == DiscordUIAction(scope="gate", action="0", entity_id=run.id)

        assert DiscordUIService(session).handle_action(action) == "Chosen: Merge and restart"
        gate = _gate(session, run)
        assert gate.status == "succeeded" and gate.output == {"answer": "Merge and restart"}
        WorkflowService(session).tick_runs()
        merge = session.scalar(
            select(WorkflowNode).where(WorkflowNode.workflow_run_id == run.id, WorkflowNode.node_key == "merge")
        )
        assert merge.status == "enqueued"
        # a second click on the old card changes nothing
        assert DiscordUIService(session).handle_action(action) == "That choice is no longer open."


def test_an_unknown_choice_is_refused(fresh_db: Path) -> None:
    with session_scope() as session:
        run = _started(session)
        with pytest.raises(ValueError, match="Unknown choice"):
            DiscordUIService(session).handle_action(DiscordUIAction(scope="gate", action="7", entity_id=run.id))


@pytest.mark.parametrize(
    "node",
    [
        {"key": "w", "kind": "work", "choices": ["a", "b"]},
        {"key": "g", "kind": "gate", "choices": ["only one"]},
        {"key": "g", "kind": "gate", "choices": ["a", "b", "c", "d", "e", "f"]},
        {"key": "g", "kind": "gate", "choices": ["a", "x" * 81]},
        {"key": "g", "kind": "gate", "choices": "a,b"},
    ],
)
def test_bad_choices_are_refused_when_the_definition_is_read(node: dict) -> None:
    with pytest.raises(ValueError, match="choices"):
        validate_definition({"nodes": [node]})
