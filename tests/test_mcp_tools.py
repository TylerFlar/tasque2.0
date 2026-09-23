from __future__ import annotations

import inspect
import json
from pathlib import Path
from typing import Any

import pytest
from PIL import Image
from sqlalchemy import func, select

from tasque2.artifacts import ArtifactStore
from tasque2.db import session_scope
from tasque2.discord.routing import DiscordService
from tasque2.mcp import tools
from tasque2.memory import MemoryService
from tasque2.models import Artifact, Memory, Schedule, WorkflowRun, WorkItem
from tasque2.work.repository import WorkRepository
from tasque2.worker import results
from tasque2.workflows import WorkflowService


def _ok(payload: str) -> dict[str, Any]:
    data = json.loads(payload)
    assert data["ok"] is True, data
    return data


def _error(payload: str) -> dict[str, Any]:
    data = json.loads(payload)
    assert data["ok"] is False, data
    return data


def _caller(monkeypatch: pytest.MonkeyPatch, **fields: Any) -> str:
    with session_scope() as session:
        caller = WorkRepository(session).create_work_item(
            title=fields.pop("title", "Caller"),
            task_instruction=fields.pop("task_instruction", "Call tools."),
            worker_kind=fields.pop("worker_kind", "provider.default"),
            **fields,
        )
        caller_id = caller.id
    monkeypatch.setenv("TASQUE2_WORK_ITEM_ID", caller_id)
    return caller_id


def _png(path: Path, size: tuple[int, int] = (40, 20), color: tuple[int, int, int] = (200, 30, 30)) -> Path:
    Image.new("RGB", size, color).save(path)
    return path


def _image_size(path: str) -> tuple[int, int]:
    with Image.open(path) as image:
        return image.size


def test_core_tools_are_unique_and_documented() -> None:
    names = [tool.__name__ for tool in tools.CORE_TOOLS]
    assert len(names) == len(set(names))
    assert set(tools.__all__) == {"CORE_TOOLS", *names}
    for tool in tools.CORE_TOOLS:
        assert (tool.__doc__ or "").strip(), tool.__name__
        assert inspect.signature(tool).return_annotation in {str, "str"}


def test_memory_create_returns_an_ack_without_the_content(fresh_db: Path) -> None:
    content = "Completed workout actual loads: bench 95x10 at RPE 8."
    created = _ok(tools.memory_create(namespace="health", kind="note", content=content, tags=["workout"]))

    ack = created["memory"]
    assert "content" not in ack
    assert ack["chars"] == len(content)
    assert (ack["namespace"], ack["kind"]) == ("health", "note")

    fetched = _ok(tools.memory_get(memory_id=ack["id"], intent="check the write"))
    assert fetched["memory"]["content"] == content
    assert fetched["_intent"] == "check the write"


def test_memory_recall_finds_created_memories(fresh_db: Path) -> None:
    created = _ok(
        tools.memory_create(
            namespace="health",
            kind="note",
            content="Completed workout actual loads: bench 95x10 at RPE 8.",
            tags=["workout", "completion"],
            importance=4,
        )
    )
    tools.memory_create(namespace="health", kind="note", content="Sleep went fine.", tags=["sleep"])

    found = _ok(
        tools.memory_recall(
            query="actual loads RPE",
            namespace="health",
            tags=["workout", "completion"],
            intent="recent workout completions",
        )
    )

    assert [item["id"] for item in found["items"]] == [created["memory"]["id"]]
    assert found["items"][0]["importance"] == 4
    assert found["items"][0]["score"] > 0


def test_memory_upsert_canonical_replaces_the_document(fresh_db: Path) -> None:
    first = _ok(
        tools.memory_upsert_canonical(
            namespace="health",
            canonical_key="current_workout_state",
            kind="summary",
            content="Current workout state: last confirmed pull.",
            tags=["workout", "state"],
        )
    )
    second = _ok(
        tools.memory_upsert_canonical(
            namespace="health",
            canonical_key="current_workout_state",
            kind="summary",
            content="Current workout state: last confirmed push.",
        )
    )
    assert "content" not in second["memory"]
    assert second["memory"]["id"] != first["memory"]["id"]

    canonical = _ok(
        tools.memory_get_canonical(
            namespace="health", canonical_key="current_workout_state", intent="current workout state"
        )
    )
    assert canonical["memory"]["id"] == second["memory"]["id"]
    assert canonical["memory"]["content"] == "Current workout state: last confirmed push."
    missing = _ok(tools.memory_get_canonical(namespace="health", canonical_key="nothing_here"))
    assert missing["memory"] is None


def test_memory_writes_record_the_calling_work_item(fresh_db: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    caller_id = _caller(monkeypatch)

    created = _ok(tools.memory_create(namespace="global", kind="note", content="From a run."))
    upserted = _ok(
        tools.memory_upsert_canonical(namespace="global", canonical_key="state", kind="summary", content="State.")
    )
    ingested = _ok(tools.memory_ingest_text(namespace="global", title="Notes", content="Ingested from a run."))

    with session_scope() as session:
        ids = [created["memory"]["id"], upserted["memory"]["id"], *ingested["memory_ids"]]
        assert {session.get(Memory, memory_id).work_item_id for memory_id in ids} == {caller_id}
        assert {session.get(Memory, memory_id).source_kind for memory_id in ids[:2]} == {"mcp"}


def test_memory_update_edits_in_place_and_acks(fresh_db: Path) -> None:
    created = _ok(tools.memory_create(namespace="local", kind="fact", content="He likes archery."))
    memory_id = created["memory"]["id"]

    updated = _ok(tools.memory_update(memory_id=memory_id, content="He likes fencing.", tags=["hobby"], importance=5))
    unchanged = _ok(tools.memory_update(memory_id=memory_id, content="   "))

    assert updated["memory"]["id"] == memory_id
    assert "content" not in updated["memory"]
    assert unchanged["memory"]["chars"] == len("He likes fencing.")
    fetched = _ok(tools.memory_get(memory_id=memory_id))["memory"]
    assert (fetched["content"], fetched["tags"], fetched["importance"]) == ("He likes fencing.", ["hobby"], 5)


def test_memory_tools_report_budget_refusals(fresh_db: Path) -> None:
    created = _ok(
        tools.memory_upsert_canonical(
            namespace="health",
            canonical_key="health_state",
            kind="summary",
            content="compact\n<!-- tasque:max_chars=200 -->\n",
        )
    )

    refused_upsert = _error(
        tools.memory_upsert_canonical(
            namespace="health", canonical_key="health_state", kind="summary", content="x" * 500
        )
    )
    refused_update = _error(tools.memory_update(memory_id=created["memory"]["id"], content="y" * 500))

    for refused in (refused_upsert, refused_update):
        assert refused["error_type"] == "MemoryBudgetExceeded"
        assert "budget of 200" in refused["error"]
    current = _ok(tools.memory_get_canonical(namespace="health", canonical_key="health_state"))
    assert current["memory"]["id"] == created["memory"]["id"]
    assert current["memory"]["content"].startswith("compact")


def test_memory_archive_and_delete(fresh_db: Path) -> None:
    kept = _ok(tools.memory_create(namespace="global", kind="note", content="Keep me around."))
    archived = _ok(tools.memory_create(namespace="global", kind="note", content="Archive me."))
    deleted = _ok(tools.memory_create(namespace="global", kind="note", content="Delete me."))

    _ok(tools.memory_archive(memory_id=archived["memory"]["id"]))
    gone = _ok(tools.memory_delete(memory_id=deleted["memory"]["id"]))

    assert gone["deleted_memory_id"] == deleted["memory"]["id"]
    assert [item["id"] for item in _ok(tools.memory_list(namespace="global"))["items"]] == [kept["memory"]["id"]]
    with_archived = _ok(tools.memory_list(namespace="global", include_archived=True))["items"]
    assert {item["id"] for item in with_archived} == {kept["memory"]["id"], archived["memory"]["id"]}
    missing = _error(tools.memory_get(memory_id=deleted["memory"]["id"]))
    assert missing["error_type"] == "KeyError"


def test_memory_list_filters_by_namespace_kind_and_tags(fresh_db: Path) -> None:
    wanted = _ok(tools.memory_create(namespace="local", kind="fact", content="A", tags=["a", "b"]))
    tools.memory_create(namespace="local", kind="fact", content="B", tags=["a"])
    tools.memory_create(namespace="local", kind="note", content="C", tags=["a", "b"])
    tools.memory_create(namespace="other", kind="fact", content="D", tags=["a", "b"])

    listed = _ok(tools.memory_list(namespace="local", kind="fact", tags=["a", "b"]))

    assert [item["id"] for item in listed["items"]] == [wanted["memory"]["id"]]


def test_memory_list_filters_by_source_kind(fresh_db: Path) -> None:
    tools.memory_create(namespace="finance", kind="fact", content="Written by a worker.")
    with session_scope() as session:
        message = MemoryService(session).create_memory(
            namespace="finance", kind="note", content="Pay on the 26th.", source_kind="discord_reply"
        )
        message_id = message.id

    listed = _ok(tools.memory_list(namespace="finance", source_kind="discord_reply"))

    assert [item["id"] for item in listed["items"]] == [message_id]


def test_memory_tools_reject_missing_required_fields(fresh_db: Path) -> None:
    assert _error(tools.memory_create(namespace="global", kind="note", content="  "))["error_type"] == "ValueError"
    assert _error(tools.memory_recall(query=""))["error_type"] == "ValueError"
    assert _error(tools.memory_update(memory_id="missing", content="x"))["error_type"] == "KeyError"


def test_memory_ingest_text_makes_a_source_recallable(fresh_db: Path) -> None:
    ingested = _ok(
        tools.memory_ingest_text(
            namespace="research",
            title="Attention paper note",
            content="Attention models should preserve source provenance.",
            source_kind="test",
            source_id="attention-note",
            tags=["paper"],
        )
    )
    assert len(ingested["memory_ids"]) == 2
    assert ingested["skipped"] is False

    found = _ok(tools.memory_recall(query="source provenance", namespace="research"))
    assert {item["id"] for item in found["items"]} == set(ingested["memory_ids"])


def test_memory_ingest_text_without_a_source_id_skips_repeated_content(fresh_db: Path) -> None:
    first = _ok(tools.memory_ingest_text(namespace="research", title="Note", content="Same body twice."))
    second = _ok(tools.memory_ingest_text(namespace="research", title="Note", content="Same body twice."))
    forced = _ok(tools.memory_ingest_text(namespace="research", title="Note", content="Same body twice.", force=True))

    assert second["skipped"] is True
    assert second["reason"] == "already_ingested"
    assert second["memory_ids"] == first["memory_ids"]
    assert forced["skipped"] is False


def test_memory_ingest_artifact_reports_files_it_cannot_read(fresh_db: Path) -> None:
    with session_scope() as session:
        text_id = ArtifactStore().write_text(session, kind="worker_report", title="Report", content="Lease notes.").id
        binary_id = (
            ArtifactStore()
            .write_bytes(
                session,
                kind="worker_file",
                title="blob.bin",
                content=b"\x00\x01",
                content_type="application/octet-stream",
            )
            .id
        )

    ingested = _ok(tools.memory_ingest_artifact(artifact_id=text_id, namespace="global", tags=["lease"]))
    skipped = _ok(tools.memory_ingest_artifact(artifact_id=binary_id))

    assert ingested["ingested"] is True
    assert len(ingested["memory_ids"]) == 2
    assert skipped == {"ok": True, "ingested": False, "reason": "not_text_or_too_large"}


def test_artifact_tools_capture_read_and_mark_for_discord(fresh_db: Path, tmp_path: Path) -> None:
    source = tmp_path / "result.txt"
    source.write_text("hello from worker file", encoding="utf-8")

    captured = _ok(
        tools.artifact_capture_file(
            path=str(source), kind="worker_file", title="Result", tags=["example"], discord_upload=True
        )
    )
    artifact = captured["artifact"]
    assert artifact["tags"] == ["example", "discord_upload"]
    assert Path(artifact["local_path"]).read_text(encoding="utf-8") == "hello from worker file"

    read = _ok(tools.artifact_read_text(artifact_id=artifact["id"], intent="verify worker file"))
    assert read["text"] == "hello from worker file"


def test_artifact_capture_file_links_the_calling_work_item(
    fresh_db: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with session_scope() as session:
        definition = WorkflowService(session).create_definition(
            name="linked", version="1", definition={"nodes": [{"key": "step", "worker_kind": "manual"}]}
        )
        run_id = WorkflowService(session).start_run(workflow_definition_id=definition.id).id
    caller_id = _caller(monkeypatch, workflow_run_id=run_id)
    source = tmp_path / "notes.md"
    source.write_text("# Notes", encoding="utf-8")

    captured = _ok(tools.artifact_capture_file(path=str(source)))

    assert captured["artifact"]["work_item_id"] == caller_id
    assert captured["artifact"]["workflow_run_id"] == run_id
    assert captured["artifact"]["title"] == "notes.md"


def test_artifact_get_list_and_read_by_path(fresh_db: Path, tmp_path: Path) -> None:
    with session_scope() as session:
        work = WorkRepository(session).create_work_item(title="Owner", task_instruction="Own.", worker_kind="manual")
        store = ArtifactStore()
        owned = store.write_text(session, kind="note", title="Owned", content="owned text", work_item_id=work.id)
        store.write_text(session, kind="note", title="Tagged", content="tagged text", tags=["keep"])
        owned_id, work_id = owned.id, work.id

    fetched = _ok(tools.artifact_get(artifact_id=owned_id, include_text=True))["artifact"]
    assert fetched["text"] == "owned text"
    assert fetched["sha256"]
    assert [item["title"] for item in _ok(tools.artifact_list(work_item_id=work_id))["items"]] == ["Owned"]
    assert [item["title"] for item in _ok(tools.artifact_list(tags=["keep"]))["items"]] == ["Tagged"]
    assert [item["title"] for item in _ok(tools.artifact_list(query="tagg"))["items"]] == ["Tagged"]

    local = tmp_path / "plain.txt"
    local.write_text("0123456789", encoding="utf-8")
    assert _ok(tools.artifact_read_text(path=str(local), max_chars=4))["text"] == "0123"
    assert _error(tools.artifact_read_text())["error_type"] == "ValueError"
    assert _error(tools.artifact_get(artifact_id="missing"))["error_type"] == "KeyError"


def test_image_crop_cuts_pixel_and_normalized_boxes(fresh_db: Path, tmp_path: Path) -> None:
    source = _png(tmp_path / "wide.png", size=(40, 20))

    pixels = _ok(tools.image_crop(source=str(source), box=[10, 5, 30, 15], label="Middle Strip"))
    fractions = _ok(tools.image_crop(source=str(source), box=[0.25, 0.25, 0.75, 0.75], normalized=True))
    clamped = _ok(tools.image_crop(source=str(source), box=[-10, -10, 100, 100]))

    assert (pixels["width"], pixels["height"]) == (20, 10)
    assert _image_size(pixels["local_path"]) == (20, 10)
    assert (fractions["width"], fractions["height"]) == (20, 10)
    assert (clamped["width"], clamped["height"]) == (40, 20)
    with session_scope() as session:
        artifact = session.get(Artifact, pixels["artifact_id"])
        assert (artifact.title, artifact.kind, artifact.content_type) == ("middle-strip.png", "image_crop", "image/png")


def test_image_crop_rejects_bad_boxes_and_sources(fresh_db: Path, tmp_path: Path) -> None:
    source = _png(tmp_path / "wide.png")

    assert _error(tools.image_crop(source=str(source), box=[0, 0, 10]))["error_type"] == "ValueError"
    empty = _error(tools.image_crop(source=str(source), box=[30, 5, 10, 15]))
    assert "Empty crop box" in empty["error"]
    missing = _error(tools.image_crop(source=str(tmp_path / "nope.png"), box=[0, 0, 1, 1]))
    assert missing["error_type"] == "FileNotFoundError"


def test_image_tools_take_artifact_ids_as_sources(fresh_db: Path, tmp_path: Path) -> None:
    saved = _ok(tools.image_save(source=str(_png(tmp_path / "base.png"))))

    cropped = _ok(tools.image_crop(source=saved["artifact_id"], box=[0, 0, 10, 10]))

    with session_scope() as session:
        artifact = session.get(Artifact, cropped["artifact_id"])
        assert artifact.source_id == saved["artifact_id"]
        assert artifact.source_kind == "image_crop"


def test_image_compose_tiles_images_into_a_captioned_grid(fresh_db: Path, tmp_path: Path) -> None:
    first = _png(tmp_path / "first.png", size=(800, 400))
    second = _png(tmp_path / "second.png", size=(100, 300), color=(20, 20, 200))
    third = _png(tmp_path / "third.png", size=(50, 50))

    captioned = _ok(
        tools.image_compose(
            sources=[str(first), str(second)], labels=["front", "back"], columns=2, label="Turnaround", send=True
        )
    )
    square = _ok(tools.image_compose(sources=[str(first), str(second), str(third)]))

    assert captioned["tiles"] == 2
    assert _image_size(captioned["local_path"]) == (20 + 2 * 420, 20 + 450)
    assert _image_size(square["local_path"]) == (20 + 2 * 420, 20 + 2 * 420)
    with session_scope() as session:
        artifact = session.get(Artifact, captioned["artifact_id"])
        assert (artifact.title, artifact.kind) == ("turnaround.png", "collage")
        assert artifact.tags == ["discord_upload"]
    assert _error(tools.image_compose(sources=[]))["error_type"] == "ValueError"


def test_image_save_find_and_send(fresh_db: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    front = _ok(
        tools.image_save(source=str(_png(tmp_path / "raw-front.png")), label="Front View", tags=["avatar", "doe"])
    )
    _ok(tools.image_save(source=str(_png(tmp_path / "raw-side.png")), tags=["avatar"], kind="reference"))

    assert front["tags"] == ["avatar", "doe"]
    by_tags = _ok(tools.image_find(tags=["avatar", "doe"]))
    assert [item["label"] for item in by_tags["items"]] == ["front-view.png"]
    assert _ok(tools.image_find(query="side"))["items"][0]["label"] == "raw-side.png"
    assert _ok(tools.image_find(kind="reference"))["count"] == 1

    caller_id = _caller(monkeypatch)
    sent = _ok(tools.image_send(tags=["doe"]))

    assert sent["artifact_ids"] == [front["artifact_id"]]
    with session_scope() as session:
        artifact = session.get(Artifact, front["artifact_id"])
        assert artifact.tags == ["avatar", "doe", "discord_upload"]
        assert artifact.work_item_id == caller_id


def test_image_send_needs_a_selector_and_a_match(fresh_db: Path, tmp_path: Path) -> None:
    saved = _ok(tools.image_save(source=str(_png(tmp_path / "one.png")), send=True))

    assert _error(tools.image_send())["error_type"] == "ValueError"
    assert _error(tools.image_send(query="nothing-like-this"))["error"] == "No matching images."
    assert _ok(tools.image_send(artifact_id=saved["artifact_id"]))["artifact_ids"] == [saved["artifact_id"]]
    with session_scope() as session:
        assert session.get(Artifact, saved["artifact_id"]).tags == ["discord_upload"]


def test_image_tools_record_the_calling_work_item(
    fresh_db: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    caller_id = _caller(monkeypatch)
    source = _png(tmp_path / "shot.png")

    cropped = _ok(tools.image_crop(source=str(source), box=[0, 0, 5, 5]))
    composed = _ok(tools.image_compose(sources=[str(source)]))
    saved = _ok(tools.image_save(source=str(source)))

    with session_scope() as session:
        for result in (cropped, composed, saved):
            assert session.get(Artifact, result["artifact_id"]).work_item_id == caller_id


def test_work_tools_enqueue_and_report_status(fresh_db: Path) -> None:
    queued = _ok(
        tools.work_enqueue(
            title="Follow-up",
            task_instruction="Do the follow-up.",
            worker_kind="manual",
            context={"memory_namespace": "global"},
        )
    )

    fetched = _ok(tools.work_get(work_item_id=queued["work_item"]["id"], intent="inspect queued follow-up"))
    assert fetched["work_item"]["task_instruction"] == "Do the follow-up."
    assert fetched["work_item"]["context"] == {"memory_namespace": "global"}
    assert fetched["work_item"]["lane"] is None

    status = _ok(tools.system_status(intent="queue counts"))
    assert status["status"]["ready_work"] == 1
    assert status["status"]["running_work"] == 0


def test_work_enqueue_can_load_a_template_file(fresh_db: Path, tmp_path: Path) -> None:
    template = tmp_path / "followup.template.md"
    template.write_text("# Follow-up Template\n\nUse the maintained template.\n", encoding="utf-8")

    queued = _ok(
        tools.work_enqueue(
            title="Templated follow-up",
            task_template_path="followup.template.md",
            template_base_dir=str(tmp_path),
            worker_kind="manual",
        )
    )

    fetched = _ok(tools.work_get(work_item_id=queued["work_item"]["id"]))
    assert fetched["work_item"]["task_instruction"] == "# Follow-up Template\n\nUse the maintained template."


def test_work_enqueue_requires_exactly_one_instruction_source(fresh_db: Path, tmp_path: Path) -> None:
    neither = _error(tools.work_enqueue(title="Nothing"))
    both = _error(tools.work_enqueue(title="Both", task_instruction="Do it.", task_template_path="x.md"))
    missing = _error(tools.work_enqueue(title="Missing", task_template_path=str(tmp_path / "absent.md")))
    bad_context = _error(tools.work_enqueue(title="Bad", task_instruction="Do it.", context=["not", "an", "object"]))

    for refused in (neither, both, missing, bad_context):
        assert refused["error_type"] == "ValueError"


def test_work_enqueue_inherits_lane_reply_config_and_parent_from_the_caller(
    fresh_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    caller_id = _caller(
        monkeypatch,
        title="Finance manager",
        lane="finance-daily",
        context={
            "memory_namespace": "finance",
            "reply_memory": {"enabled": True, "namespace": "finance", "kind": "working"},
            "reply_followup_work": {
                "enabled": True,
                "title": "Finance manager reply",
                "task_instruction": "Process the finance reply.",
            },
        },
    )

    queued = _ok(
        tools.work_enqueue(
            title="Route the paycheck",
            task_instruction="Route it.",
            worker_kind="provider.default",
            context={"memory_namespace": "finance"},
        )
    )

    assert queued["work_item"]["lane"] == "finance-daily"
    with session_scope() as session:
        child = session.get(WorkItem, queued["work_item"]["id"])
        assert child.source_kind == "mcp"
        assert child.source_id == caller_id
        assert child.lane == "finance-daily"
        assert child.context["parent_work_item_id"] == caller_id
        assert child.context["reply_followup_work"]["title"] == "Finance manager reply"
        assert child.context["reply_memory"]["namespace"] == "finance"


def test_work_enqueue_keeps_explicit_child_lane_and_reply_config(
    fresh_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _caller(
        monkeypatch,
        lane="finance-daily",
        context={"reply_followup_work": {"enabled": True, "title": "Finance manager reply"}},
    )

    queued = _ok(
        tools.work_enqueue(
            title="One-shot job",
            task_instruction="Do it once.",
            context={"reply_followup_work": {"enabled": False}},
            lane="finance-oneoff",
        )
    )
    from_context = _ok(tools.work_enqueue(title="Context lane", task_instruction="Go.", context={"lane": "kitchen"}))

    with session_scope() as session:
        child = session.get(WorkItem, queued["work_item"]["id"])
        assert child.context["reply_followup_work"] == {"enabled": False}
        assert child.lane == "finance-oneoff"
        assert session.get(WorkItem, from_context["work_item"]["id"]).lane == "kitchen"


def test_work_enqueue_with_a_disabled_reply_keeps_the_callers_config_out(
    fresh_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _caller(monkeypatch, context={"reply_followup_work": {"enabled": True, "title": "Reply"}})

    queued = _ok(tools.work_enqueue(title="Quiet", task_instruction="Go.", context={"reply_followup_disabled": True}))

    with session_scope() as session:
        assert "reply_followup_work" not in session.get(WorkItem, queued["work_item"]["id"]).context


def test_work_list_filters_by_status_and_lane(fresh_db: Path) -> None:
    kitchen = _ok(tools.work_enqueue(title="Kitchen", task_instruction="Cook.", worker_kind="manual", lane="kitchen"))
    finance = _ok(tools.work_enqueue(title="Finance", task_instruction="Pay.", worker_kind="manual", lane="finance"))
    _ok(tools.work_cancel(work_item_id=finance["work_item"]["id"]))

    by_lane = _ok(tools.work_list(lane="kitchen"))["items"]
    by_status = _ok(tools.work_list(status=["canceled"]))["items"]
    single_status = _ok(tools.work_list(status="ready"))["items"]

    assert [item["id"] for item in by_lane] == [kitchen["work_item"]["id"]]
    assert [item["id"] for item in by_status] == [finance["work_item"]["id"]]
    assert [item["id"] for item in single_status] == [kitchen["work_item"]["id"]]


def test_work_events_cancel_and_retry(fresh_db: Path) -> None:
    queued = _ok(tools.work_enqueue(title="Job", task_instruction="Go.", worker_kind="manual"))
    work_id = queued["work_item"]["id"]

    canceled = _ok(tools.work_cancel(work_item_id=work_id))
    events = _ok(tools.work_events(work_item_id=work_id))["items"]

    assert canceled["work_item"]["status"] == "canceled"
    assert [event["event_type"] for event in events] == ["work.canceled", "work.created"]

    with session_scope() as session:
        session.get(WorkItem, work_id).status = "dead_letter"
    retried = _ok(tools.work_retry(work_item_id=work_id))
    assert retried["work_item"]["status"] == "ready"
    assert _error(tools.work_get(work_item_id="missing"))["error_type"] == "KeyError"


def test_schedule_tools_create_and_list_a_work_schedule(fresh_db: Path) -> None:
    created = _ok(
        tools.schedule_create_work(
            name="Daily check",
            schedule_type="cron",
            expression="0 9 * * *",
            task_instruction="Run the daily check.",
            worker_kind="provider.default",
            context={"memory_namespace": "personal"},
            runtime_contract={"model_profile": "low"},
            lane="checks",
        )
    )

    schedule = created["schedule"]
    assert schedule["name"] == "Daily check"
    assert schedule["timezone"] == "America/Los_Angeles"
    assert schedule["payload"] == {
        "title": "Daily check",
        "task_instruction": "Run the daily check.",
        "lane": "checks",
        "context": {"memory_namespace": "personal"},
    }
    assert schedule["runtime_contract"] == {"model_profile": "low"}

    listed = _ok(tools.schedule_list(enabled=True, intent="inspect schedules"))
    assert [item["id"] for item in listed["items"]] == [schedule["id"]]
    assert _ok(tools.schedule_list(enabled=False))["items"] == []


def test_schedule_tools_create_get_update_toggle_fire_and_delete(fresh_db: Path) -> None:
    created = _ok(
        tools.schedule_create_work(
            name="watch: cooking classes",
            schedule_type="cron",
            expression="0 9 * * FRI",
            task_instruction="Check for new cooking classes and post any to the thread.",
            worker_kind="provider.default",
            context={"memory_namespace": "local", "watch": {"query": "cooking classes"}},
            discord_thread_id="scout-thread-1",
        )
    )
    schedule_id = created["schedule"]["id"]
    assert created["schedule"]["enabled"] is True
    assert created["schedule"]["payload"]["discord_thread_id"] == "scout-thread-1"

    got = _ok(tools.schedule_get(schedule_id=schedule_id))
    assert got["schedule"]["payload"]["context"]["watch"]["query"] == "cooking classes"

    updated = _ok(
        tools.schedule_update(
            schedule_id=schedule_id,
            expression="0 18 * * SAT",
            context={"memory_namespace": "local", "watch": {"query": "cooking classes", "level": "beginner"}},
        )
    )
    assert updated["schedule"]["expression"] == "0 18 * * SAT"
    assert updated["schedule"]["payload"]["context"]["watch"]["level"] == "beginner"
    assert updated["schedule"]["payload"]["discord_thread_id"] == "scout-thread-1"

    assert _ok(tools.schedule_set_enabled(schedule_id=schedule_id, enabled=False))["schedule"]["enabled"] is False
    assert _ok(tools.schedule_set_enabled(schedule_id=schedule_id, enabled=True))["schedule"]["enabled"] is True

    fired = _ok(tools.schedule_fire_now(schedule_id=schedule_id))
    assert fired["work_item_id"]
    assert fired["workflow_run_id"] is None
    with session_scope() as session:
        work = session.get(WorkItem, fired["work_item_id"])
        assert work.source_kind == "schedule"
        assert work.discord_thread_id == "scout-thread-1"
        assert work.lane == "watch: cooking classes"

    deleted = _ok(tools.schedule_delete(schedule_id=schedule_id))
    assert deleted["deleted_schedule_id"] == schedule_id
    assert _error(tools.schedule_get(schedule_id=schedule_id))["error_type"] == "KeyError"


def test_schedule_update_can_clear_the_thread(fresh_db: Path) -> None:
    created = _ok(
        tools.schedule_create_work(
            name="Threaded",
            schedule_type="interval",
            expression="hours=6",
            task_instruction="Post.",
            discord_thread_id="thread-9",
        )
    )

    updated = _ok(tools.schedule_update(schedule_id=created["schedule"]["id"], discord_thread_id=""))

    assert "discord_thread_id" not in updated["schedule"]["payload"]
    assert updated["schedule"]["payload"]["task_instruction"] == "Post."


def test_schedule_update_renames_the_work_it_queues(fresh_db: Path) -> None:
    created = _ok(
        tools.schedule_create_work(
            name="Morning desk", schedule_type="cron", expression="30 6 * * *", task_instruction="Post."
        )
    )
    schedule_id = created["schedule"]["id"]

    renamed = _ok(tools.schedule_update(schedule_id=schedule_id, name="Front desk"))
    fired = _ok(tools.schedule_fire_now(schedule_id=schedule_id))

    assert renamed["schedule"]["payload"]["title"] == "Front desk"
    with session_scope() as session:
        work = session.get(WorkItem, fired["work_item_id"])
        assert (work.title, work.lane) == ("Front desk", "Front desk")


def test_schedule_update_resumes_a_paused_schedule_from_now(fresh_db: Path) -> None:
    created = _ok(
        tools.schedule_create_work(
            name="Paused", schedule_type="interval", expression="hours=1", task_instruction="Go.", enabled=False
        )
    )
    schedule_id = created["schedule"]["id"]

    resumed = _ok(tools.schedule_update(schedule_id=schedule_id, enabled=True))
    paused = _ok(tools.schedule_update(schedule_id=schedule_id, enabled=False, expression="hours=2"))

    assert resumed["schedule"]["enabled"] is True
    assert resumed["schedule"]["last_evaluated_at"] is not None
    assert (paused["schedule"]["enabled"], paused["schedule"]["expression"]) == (False, "hours=2")
    assert _error(tools.schedule_update(schedule_id="missing", enabled=True))["error_type"] == "KeyError"
    assert _error(tools.schedule_update(schedule_id=schedule_id, expression="daily"))["error_type"] == "ValueError"


def test_schedule_create_work_can_reference_a_template_file(fresh_db: Path, tmp_path: Path) -> None:
    template = tmp_path / "scheduled.template.md"
    template.write_text("# Scheduled Template\n\nRun from Markdown.", encoding="utf-8")

    created = _ok(
        tools.schedule_create_work(
            name="Templated daily check",
            schedule_type="cron",
            expression="0 9 * * *",
            task_template_path="scheduled.template.md",
            template_base_dir=str(tmp_path),
            worker_kind="provider.default",
            context={"memory_namespace": "personal"},
        )
    )

    payload = created["schedule"]["payload"]
    assert payload["task_template_path"] == "scheduled.template.md"
    assert payload["template_base_dir"] == str(tmp_path)
    assert payload["context"]["memory_namespace"] == "personal"
    assert "task_instruction" not in payload


def test_schedule_create_work_rejects_bad_input(fresh_db: Path) -> None:
    both = _error(
        tools.schedule_create_work(
            name="Both", schedule_type="cron", expression="0 9 * * *", task_instruction="x", task_template_path="y.md"
        )
    )
    bad_type = _error(
        tools.schedule_create_work(name="Bad", schedule_type="weekly", expression="MON", task_instruction="x")
    )
    bad_timezone = _error(
        tools.schedule_create_work(
            name="Bad tz", schedule_type="cron", expression="0 9 * * *", task_instruction="x", timezone_name="Mars/Base"
        )
    )

    assert "exactly one" in both["error"]
    assert bad_type["error_type"] == "ValueError"
    assert bad_timezone["error_type"] == "ZoneInfoNotFoundError"
    with session_scope() as session:
        assert session.scalar(select(func.count()).select_from(Schedule)) == 0


def test_schedule_create_work_inherits_reply_config(fresh_db: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _caller(
        monkeypatch,
        title="Finance manager",
        context={"reply_followup_work": {"enabled": True, "title": "Finance manager reply"}},
    )

    created = _ok(
        tools.schedule_create_work(
            name="Settlement follow-up",
            schedule_type="cron",
            expression="0 9 * * *",
            task_instruction="Check the settlement.",
            worker_kind="provider.default",
            context={"memory_namespace": "finance"},
        )
    )

    payload = created["schedule"]["payload"]
    assert payload["context"]["reply_followup_work"]["title"] == "Finance manager reply"
    assert payload["context"]["memory_namespace"] == "finance"
    assert "parent_work_item_id" not in payload["context"]


def test_workflow_tools_list_and_start_workflow(fresh_db: Path) -> None:
    with session_scope() as session:
        WorkflowService(session).create_definition(
            name="daily-gmail-cleanup",
            version="1",
            definition={
                "nodes": [
                    {"key": "noop", "kind": "work", "task_instruction": "Run once.", "worker_kind": "function.echo"}
                ]
            },
        )

    listed = _ok(tools.workflow_list(enabled=True, intent="find email cleanup workflow"))
    assert listed["items"][0]["name"] == "daily-gmail-cleanup"
    assert listed["items"][0]["node_count"] == 1

    started = _ok(
        tools.workflow_start(
            workflow_name="daily-gmail-cleanup",
            run_name="Manual email cleanup",
            input={"source": "test"},
            discord_thread_id="thread-7",
        )
    )
    run_id = started["workflow_run"]["id"]
    assert started["workflow_run"]["name"] == "Manual email cleanup"

    with session_scope() as session:
        run = session.get(WorkflowRun, run_id)
        assert run.input["source"] == "test"
        assert run.discord_thread_id == "thread-7"


def test_workflow_start_by_definition_id_and_unknown_names(fresh_db: Path) -> None:
    with session_scope() as session:
        definition_id = (
            WorkflowService(session)
            .create_definition(name="by-id", version="2", definition={"nodes": [{"key": "only"}]})
            .id
        )

    started = _ok(tools.workflow_start(workflow_definition_id=definition_id))
    assert started["workflow_run"]["name"] == "by-id"
    assert _ok(tools.workflow_start(workflow_name="by-id", version="2"))["workflow_definition"]["version"] == "2"
    assert _error(tools.workflow_start(workflow_name="by-id"))["error_type"] == "KeyError"
    assert _error(tools.workflow_start(workflow_name="by-id", version="2", input=[1]))["error_type"] == "ValueError"


def test_submit_worker_result_deposits_in_the_inbox(fresh_db: Path) -> None:
    token = results.mint_token()

    submitted = _ok(
        tools.submit_worker_result(
            result_token=token, report="Workout report", summary="Workout summary", produces={"focus": "push"}
        )
    )
    assert submitted["result_token"] == token

    assert results.read_and_consume(token) == {
        "status": "succeeded",
        "report": "Workout report",
        "summary": "Workout summary",
        "produces": {"focus": "push"},
        "error": None,
        "work_item_id": None,
    }


def test_submit_worker_result_carries_the_work_item_id(fresh_db: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TASQUE2_WORK_ITEM_ID", "work-123")
    token = results.mint_token()

    _ok(
        tools.submit_worker_result(
            result_token=token, report="Blocked report", summary="Blocked", status=" Blocked ", error="Needs a code."
        )
    )

    with session_scope() as session:
        payload = results.consume_for_work_item(session, "work-123")
        assert payload["work_item_id"] == "work-123"
        assert (payload["status"], payload["error"]) == ("blocked", "Needs a code.")
        assert results.consume_for_work_item(session, "work-123") is None
    assert results.read_and_consume(token) is None


def test_submit_worker_result_validates_its_arguments(fresh_db: Path) -> None:
    assert _error(tools.submit_worker_result(result_token=" ", summary="s", report="r"))["error_type"] == "ValueError"
    bad_produces = _error(tools.submit_worker_result(result_token="t", summary="s", report="r", produces=["x"]))
    assert "produces" in bad_produces["error"]
    assert _error(tools.submit_worker_result(result_token="t", summary=None, report="r"))["error_type"] == "ValueError"
    assert results.peek("t") is False


def test_discord_history_returns_the_users_messages_in_a_lane_newest_first(fresh_db: Path) -> None:
    with session_scope() as session:
        owner = WorkRepository(session).create_work_item(
            title="Finance opener", task_instruction="Open.", worker_kind="manual", lane="finance"
        )
        discord = DiscordService(session)
        discord.bind_thread(purpose="work", discord_channel_id="jobs", discord_thread_id="t-fin", work_item_id=owner.id)
        messages = [
            ("m1", "t-fin", "inbound", "Remind me to pay on the 26th."),
            ("m2", "t-fin", "outbound", "Noted: a reminder on the 26th."),
            ("m3", "t-fin", "inbound", "TRIP does accept credit cards."),
            ("m4", "t-other", "inbound", "Add the green cap to the wardrobe."),
        ]
        for message_id, thread_id, direction, content in messages:
            discord.record_message(
                discord_message_id=message_id,
                discord_channel_id="jobs",
                discord_thread_id=thread_id,
                direction=direction,
                author="user" if direction == "inbound" else "tasque",
                content_preview=content,
            )

    lane = _ok(tools.discord_history(lane="finance"))["items"]
    matched = _ok(tools.discord_history(thread_id="t-fin", query="credit cards"))["items"]
    everything = _ok(tools.discord_history(lane="finance", include_tasque=True))["items"]

    assert [item["content"] for item in lane] == ["TRIP does accept credit cards.", "Remind me to pay on the 26th."]
    assert [item["content"] for item in matched] == ["TRIP does accept credit cards."]
    assert [item["from"] for item in everything] == ["user", "tasque", "user"]
