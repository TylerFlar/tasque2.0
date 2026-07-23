from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy import select

from tasque2 import android as android_device
from tasque2 import result_inbox
from tasque2.artifacts import ArtifactService, ArtifactStore
from tasque2.db import session_scope
from tasque2.memory import MemoryService
from tasque2.memory_ingest import MemoryIngestService
from tasque2.models import (
    Artifact,
    Memory,
    Schedule,
    WorkEvent,
    WorkflowDefinition,
    WorkflowRun,
    WorkItem,
)
from tasque2.queue import WorkQueue
from tasque2.repo import WorkRepository
from tasque2.scheduler import ScheduleService
from tasque2.status import get_system_status
from tasque2.templates import read_template_file
from tasque2.weather import fetch_local_weather
from tasque2.workflows import WorkflowService


def memory_search(
    intent: str,
    query: str | None = None,
    namespace: str | None = None,
    tags: list[str] | None = None,
    limit: int = 10,
) -> str:
    """Search memories by natural language-ish terms, namespace, or tags."""
    return _run_json(
        lambda: _memory_search(
            query=query,
            namespace=namespace,
            tags=tags,
            limit=limit,
        ),
        intent=intent,
    )


def memory_search_any(
    intent: str,
    keywords: list[str],
    namespace: str | None = None,
    tags: list[str] | None = None,
    limit: int = 10,
) -> str:
    """Search memories with several keyword probes and return de-duplicated matches."""
    return _run_json(
        lambda: _memory_search_any(
            keywords=keywords,
            namespace=namespace,
            tags=tags,
            limit=limit,
        ),
        intent=intent,
    )


def memory_list(
    intent: str,
    namespace: str | None = None,
    kind: str | None = None,
    tags: list[str] | None = None,
    include_archived: bool = False,
    limit: int = 20,
) -> str:
    """List recent memories, optionally scoped by namespace, kind, or tags."""
    return _run_json(
        lambda: _memory_list(
            namespace=namespace,
            kind=kind,
            tags=tags,
            include_archived=include_archived,
            limit=limit,
        ),
        intent=intent,
    )


def memory_get(intent: str, memory_id: str) -> str:
    """Fetch one memory by id."""
    return _run_json(lambda: _memory_get(memory_id), intent=intent)


def memory_get_canonical(intent: str, namespace: str, canonical_key: str) -> str:
    """Fetch the active canonical memory for a namespace/key pair."""
    return _run_json(
        lambda: _memory_get_canonical(namespace=namespace, canonical_key=canonical_key),
        intent=intent,
    )


def memory_create(
    namespace: str,
    kind: str,
    content: str,
    tags: list[str] | None = None,
    canonical_key: str | None = None,
    pinned: bool = False,
    ttl_days: int | None = None,
    source: str = "mcp",
    work_item_id: str | None = None,
) -> str:
    """Create a durable or working memory."""
    return _run_json(
        lambda: _memory_create(
            namespace=namespace,
            kind=kind,
            content=content,
            tags=tags,
            canonical_key=canonical_key,
            pinned=pinned,
            ttl_days=ttl_days,
            source=source,
            work_item_id=work_item_id,
        )
    )


def memory_upsert_canonical(
    namespace: str,
    canonical_key: str,
    kind: str,
    content: str,
    tags: list[str] | None = None,
    pinned: bool = False,
    ttl_days: int | None = None,
    source: str = "mcp",
    work_item_id: str | None = None,
) -> str:
    """Replace the active canonical memory for a namespace/key pair."""
    return _run_json(
        lambda: _memory_upsert_canonical(
            namespace=namespace,
            canonical_key=canonical_key,
            kind=kind,
            content=content,
            tags=tags,
            pinned=pinned,
            ttl_days=ttl_days,
            source=source,
            work_item_id=work_item_id,
        )
    )


def memory_supersede(memory_id: str, content: str, tags: list[str] | None = None) -> str:
    """Archive an old memory and create a replacement carrying its metadata."""
    return _run_json(lambda: _memory_supersede(memory_id=memory_id, content=content, tags=tags))


def memory_archive(memory_id: str) -> str:
    """Archive a memory without deleting it."""
    return _run_json(lambda: _memory_archive(memory_id=memory_id))


def memory_recall(
    intent: str,
    query: str,
    namespace: str | None = None,
    tags: list[str] | None = None,
    limit: int = 8,
) -> str:
    """Recall the most relevant memories via hybrid (semantic + keyword) search.

    Prefer this over memory_search when you want the best items to reason with:
    it fuses vector similarity, keyword match, recency, and importance, and
    returns items already ranked with a relevance ``score``. Pass the namespace
    to scope the recall to one domain (e.g. ``local``, ``finance``, ``health``).
    """
    return _run_json(
        lambda: _memory_recall(query=query, namespace=namespace, tags=tags, limit=limit),
        intent=intent,
    )


def memory_update(
    memory_id: str,
    content: str | None = None,
    tags: list[str] | None = None,
    importance: int | None = None,
) -> str:
    """Edit ONE memory item in place — the fact-level update primitive.

    Use this to change a single discrete fact instead of rewriting a whole
    canonical document: no new row, no archived copy. ``importance`` is an
    optional 1-5 salience that boosts the item in future recall.
    """
    return _run_json(
        lambda: _memory_update(
            memory_id=memory_id, content=content, tags=tags, importance=importance
        )
    )


def memory_delete(memory_id: str) -> str:
    """Permanently delete ONE memory item — fact-level unlearning.

    Use for a stale or contradicted fact. Unlike memory_archive this removes the
    row and its search/embedding index entirely (real forgetting).
    """
    return _run_json(lambda: _memory_delete(memory_id=memory_id))


def memory_ingest_text(
    namespace: str,
    title: str,
    content: str,
    source_kind: str = "mcp_text",
    source_id: str | None = None,
    tags: list[str] | None = None,
    work_item_id: str | None = None,
    force: bool = False,
) -> str:
    """Ingest a text source into searchable summary/chunk memories."""
    return _run_json(
        lambda: _memory_ingest_text(
            namespace=namespace,
            title=title,
            content=content,
            source_kind=source_kind,
            source_id=source_id,
            tags=tags,
            work_item_id=work_item_id,
            force=force,
        )
    )


def memory_ingest_artifact(
    artifact_id: str,
    namespace: str | None = None,
    tags: list[str] | None = None,
    max_bytes: int = 2_000_000,
    force: bool = False,
) -> str:
    """Ingest a text artifact into searchable summary/chunk memories."""
    return _run_json(
        lambda: _memory_ingest_artifact(
            artifact_id=artifact_id,
            namespace=namespace,
            tags=tags,
            max_bytes=max_bytes,
            force=force,
        )
    )


def memory_ingest_pending(limit: int = 25) -> str:
    """Ingest pending text artifacts and inbound Discord messages."""
    return _run_json(lambda: _memory_ingest_pending(limit=limit))


def todo_write(
    scope: str,
    items: list[Any],
    namespace: str = "global",
    work_item_id: str | None = None,
) -> str:
    """Write a durable todo/checklist memory for coordination."""
    return _run_json(
        lambda: _todo_write(
            scope=scope,
            items=items,
            namespace=namespace,
            work_item_id=work_item_id,
        )
    )


def ask_user(
    question: str,
    context: str | None = None,
    namespace: str = "global",
    work_item_id: str | None = None,
) -> str:
    """Record a blocking or useful question for the user."""
    return _run_json(
        lambda: _ask_user(
            question=question,
            context=context,
            namespace=namespace,
            work_item_id=work_item_id,
        )
    )


def artifact_list(
    intent: str,
    query: str | None = None,
    kind: str | None = None,
    tags: list[str] | None = None,
    work_item_id: str | None = None,
    source_kind: str | None = None,
    include_archived: bool = False,
    limit: int = 20,
) -> str:
    """List/search artifact metadata and local paths."""
    return _run_json(
        lambda: _artifact_list(
            query=query,
            kind=kind,
            tags=tags,
            work_item_id=work_item_id,
            source_kind=source_kind,
            include_archived=include_archived,
            limit=limit,
        ),
        intent=intent,
    )


def artifact_get(intent: str, artifact_id: str, include_text: bool = False, max_chars: int = 20000) -> str:
    """Fetch artifact metadata and optionally a text preview from its local file."""
    return _run_json(
        lambda: _artifact_get(artifact_id=artifact_id, include_text=include_text, max_chars=max_chars),
        intent=intent,
    )


def artifact_read_text(
    intent: str,
    artifact_id: str | None = None,
    path: str | None = None,
    max_chars: int = 20000,
) -> str:
    """Read a text artifact or local path. Binary files return metadata plus an error."""
    return _run_json(
        lambda: _artifact_read_text(artifact_id=artifact_id, path=path, max_chars=max_chars),
        intent=intent,
    )


def artifact_capture_file(
    path: str,
    kind: str = "worker_file",
    title: str | None = None,
    tags: list[str] | None = None,
    discord_upload: bool = False,
    work_item_id: str | None = None,
    workflow_run_id: str | None = None,
    source: str = "mcp",
) -> str:
    """Copy a local file into Tasque artifact storage and return its artifact id/path."""
    return _run_json(
        lambda: _artifact_capture_file(
            path=path,
            kind=kind,
            title=title,
            tags=tags,
            discord_upload=discord_upload,
            work_item_id=work_item_id,
            workflow_run_id=workflow_run_id,
            source=source,
        )
    )


def artifact_archive(artifact_id: str) -> str:
    """Archive artifact metadata without deleting the local file."""
    return _run_json(lambda: _artifact_archive(artifact_id=artifact_id))


def work_enqueue(
    title: str,
    task_instruction: str | None = None,
    worker_kind: str = "provider.default",
    runtime_contract: dict[str, Any] | None = None,
    context: dict[str, Any] | None = None,
    priority: int = 0,
    max_attempts: int = 1,
    idempotency_key: str | None = None,
    workflow_run_id: str | None = None,
    discord_thread_id: str | None = None,
    task_template_path: str | None = None,
    template_base_dir: str | None = None,
) -> str:
    """Queue a normal Tasque WorkItem from direct instructions or a Markdown template file."""
    return _run_json(
        lambda: _work_enqueue(
            title=title,
            task_instruction=task_instruction,
            worker_kind=worker_kind,
            runtime_contract=runtime_contract,
            context=context,
            priority=priority,
            max_attempts=max_attempts,
            idempotency_key=idempotency_key,
            workflow_run_id=workflow_run_id,
            discord_thread_id=discord_thread_id,
            task_template_path=task_template_path,
            template_base_dir=template_base_dir,
        )
    )


def work_list(
    intent: str,
    status: str | list[str] | None = None,
    worker_kind: str | None = None,
    limit: int = 20,
) -> str:
    """List recent work items."""
    return _run_json(
        lambda: _work_list(status=status, worker_kind=worker_kind, limit=limit),
        intent=intent,
    )


def work_get(intent: str, work_item_id: str) -> str:
    """Fetch one WorkItem with latest attempt metadata."""
    return _run_json(lambda: _work_get(work_item_id=work_item_id), intent=intent)


def work_events(intent: str, work_item_id: str, limit: int = 50) -> str:
    """List recent event history for a WorkItem."""
    return _run_json(lambda: _work_events(work_item_id=work_item_id, limit=limit), intent=intent)


def work_pause(work_item_id: str) -> str:
    """Pause a non-terminal WorkItem."""
    return _run_json(lambda: _work_transition(work_item_id=work_item_id, action="pause"))


def work_resume(work_item_id: str) -> str:
    """Resume a paused WorkItem."""
    return _run_json(lambda: _work_transition(work_item_id=work_item_id, action="resume"))


def work_cancel(work_item_id: str) -> str:
    """Cancel or request cancellation for a WorkItem."""
    return _run_json(lambda: _work_transition(work_item_id=work_item_id, action="cancel"))


def work_retry(work_item_id: str) -> str:
    """Return a dead-lettered WorkItem to the ready queue."""
    return _run_json(lambda: _work_transition(work_item_id=work_item_id, action="retry"))


def schedule_create_work(
    name: str,
    schedule_type: str,
    expression: str,
    task_instruction: str | None = None,
    worker_kind: str = "provider.default",
    timezone_name: str | None = None,
    runtime_contract: dict[str, Any] | None = None,
    context: dict[str, Any] | None = None,
    catchup_policy: str = "coalesce",
    max_backfill: int = 10,
    enabled: bool = True,
    task_template_path: str | None = None,
    template_base_dir: str | None = None,
    discord_thread_id: str | None = None,
) -> str:
    """Create a recurring or one-time schedule ("watch") that enqueues a WorkItem.

    schedule_type is "cron" (expression e.g. "0 9 * * FRI"), "interval"
    (e.g. "hours=6"), or "date" (one-time; expression an ISO datetime). Pass
    discord_thread_id to deliver each firing's result back into that thread.
    """
    return _run_json(
        lambda: _schedule_create_work(
            name=name,
            schedule_type=schedule_type,
            expression=expression,
            task_instruction=task_instruction,
            worker_kind=worker_kind,
            timezone_name=timezone_name,
            runtime_contract=runtime_contract,
            context=context,
            catchup_policy=catchup_policy,
            max_backfill=max_backfill,
            enabled=enabled,
            task_template_path=task_template_path,
            template_base_dir=template_base_dir,
            discord_thread_id=discord_thread_id,
        )
    )


def schedule_list(intent: str, enabled: bool | None = None, limit: int = 20) -> str:
    """List recent schedules ("watches")."""
    return _run_json(lambda: _schedule_list(enabled=enabled, limit=limit), intent=intent)


def schedule_get(schedule_id: str) -> str:
    """Get one schedule ("watch") by id, including its full payload."""
    return _run_json(lambda: _schedule_get(schedule_id=schedule_id))


def schedule_update(
    schedule_id: str,
    name: str | None = None,
    schedule_type: str | None = None,
    expression: str | None = None,
    timezone_name: str | None = None,
    context: dict[str, Any] | None = None,
    catchup_policy: str | None = None,
    max_backfill: int | None = None,
    enabled: bool | None = None,
    discord_thread_id: str | None = None,
) -> str:
    """Update a schedule ("watch"): cadence (schedule_type/expression), context,
    enabled, etc. Only the fields you pass change; context/discord_thread_id are
    merged into the existing payload."""
    return _run_json(
        lambda: _schedule_update(
            schedule_id=schedule_id,
            name=name,
            schedule_type=schedule_type,
            expression=expression,
            timezone_name=timezone_name,
            context=context,
            catchup_policy=catchup_policy,
            max_backfill=max_backfill,
            enabled=enabled,
            discord_thread_id=discord_thread_id,
        )
    )


def schedule_set_enabled(schedule_id: str, enabled: bool) -> str:
    """Enable (resume) or disable (pause) a schedule ("watch") without deleting it."""
    return _run_json(
        lambda: _schedule_set_enabled(schedule_id=schedule_id, enabled=enabled)
    )


def schedule_delete(schedule_id: str) -> str:
    """Delete a schedule ("watch") permanently."""
    return _run_json(lambda: _schedule_delete(schedule_id=schedule_id))


def schedule_fire_now(schedule_id: str) -> str:
    """Fire a schedule ("watch") immediately, in addition to its normal cadence."""
    return _run_json(lambda: _schedule_fire_now(schedule_id=schedule_id))


def workflow_list(intent: str, enabled: bool | None = None, limit: int = 20) -> str:
    """List workflow definitions available to start."""
    return _run_json(lambda: _workflow_list(enabled=enabled, limit=limit), intent=intent)


def workflow_start(
    workflow_name: str | None = None,
    workflow_definition_id: str | None = None,
    version: str = "1",
    run_name: str | None = None,
    input: dict[str, Any] | None = None,
    discord_thread_id: str | None = None,
) -> str:
    """Start a WorkflowRun by workflow name or definition id."""
    return _run_json(
        lambda: _workflow_start(
            workflow_name=workflow_name,
            workflow_definition_id=workflow_definition_id,
            version=version,
            run_name=run_name,
            input=input,
            discord_thread_id=discord_thread_id,
        )
    )


def weather_now(intent: str, days: int = 3) -> str:
    """Read the local weather: current conditions + daily forecast.

    Returns current temp/feels-like/wind/conditions and per-day high/low,
    feels-like range, rain chance, and sunset for the configured home location
    — for dressing to the actual day (fabric weight, layers, rain) rather than
    the season. days = how many forecast days to include (1-7).
    """
    return _run_json(lambda: {"ok": True, "weather": fetch_local_weather(days=days)}, intent=intent)


def android_status(intent: str) -> str:
    """Check the Android automation device: attached devices, screen size, lease.

    Safe to call anytime (takes no lease). Reports the configured adb path and
    serial, every attached device with its state, the screen dimensions, whether
    the ADBKeyBoard IME (needed for non-ASCII typing) is installed, and who
    currently holds the device lease. Start any device session here.
    """
    return _run_json(lambda: {"ok": True, "android": android_device.device_status()}, intent=intent)


def android_reconnect(intent: str) -> str:
    """(Re)establish the wireless adb link to the phone if it has dropped.

    android_status already does this automatically at session start; call this
    directly only to recover mid-session if a device action fails with a
    connection error. Heals wifi blips and slept-phone drops; it cannot revive
    the link after a phone reboot (that needs re-enabling Wireless Debugging /
    adb tcpip per dating_ops).
    """
    return _run_json(
        lambda: {"ok": True, "reconnect": android_device.ensure_connected()}, intent=intent
    )


def android_unlock(intent: str) -> str:
    """Wake the Android device and dismiss the lock screen for a session.

    Call this once at the start of any device session (right after
    android_status), before launching apps. Wakes the screen and, if locked,
    enters the configured PIN; a no-op if already unlocked. If it reports the
    device still locked (or no PIN configured), stop the device work and ask
    the user to unlock the phone by hand — do not keep retrying.
    """
    return _run_json(lambda: _android_action(lambda: android_device.unlock()))


def android_screenshot(intent: str, label: str | None = None) -> str:
    """Capture the Android device screen to a local PNG you can Read to see it.

    Returns the file path plus pixel width/height — tap/swipe coordinates are
    in this same pixel space. The loop for driving any app is: screenshot →
    Read the path to view it → act (android_tap / android_swipe / android_type)
    → screenshot again to verify the screen changed as expected. ``label`` goes
    into the filename for later reference. To show the user a screenshot in
    Discord, pass its path to image_save with send=true.
    """
    return _run_json(lambda: _android_action(lambda: android_device.take_screenshot(label)))


def android_ui(intent: str, max_nodes: int = 120) -> str:
    """Dump the current Android view hierarchy as labelled elements with centers.

    Each node carries text / content-desc / resource-id, bounds, a ready-to-tap
    ``center``, and whether it is clickable — use it to find exact coordinates
    for buttons and fields instead of eyeballing the screenshot. Caveat: it
    occasionally captures only an overlay and misses the content behind it; the
    screenshot stays the source of truth.
    """
    return _run_json(lambda: _android_action(lambda: android_device.ui_dump(max_nodes=max_nodes)))


def android_apps(intent: str, query: str | None = None) -> str:
    """List installed Android package names, optionally filtered by substring.

    Use once during setup to discover exact package names (e.g. query "hinge"
    or "bumble") for android_launch; record them in a memory so future sessions
    skip this call.
    """
    return _run_json(
        lambda: {"ok": True, "packages": android_device.list_packages(query)}, intent=intent
    )


def android_tap(x: int, y: int) -> str:
    """Tap the Android screen at pixel (x, y) in screenshot coordinates.

    Verify with a fresh android_screenshot afterwards before chaining further
    actions — never fire taps blind.
    """
    return _run_json(lambda: _android_action(lambda: android_device.tap(x, y) or {"tapped": [x, y]}))


def android_swipe(x1: int, y1: int, x2: int, y2: int, duration_ms: int = 300) -> str:
    """Swipe/drag on the Android screen from (x1, y1) to (x2, y2).

    Longer duration_ms (500-800) reads as a deliberate drag (e.g. reordering);
    short (150-300) as a scroll flick. Coordinates are screenshot pixels.
    """
    return _run_json(
        lambda: _android_action(
            lambda: android_device.swipe(x1, y1, x2, y2, duration_ms)
            or {"swiped": [[x1, y1], [x2, y2]], "duration_ms": duration_ms}
        )
    )


def android_type(text: str) -> str:
    """Type text into the currently focused Android input field.

    Tap the field first (cursor visible), then type. ASCII is typed directly;
    newlines become ENTER presses. Non-ASCII (emoji, accents) requires the
    ADBKeyBoard IME on the device — without it this errors instead of mangling
    the text, so keep drafts ASCII unless android_status shows it installed.
    Screenshot afterwards to confirm what actually landed in the field.
    """
    return _run_json(lambda: _android_action(lambda: android_device.type_text(text)))


def android_key(key: str) -> str:
    """Press one Android key: a name (back, home, enter, delete, tab, app_switch,
    paste, dpad_up/down/left/right, page_up/page_down...) or a numeric keycode.

    ``back`` dismisses keyboards/dialogs; ``home`` bails out of a broken flow
    before relaunching the app.
    """
    return _run_json(
        lambda: _android_action(lambda: {"keycode": android_device.press_key(key)})
    )


def android_launch(package: str, relaunch: bool = False) -> str:
    """Launch an Android app by package name (see android_apps to find it).

    relaunch=true force-stops the app first — the recovery move when an app is
    wedged on an unexpected screen. After launching, screenshot to see where
    the app actually opened.
    """
    return _run_json(
        lambda: _android_action(
            lambda: android_device.launch_app(package, relaunch=relaunch)
            or {"launched": package, "relaunched": relaunch}
        )
    )


def android_push_photo(path: str, name: str | None = None) -> str:
    """Copy a local image onto the Android device so app photo pickers see it.

    ``path`` is a local file (e.g. an artifact local_path from a photo the user
    sent). Pushes to the device gallery and triggers a MediaStore scan; the
    image then appears in the app's photo picker under Pictures/tasque. Returns
    the device path.
    """
    return _run_json(lambda: _android_action(lambda: android_device.push_photo(path, name)))


def system_status(intent: str) -> str:
    """Summarize queue, scheduler, and workflow counts."""
    return _run_json(lambda: _system_status(), intent=intent)


def submit_worker_result(
    result_token: str,
    report: str,
    summary: str,
    produces: dict[str, Any] | None = None,
    status: str = "succeeded",
    error: str | None = None,
) -> str:
    """Submit the authoritative structured result for this worker run.

    Call exactly once near the end of the coordinator turn with the result_token
    from the prompt. Tasque reads this payload after the provider process exits;
    provider stdout is not used as a result fallback.
    """
    return _run_json(
        lambda: _submit_worker_result(
            result_token=result_token,
            report=report,
            summary=summary,
            produces=produces,
            status=status,
            error=error,
        )
    )


def submit_result(
    result_key: str,
    report: str,
    summary: str,
    produces: dict[str, Any] | None = None,
    status: str = "succeeded",
    error: str | None = None,
) -> str:
    """Alias for submit_worker_result using the more generic result_key name."""
    return _run_json(
        lambda: _submit_worker_result(
            result_token=result_key,
            report=report,
            summary=summary,
            produces=produces,
            status=status,
            error=error,
        )
    )


def _memory_search(
    *,
    query: str | None,
    namespace: str | None,
    tags: list[str] | None,
    limit: int,
) -> dict[str, Any]:
    with session_scope() as session:
        service = MemoryService(session)
        text_query = _optional_string(query)
        if text_query:
            scored = service.search_hybrid(
                query=text_query,
                namespace=_optional_string(namespace),
                tags=_string_list(tags),
                limit=_limit(limit),
            )
            memories = [entry.memory for entry in scored]
        else:
            memories = list(
                service.search(
                    query=None,
                    namespace=_optional_string(namespace),
                    tags=_string_list(tags),
                    limit=_limit(limit),
                )
            )
        return {"ok": True, "items": [_memory_data(memory) for memory in memories]}


def _memory_search_any(
    *,
    keywords: list[str],
    namespace: str | None,
    tags: list[str] | None,
    limit: int,
) -> dict[str, Any]:
    seen: set[str] = set()
    items: list[dict[str, Any]] = []
    per_query_limit = max(_limit(limit), 1)
    for keyword in _string_list(keywords):
        result = _memory_search(
            query=keyword,
            namespace=namespace,
            tags=tags,
            limit=per_query_limit,
        )
        for item in result["items"]:
            if item["id"] in seen:
                continue
            seen.add(item["id"])
            items.append(item)
            if len(items) >= _limit(limit):
                return {"ok": True, "items": items}
    return {"ok": True, "items": items}


def _memory_list(
    *,
    namespace: str | None,
    kind: str | None,
    tags: list[str] | None,
    include_archived: bool,
    limit: int,
) -> dict[str, Any]:
    with session_scope() as session:
        statement = select(Memory).order_by(Memory.pinned.desc(), Memory.created_at.desc()).limit(
            _limit(limit) * 4
        )
        if not include_archived:
            statement = statement.where(Memory.archived_at.is_(None))
        if namespace:
            statement = statement.where(Memory.namespace == namespace)
        if kind:
            statement = statement.where(Memory.kind == kind)
        memories = list(session.scalars(statement).all())
        wanted_tags = set(_string_list(tags))
        if wanted_tags:
            memories = [memory for memory in memories if wanted_tags.issubset(set(memory.tags or []))]
        return {"ok": True, "items": [_memory_data(memory) for memory in memories[: _limit(limit)]]}


def _memory_get(memory_id: str) -> dict[str, Any]:
    with session_scope() as session:
        memory = session.get(Memory, memory_id)
        if memory is None:
            raise KeyError(f"Unknown memory: {memory_id}")
        return {"ok": True, "memory": _memory_data(memory, full=True)}


def _memory_get_canonical(*, namespace: str, canonical_key: str) -> dict[str, Any]:
    with session_scope() as session:
        memory = MemoryService(session).get_canonical(namespace=namespace, canonical_key=canonical_key)
        return {"ok": True, "memory": _memory_data(memory, full=True) if memory is not None else None}


def _memory_create(
    *,
    namespace: str,
    kind: str,
    content: str,
    tags: list[str] | None,
    canonical_key: str | None,
    pinned: bool,
    ttl_days: int | None,
    source: str,
    work_item_id: str | None,
) -> dict[str, Any]:
    with session_scope() as session:
        memory = MemoryService(session).create_memory(
            namespace=_required(namespace, "namespace"),
            kind=_required(kind, "kind"),
            content=_required(content, "content"),
            tags=_string_list(tags),
            source_kind=source,
            work_item_id=_optional_string(work_item_id),
            canonical_key=_optional_string(canonical_key),
            pinned=bool(pinned),
            ttl_days=_optional_int(ttl_days),
        )
        return {"ok": True, "memory": _memory_data(memory, full=True)}


def _memory_upsert_canonical(
    *,
    namespace: str,
    canonical_key: str,
    kind: str,
    content: str,
    tags: list[str] | None,
    pinned: bool,
    ttl_days: int | None,
    source: str,
    work_item_id: str | None,
) -> dict[str, Any]:
    with session_scope() as session:
        memory = MemoryService(session).upsert_canonical(
            namespace=_required(namespace, "namespace"),
            canonical_key=_required(canonical_key, "canonical_key"),
            kind=_required(kind, "kind"),
            content=_required(content, "content"),
            tags=_string_list(tags),
            source_kind=source,
            work_item_id=_optional_string(work_item_id),
            pinned=bool(pinned),
            ttl_days=_optional_int(ttl_days),
        )
        return {"ok": True, "memory": _memory_data(memory, full=True)}


def _memory_supersede(*, memory_id: str, content: str, tags: list[str] | None) -> dict[str, Any]:
    with session_scope() as session:
        memory = MemoryService(session).supersede_memory(
            _required(memory_id, "memory_id"),
            content=_required(content, "content"),
            tags=_string_list(tags) if tags is not None else None,
        )
        return {"ok": True, "memory": _memory_data(memory, full=True)}


def _memory_archive(*, memory_id: str) -> dict[str, Any]:
    with session_scope() as session:
        memory = MemoryService(session).archive_memory(_required(memory_id, "memory_id"))
        return {"ok": True, "memory": _memory_data(memory, full=True)}


def _memory_recall(
    *,
    query: str | None,
    namespace: str | None,
    tags: list[str] | None,
    limit: int,
) -> dict[str, Any]:
    with session_scope() as session:
        scored = MemoryService(session).search_hybrid(
            query=_optional_string(query),
            namespace=_optional_string(namespace),
            tags=_string_list(tags),
            limit=_limit(limit),
        )
        items: list[dict[str, Any]] = []
        for entry in scored:
            data = _memory_data(entry.memory)
            if data is not None:
                data["score"] = round(entry.score, 4)
                items.append(data)
        return {"ok": True, "items": items}


def _memory_update(
    *,
    memory_id: str,
    content: str | None,
    tags: list[str] | None,
    importance: int | None,
) -> dict[str, Any]:
    with session_scope() as session:
        memory = MemoryService(session).update_memory(
            _required(memory_id, "memory_id"),
            content=_optional_string(content),
            tags=_string_list(tags) if tags is not None else None,
            importance=_optional_int(importance),
        )
        return {"ok": True, "memory": _memory_data(memory, full=True)}


def _memory_delete(*, memory_id: str) -> dict[str, Any]:
    with session_scope() as session:
        MemoryService(session).delete_memory(_required(memory_id, "memory_id"))
        return {"ok": True, "deleted_memory_id": memory_id}


def _memory_ingest_text(
    *,
    namespace: str,
    title: str,
    content: str,
    source_kind: str,
    source_id: str | None,
    tags: list[str] | None,
    work_item_id: str | None,
    force: bool,
) -> dict[str, Any]:
    clean_source_id = _optional_string(source_id) or _content_source_id(content)
    with session_scope() as session:
        result = MemoryIngestService(session).ingest_text(
            namespace=_required(namespace, "namespace"),
            title=_required(title, "title"),
            content=_required(content, "content"),
            source_kind=_required(source_kind, "source_kind"),
            source_id=clean_source_id,
            tags=_string_list(tags),
            work_item_id=_optional_string(work_item_id),
            force=bool(force),
        )
        memories = [
            session.get(Memory, memory_id)
            for memory_id in result.memory_ids
        ]
        return {
            "ok": True,
            "source_kind": result.source_kind,
            "source_id": result.source_id,
            "skipped": result.skipped,
            "reason": result.reason,
            "memory_ids": result.memory_ids,
            "memories": [_memory_data(memory) for memory in memories if memory is not None],
        }


def _memory_ingest_artifact(
    *,
    artifact_id: str,
    namespace: str | None,
    tags: list[str] | None,
    max_bytes: int,
    force: bool,
) -> dict[str, Any]:
    with session_scope() as session:
        result = MemoryIngestService(session).ingest_artifact(
            _required(artifact_id, "artifact_id"),
            namespace=_optional_string(namespace),
            tags=_string_list(tags),
            max_bytes=_limit_bytes(max_bytes),
            force=bool(force),
        )
        if result is None:
            return {"ok": True, "ingested": False, "reason": "not_text_or_too_large"}
        memories = [
            session.get(Memory, memory_id)
            for memory_id in result.memory_ids
        ]
        return {
            "ok": True,
            "ingested": not result.skipped,
            "source_kind": result.source_kind,
            "source_id": result.source_id,
            "skipped": result.skipped,
            "reason": result.reason,
            "memory_ids": result.memory_ids,
            "memories": [_memory_data(memory) for memory in memories if memory is not None],
        }


def _memory_ingest_pending(*, limit: int) -> dict[str, Any]:
    with session_scope() as session:
        result = MemoryIngestService(session).auto_ingest_pending(limit=_limit(limit))
        return {
            "ok": True,
            "ingested_sources": result.ingested_sources,
            "skipped_sources": result.skipped_sources,
            "memory_ids": result.memory_ids,
        }


def _todo_write(
    *,
    scope: str,
    items: list[Any],
    namespace: str,
    work_item_id: str | None,
) -> dict[str, Any]:
    clean_scope = _required(scope, "scope")
    if not isinstance(items, list):
        raise ValueError("items must be a list.")
    lines = [f"# Todo: {clean_scope}", ""]
    for index, item in enumerate(items, start=1):
        if isinstance(item, dict):
            text = item.get("text") or item.get("task") or item.get("title") or json.dumps(item)
            status = item.get("status")
            prefix = f"{index}. [{status}] " if status else f"{index}. "
            lines.append(prefix + str(text).strip())
        else:
            lines.append(f"{index}. {str(item).strip()}")
    with session_scope() as session:
        memory = MemoryService(session).upsert_canonical(
            namespace=_required(namespace, "namespace"),
            canonical_key=f"todo:{clean_scope}",
            kind="todo",
            content="\n".join(lines).strip(),
            tags=["todo", "coordination"],
            source_kind="mcp_todo",
            work_item_id=_optional_string(work_item_id),
        )
        return {"ok": True, "memory": _memory_data(memory, full=True)}


def _ask_user(
    *,
    question: str,
    context: str | None,
    namespace: str,
    work_item_id: str | None,
) -> dict[str, Any]:
    body = _required(question, "question")
    if context and str(context).strip():
        body = f"{body}\n\nContext:\n{str(context).strip()}"
    with session_scope() as session:
        memory = MemoryService(session).create_memory(
            namespace=_required(namespace, "namespace"),
            kind="question",
            content=body,
            tags=["question", "needs_user"],
            source_kind="mcp_question",
            work_item_id=_optional_string(work_item_id),
        )
        return {"ok": True, "memory": _memory_data(memory, full=True)}


def _artifact_list(
    *,
    query: str | None,
    kind: str | None,
    tags: list[str] | None,
    work_item_id: str | None,
    source_kind: str | None,
    include_archived: bool,
    limit: int,
) -> dict[str, Any]:
    with session_scope() as session:
        artifacts = ArtifactService(session).list_artifacts(
            query=_optional_string(query),
            kind=_optional_string(kind),
            tag=_string_list(tags),
            work_item_id=_optional_string(work_item_id),
            source_kind=_optional_string(source_kind),
            include_archived=include_archived,
            limit=_limit(limit),
        )
        return {"ok": True, "items": [_artifact_data(artifact) for artifact in artifacts]}


def _artifact_get(*, artifact_id: str, include_text: bool, max_chars: int) -> dict[str, Any]:
    with session_scope() as session:
        artifact = ArtifactService(session).get_artifact(_required(artifact_id, "artifact_id"))
        data = _artifact_data(artifact, full=True)
    if include_text:
        data["text"] = _read_text_file(data["local_path"], max_chars=_limit_chars(max_chars))
    return {"ok": True, "artifact": data}


def _artifact_read_text(
    *,
    artifact_id: str | None,
    path: str | None,
    max_chars: int,
) -> dict[str, Any]:
    if artifact_id:
        with session_scope() as session:
            artifact = ArtifactService(session).get_artifact(artifact_id)
            path = artifact.local_path
    if not path:
        raise ValueError("artifact_id or path is required.")
    return {
        "ok": True,
        "path": str(Path(path).expanduser().resolve()),
        "text": _read_text_file(path, max_chars=_limit_chars(max_chars)),
    }


def _artifact_capture_file(
    *,
    path: str,
    kind: str,
    title: str | None,
    tags: list[str] | None,
    discord_upload: bool,
    work_item_id: str | None,
    workflow_run_id: str | None,
    source: str,
) -> dict[str, Any]:
    clean_tags = _string_list(tags)
    if discord_upload and "discord_upload" not in clean_tags:
        clean_tags.append("discord_upload")
    with session_scope() as session:
        artifact = ArtifactStore().capture_file(
            session,
            path=_required(path, "path"),
            kind=_required(kind, "kind"),
            title=_optional_string(title),
            tags=clean_tags,
            work_item_id=_optional_string(work_item_id),
            workflow_run_id=_optional_string(workflow_run_id),
            source_kind=source,
            source_id=str(Path(path).expanduser()),
        )
        return {"ok": True, "artifact": _artifact_data(artifact, full=True)}


def _artifact_archive(*, artifact_id: str) -> dict[str, Any]:
    with session_scope() as session:
        artifact = ArtifactService(session).archive_artifact(_required(artifact_id, "artifact_id"))
        return {"ok": True, "artifact": _artifact_data(artifact, full=True)}


_INHERITED_REPLY_FOLLOWUP_KEYS = ("reply_followup_work", "reply_processor")


def _calling_work_item(session: Any) -> WorkItem | None:
    """The work item whose provider run is invoking this MCP server, when known."""
    work_item_id = (os.environ.get("TASQUE2_WORK_ITEM_ID") or "").strip()
    if not work_item_id:
        return None
    return session.get(WorkItem, work_item_id)


def _context_with_inherited_reply_config(
    context: dict[str, Any],
    caller: WorkItem | None,
    *,
    inherit_parent_pointer: bool = False,
) -> dict[str, Any]:
    """Carry the caller's Discord reply handling onto work it spawns.

    Workers that enqueue child work rarely think to re-attach reply processing, which
    leaves the child's Discord thread deaf to user replies. Unless the child context
    already takes a position, inherit the caller's reply follow-up/memory config.
    """
    if caller is None:
        return context
    caller_context = caller.context or {}
    updated = dict(context)
    if not any(key in updated for key in (*_INHERITED_REPLY_FOLLOWUP_KEYS, "reply_followup_disabled")):
        for key in _INHERITED_REPLY_FOLLOWUP_KEYS:
            if isinstance(caller_context.get(key), dict):
                updated[key] = caller_context[key]
                break
    if "reply_memory" not in updated and isinstance(caller_context.get("reply_memory"), dict):
        updated["reply_memory"] = caller_context["reply_memory"]
    if inherit_parent_pointer:
        updated.setdefault("parent_work_item_id", caller.id)
    return updated


def _work_enqueue(
    *,
    title: str,
    task_instruction: str | None,
    worker_kind: str,
    runtime_contract: dict[str, Any] | None,
    context: dict[str, Any] | None,
    priority: int,
    max_attempts: int,
    idempotency_key: str | None,
    workflow_run_id: str | None,
    discord_thread_id: str | None,
    task_template_path: str | None,
    template_base_dir: str | None,
) -> dict[str, Any]:
    resolved_task_instruction = _resolve_task_instruction(
        task_instruction=task_instruction,
        task_template_path=task_template_path,
        template_base_dir=template_base_dir,
    )
    with session_scope() as session:
        caller = _calling_work_item(session)
        work = WorkRepository(session).create_work_item(
            title=_required(title, "title"),
            task_instruction=resolved_task_instruction,
            worker_kind=_required(worker_kind, "worker_kind"),
            runtime_contract=dict(runtime_contract or {}),
            context=_context_with_inherited_reply_config(
                dict(context or {}),
                caller,
                inherit_parent_pointer=True,
            ),
            priority=int(priority),
            max_attempts=max(1, int(max_attempts)),
            idempotency_key=_optional_string(idempotency_key),
            source_kind="mcp",
            source_id=caller.id if caller is not None else None,
            workflow_run_id=_optional_string(workflow_run_id),
            discord_thread_id=_optional_string(discord_thread_id),
        )
        return {"ok": True, "work_item": _work_data(work)}


def _work_list(
    *,
    status: str | list[str] | None,
    worker_kind: str | None,
    limit: int,
) -> dict[str, Any]:
    with session_scope() as session:
        statement = select(WorkItem).order_by(WorkItem.created_at.desc()).limit(_limit(limit) * 4)
        statuses = _string_list(status)
        if statuses:
            statement = statement.where(WorkItem.status.in_(statuses))
        if worker_kind:
            statement = statement.where(WorkItem.worker_kind == worker_kind)
        rows = list(session.scalars(statement).all())[: _limit(limit)]
        return {"ok": True, "items": [_work_data(work) for work in rows]}


def _work_get(*, work_item_id: str) -> dict[str, Any]:
    with session_scope() as session:
        work = session.get(WorkItem, _required(work_item_id, "work_item_id"))
        if work is None:
            raise KeyError(f"Unknown work item: {work_item_id}")
        return {"ok": True, "work_item": _work_data(work, full=True)}


def _work_events(*, work_item_id: str, limit: int) -> dict[str, Any]:
    with session_scope() as session:
        rows = session.scalars(
            select(WorkEvent)
            .where(WorkEvent.work_item_id == _required(work_item_id, "work_item_id"))
            .order_by(WorkEvent.created_at.desc(), WorkEvent.id.desc())
            .limit(_limit(limit))
        ).all()
        return {"ok": True, "items": [_event_data(event) for event in rows]}


def _work_transition(*, work_item_id: str, action: str) -> dict[str, Any]:
    with session_scope() as session:
        queue = WorkQueue(session)
        if action == "pause":
            work = queue.pause_work(work_item_id)
        elif action == "resume":
            work = queue.resume_work(work_item_id)
        elif action == "cancel":
            work = queue.request_cancel(work_item_id)
        elif action == "retry":
            work = queue.retry_dead_letter(work_item_id)
        else:
            raise ValueError(f"Unknown work transition action: {action}")
        return {"ok": True, "work_item": _work_data(work)}


def _schedule_create_work(
    *,
    name: str,
    schedule_type: str,
    expression: str,
    task_instruction: str | None,
    worker_kind: str,
    timezone_name: str | None,
    runtime_contract: dict[str, Any] | None,
    context: dict[str, Any] | None,
    catchup_policy: str,
    max_backfill: int,
    enabled: bool,
    task_template_path: str | None,
    template_base_dir: str | None,
    discord_thread_id: str | None = None,
) -> dict[str, Any]:
    if runtime_contract is not None and not isinstance(runtime_contract, dict):
        raise ValueError("runtime_contract must be an object or omitted.")
    if context is not None and not isinstance(context, dict):
        raise ValueError("context must be an object or omitted.")
    payload = _schedule_work_payload(
        name=name,
        task_instruction=task_instruction,
        task_template_path=task_template_path,
        template_base_dir=template_base_dir,
        context=context,
        discord_thread_id=discord_thread_id,
    )
    with session_scope() as session:
        payload["context"] = _context_with_inherited_reply_config(
            payload.get("context") or {},
            _calling_work_item(session),
        )
        schedule = ScheduleService(session).create_schedule(
            name=_required(name, "name"),
            schedule_type=_required(schedule_type, "schedule_type"),
            expression=_required(expression, "expression"),
            worker_kind=_required(worker_kind, "worker_kind"),
            payload=payload,
            timezone_name=_optional_string(timezone_name),
            runtime_contract=dict(runtime_contract or {}),
            catchup_policy=_required(catchup_policy, "catchup_policy"),
            max_backfill=max(1, int(max_backfill)),
            enabled=bool(enabled),
        )
        return {"ok": True, "schedule": _schedule_data(schedule, full=True)}


def _schedule_get(*, schedule_id: str) -> dict[str, Any]:
    with session_scope() as session:
        schedule = session.get(Schedule, _required(schedule_id, "schedule_id"))
        if schedule is None:
            raise ValueError(f"Unknown schedule: {schedule_id}")
        return {"ok": True, "schedule": _schedule_data(schedule, full=True)}


def _schedule_update(
    *,
    schedule_id: str,
    name: str | None,
    schedule_type: str | None,
    expression: str | None,
    timezone_name: str | None,
    context: dict[str, Any] | None,
    catchup_policy: str | None,
    max_backfill: int | None,
    enabled: bool | None,
    discord_thread_id: str | None,
) -> dict[str, Any]:
    if context is not None and not isinstance(context, dict):
        raise ValueError("context must be an object or omitted.")
    schedule_id = _required(schedule_id, "schedule_id")
    with session_scope() as session:
        service = ScheduleService(session)
        new_payload: dict[str, Any] | None = None
        if context is not None or discord_thread_id is not None:
            existing = session.get(Schedule, schedule_id)
            if existing is None:
                raise ValueError(f"Unknown schedule: {schedule_id}")
            new_payload = dict(existing.payload or {})
            if context is not None:
                new_payload["context"] = dict(context)
            if discord_thread_id is not None:
                thread_id = _optional_string(discord_thread_id)
                if thread_id:
                    new_payload["discord_thread_id"] = thread_id
                else:
                    new_payload.pop("discord_thread_id", None)
        schedule = service.update_schedule(
            schedule_id,
            name=_optional_string(name),
            schedule_type=_optional_string(schedule_type),
            expression=_optional_string(expression),
            payload=new_payload,
            timezone_name=_optional_string(timezone_name),
            catchup_policy=_optional_string(catchup_policy),
            max_backfill=None if max_backfill is None else max(1, int(max_backfill)),
            enabled=enabled,
        )
        return {"ok": True, "schedule": _schedule_data(schedule, full=True)}


def _schedule_set_enabled(*, schedule_id: str, enabled: bool) -> dict[str, Any]:
    schedule_id = _required(schedule_id, "schedule_id")
    with session_scope() as session:
        service = ScheduleService(session)
        schedule = (
            service.enable_schedule(schedule_id)
            if enabled
            else service.disable_schedule(schedule_id)
        )
        return {"ok": True, "schedule": _schedule_data(schedule, full=True)}


def _schedule_delete(*, schedule_id: str) -> dict[str, Any]:
    schedule_id = _required(schedule_id, "schedule_id")
    with session_scope() as session:
        ScheduleService(session).delete_schedule(schedule_id)
        return {"ok": True, "deleted_schedule_id": schedule_id}


def _schedule_fire_now(*, schedule_id: str) -> dict[str, Any]:
    schedule_id = _required(schedule_id, "schedule_id")
    with session_scope() as session:
        occurrence = ScheduleService(session).fire_schedule_now(schedule_id)
        return {
            "ok": True,
            "schedule_id": schedule_id,
            "occurrence_id": occurrence.id,
            "work_item_id": occurrence.work_item_id,
            "workflow_run_id": occurrence.workflow_run_id,
        }


def _schedule_list(*, enabled: bool | None, limit: int) -> dict[str, Any]:
    with session_scope() as session:
        statement = select(Schedule).order_by(Schedule.created_at.desc()).limit(_limit(limit))
        if enabled is not None:
            statement = statement.where(Schedule.enabled.is_(bool(enabled)))
        schedules = session.scalars(statement).all()
        return {"ok": True, "items": [_schedule_data(schedule) for schedule in schedules]}


def _workflow_list(*, enabled: bool | None, limit: int) -> dict[str, Any]:
    with session_scope() as session:
        statement = select(WorkflowDefinition).order_by(WorkflowDefinition.created_at.desc()).limit(
            _limit(limit)
        )
        if enabled is not None:
            statement = statement.where(WorkflowDefinition.enabled.is_(bool(enabled)))
        definitions = session.scalars(statement).all()
        return {
            "ok": True,
            "items": [_workflow_definition_data(definition) for definition in definitions],
        }


def _workflow_start(
    *,
    workflow_name: str | None,
    workflow_definition_id: str | None,
    version: str,
    run_name: str | None,
    input: dict[str, Any] | None,
    discord_thread_id: str | None,
) -> dict[str, Any]:
    if input is not None and not isinstance(input, dict):
        raise ValueError("input must be an object or omitted.")
    with session_scope() as session:
        definition = _resolve_workflow_definition(
            session,
            workflow_name=workflow_name,
            workflow_definition_id=workflow_definition_id,
            version=version,
        )
        run = WorkflowService(session).start_run(
            workflow_definition_id=definition.id,
            name=_optional_string(run_name) or definition.name,
            input=dict(input or {}),
            discord_thread_id=_optional_string(discord_thread_id),
        )
        return {
            "ok": True,
            "workflow_definition": _workflow_definition_data(definition),
            "workflow_run": _workflow_run_data(run),
        }


def _resolve_workflow_definition(
    session,
    *,
    workflow_name: str | None,
    workflow_definition_id: str | None,
    version: str,
) -> WorkflowDefinition:
    if workflow_definition_id:
        definition = session.get(WorkflowDefinition, workflow_definition_id)
        if definition is None:
            raise KeyError(f"Unknown workflow definition: {workflow_definition_id}")
        return definition
    name = _required(workflow_name, "workflow_name")
    definition = session.scalar(
        select(WorkflowDefinition).where(
            WorkflowDefinition.name == name,
            WorkflowDefinition.version == str(version or "1"),
        )
    )
    if definition is None:
        raise KeyError(f"Unknown workflow definition: {name}@{version}")
    return definition


def _android_action(action: Callable[[], dict[str, Any] | None]) -> dict[str, Any]:
    """Run one device action under the single-device lease.

    The lease is keyed by the calling work item (or "adhoc" outside one) and
    hands over automatically when the holder finishes or goes stale, so a
    reply-processor can pick up the device right after a session run ends.
    """
    with session_scope() as session:
        caller = _calling_work_item(session)
        owner = caller.id if caller is not None else "adhoc"

        def _holder_running(holder: str) -> bool:
            if holder == "adhoc":
                return True
            item = session.get(WorkItem, holder)
            return bool(item is not None and item.status == "running")

        android_device.ensure_lease(owner, is_owner_active=_holder_running)
    result = action() or {}
    result.setdefault("ok", True)
    return result


def _system_status() -> dict[str, Any]:
    with session_scope() as session:
        status = get_system_status(session)
        return {
            "ok": True,
            "status": {
                "work_items": status.work_items,
                "work_attempts": status.work_attempts,
                "failed_work_unresolved": status.failed_work_unresolved,
                "schedules_enabled": status.schedules_enabled,
                "workflow_runs": status.workflow_runs,
                "ready_work": status.ready_work,
                "running_work": status.running_work,
            },
        }


def _submit_worker_result(
    *,
    result_token: str,
    report: str,
    summary: str,
    produces: dict[str, Any] | None,
    status: str,
    error: str | None,
) -> dict[str, Any]:
    token = _required(result_token, "result_token")
    if produces is not None and not isinstance(produces, dict):
        raise ValueError("produces must be an object or omitted.")
    clean_status = str(status or "succeeded").strip().lower()
    clean_error = str(error).strip() if error is not None else None
    if clean_error == "":
        clean_error = None
    result_inbox.deposit(
        result_token=token,
        agent_kind="worker",
        payload={
            "status": clean_status,
            "report": _string_field(report, "report"),
            "summary": _string_field(summary, "summary"),
            "produces": produces or {},
            "error": clean_error,
        },
    )
    return {"ok": True, "result_token": token}


def _memory_data(memory: Memory | None, *, full: bool = False) -> dict[str, Any] | None:
    if memory is None:
        return None
    content = memory.content if full else _truncate(memory.content, 1200)
    return {
        "id": memory.id,
        "namespace": memory.namespace,
        "kind": memory.kind,
        "content": content,
        "tags": memory.tags or [],
        "canonical_key": memory.canonical_key,
        "work_item_id": memory.work_item_id,
        "source_kind": memory.source_kind,
        "source_id": memory.source_id,
        "pinned": memory.pinned,
        "ttl_days": memory.ttl_days,
        "superseded_by": memory.superseded_by,
        "archived_at": _iso(memory.archived_at),
        "created_at": _iso(memory.created_at),
        "updated_at": _iso(memory.updated_at),
    }


def _artifact_data(artifact: Artifact, *, full: bool = False) -> dict[str, Any]:
    data = {
        "id": artifact.id,
        "kind": artifact.kind,
        "title": artifact.title,
        "local_path": artifact.local_path,
        "content_type": artifact.content_type,
        "size_bytes": artifact.size_bytes,
        "sha256": artifact.sha256,
        "summary": artifact.summary if full else _truncate(artifact.summary or "", 500),
        "tags": artifact.tags or [],
        "workflow_run_id": artifact.workflow_run_id,
        "work_item_id": artifact.work_item_id,
        "attempt_id": artifact.attempt_id,
        "source_kind": artifact.source_kind,
        "source_id": artifact.source_id,
        "archived_at": _iso(artifact.archived_at),
        "created_at": _iso(artifact.created_at),
        "updated_at": _iso(artifact.updated_at),
    }
    if not data["summary"]:
        data.pop("summary")
    return data


def _work_data(work: WorkItem, *, full: bool = False) -> dict[str, Any]:
    data = {
        "id": work.id,
        "title": work.title,
        "status": work.status,
        "worker_kind": work.worker_kind,
        "priority": work.priority,
        "attempt_count": work.attempt_count,
        "max_attempts": work.max_attempts,
        "source_kind": work.source_kind,
        "source_id": work.source_id,
        "workflow_run_id": work.workflow_run_id,
        "workflow_node_id": work.workflow_node_id,
        "schedule_id": work.schedule_id,
        "discord_thread_id": work.discord_thread_id,
        "not_before": _iso(work.not_before),
        "deadline_at": _iso(work.deadline_at),
        "created_at": _iso(work.created_at),
        "updated_at": _iso(work.updated_at),
    }
    if full:
        data.update(
            {
                "task_instruction": work.task_instruction,
                "runtime_contract": work.runtime_contract or {},
                "context": work.context or {},
                "retry_policy": work.retry_policy or {},
            }
        )
    return data


def _event_data(event: WorkEvent) -> dict[str, Any]:
    return {
        "id": event.id,
        "event_type": event.event_type,
        "entity_kind": event.entity_kind,
        "entity_id": event.entity_id,
        "work_item_id": event.work_item_id,
        "attempt_id": event.attempt_id,
        "workflow_run_id": event.workflow_run_id,
        "schedule_id": event.schedule_id,
        "source": event.source,
        "summary": event.summary,
        "payload": event.payload or {},
        "created_at": _iso(event.created_at),
    }


def _schedule_data(schedule: Schedule, *, full: bool = False) -> dict[str, Any]:
    data = {
        "id": schedule.id,
        "name": schedule.name,
        "enabled": schedule.enabled,
        "schedule_type": schedule.schedule_type,
        "expression": schedule.expression,
        "timezone": schedule.timezone,
        "worker_kind": schedule.worker_kind,
        "catchup_policy": schedule.catchup_policy,
        "max_backfill": schedule.max_backfill,
        "max_active_runs": schedule.max_active_runs,
        "last_evaluated_at": _iso(schedule.last_evaluated_at),
        "created_at": _iso(schedule.created_at),
        "updated_at": _iso(schedule.updated_at),
    }
    if full:
        data.update(
            {
                "payload": schedule.payload or {},
                "runtime_contract": schedule.runtime_contract or {},
                "misfire_grace_seconds": schedule.misfire_grace_seconds,
            }
        )
    return data


def _workflow_definition_data(definition: WorkflowDefinition) -> dict[str, Any]:
    nodes = (definition.definition or {}).get("nodes") or []
    return {
        "id": definition.id,
        "name": definition.name,
        "version": definition.version,
        "enabled": definition.enabled,
        "node_count": len(nodes) if isinstance(nodes, list) else 0,
        "created_at": _iso(definition.created_at),
        "updated_at": _iso(definition.updated_at),
    }


def _workflow_run_data(run: WorkflowRun) -> dict[str, Any]:
    return {
        "id": run.id,
        "name": run.name,
        "status": run.status,
        "workflow_definition_id": run.workflow_definition_id,
        "discord_thread_id": run.discord_thread_id,
        "started_at": _iso(run.started_at),
        "ended_at": _iso(run.ended_at),
    }


def _read_text_file(path: str | Path, *, max_chars: int) -> str:
    resolved = Path(path).expanduser().resolve()
    if not resolved.is_file():
        raise FileNotFoundError(f"File does not exist: {resolved}")
    content = resolved.read_text(encoding="utf-8", errors="replace")
    return content[:max_chars]


def _run_json(callback: Callable[[], dict[str, Any]], *, intent: str | None = None) -> str:
    try:
        result = callback()
        if intent:
            result.setdefault("_intent", intent)
        return _json(result)
    except Exception as exc:
        return _json({"ok": False, "error": str(exc), "error_type": type(exc).__name__})


def _json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, default=_json_default)


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _fts_query(value: str | None) -> str | None:
    if not value:
        return None
    tokens = []
    for raw in str(value).split():
        token = "".join(character for character in raw.lower() if character.isalnum() or character == "_")
        if len(token) >= 2 and token not in tokens:
            tokens.append(token)
    return " OR ".join(tokens[:12]) or None


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


def _resolve_task_instruction(
    *,
    task_instruction: str | None,
    task_template_path: str | None,
    template_base_dir: str | None,
) -> str:
    instruction = _optional_string(task_instruction)
    template_path = _optional_string(task_template_path)
    if bool(instruction) == bool(template_path):
        raise ValueError("Provide exactly one of task_instruction or task_template_path.")
    if template_path:
        base_dir = _optional_string(template_base_dir)
        return read_template_file(
            template_path,
            base_dir=Path(base_dir) if base_dir else None,
        )
    return _required(instruction, "task_instruction")


def _schedule_work_payload(
    *,
    name: str,
    task_instruction: str | None,
    task_template_path: str | None,
    template_base_dir: str | None,
    context: dict[str, Any] | None,
    discord_thread_id: str | None = None,
) -> dict[str, Any]:
    instruction = _optional_string(task_instruction)
    template_path = _optional_string(task_template_path)
    if bool(instruction) == bool(template_path):
        raise ValueError("Provide exactly one of task_instruction or task_template_path.")
    payload: dict[str, Any] = {
        "title": _required(name, "name"),
        "context": dict(context or {}),
    }
    thread_id = _optional_string(discord_thread_id)
    if thread_id:
        payload["discord_thread_id"] = thread_id
    if template_path:
        payload["task_template_path"] = template_path
        base_dir = _optional_string(template_base_dir)
        if base_dir:
            payload["template_base_dir"] = base_dir
    else:
        payload["task_instruction"] = _required(instruction, "task_instruction")
    return payload


def _required(value: Any, field_name: str) -> str:
    text = str(value).strip() if value is not None else ""
    if not text:
        raise ValueError(f"{field_name} is required.")
    return text


def _string_field(value: Any, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string.")
    return value


def _optional_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _optional_int(value: Any) -> int | None:
    if value is None or str(value).strip() == "":
        return None
    return int(value)


def _limit(value: int | None, *, default: int = 20, max_value: int = 100) -> int:
    if value is None:
        return default
    return max(1, min(int(value), max_value))


def _limit_chars(value: int | None) -> int:
    return _limit(value, default=20000, max_value=200000)


def _limit_bytes(value: int | None) -> int:
    return _limit(value, default=2_000_000, max_value=20_000_000)


# --- Image tools ------------------------------------------------------------------------------
# Vision itself is handled by the worker's built-in Read tool: it can open an image artifact's
# local_path and actually see it. These tools cover what Read cannot -- cropping a region out of an
# image, pulling a web image in to look at, and storing / finding / sending images as tagged
# artifacts. They return artifact ids + local paths (JSON), which the worker then Reads (to view) or
# adds to produces.discord_upload_artifact_ids (to send). Grouping (e.g. a wardrobe) is done purely
# with tags the caller chooses -- nothing here is domain-specific.


def _slug(value: str) -> str:
    cleaned = "".join(ch.lower() if ch.isalnum() else "-" for ch in str(value).strip())
    while "--" in cleaned:
        cleaned = cleaned.replace("--", "-")
    return cleaned.strip("-")[:60] or "item"


def image_edit(
    source: str,
    label: str | None = None,
    exposure: float = 0.0,
    contrast: float = 0.0,
    highlights: float = 0.0,
    shadows: float = 0.0,
    whites: float = 0.0,
    blacks: float = 0.0,
    temperature: float = 0.0,
    tint: float = 0.0,
    vibrance: float = 0.0,
    saturation: float = 0.0,
    clarity: float = 0.0,
    sharpen: float = 0.0,
    denoise: float = 0.0,
    dehaze: float = 0.0,
    grain: float = 0.0,
    vignette: float = 0.0,
    straighten: float = 0.0,
    depth_of_field: dict[str, Any] | None = None,
    auto: bool = False,
    tags: list[str] | None = None,
    work_item_id: str | None = None,
    send: bool = False,
) -> str:
    """Edit a real photo in the darkroom -- non-generative, Lightroom/Camera-Raw style.

    NOT AI generation: this only adjusts the pixels of an existing photo.
    ``source`` is a local path or an artifact id. Every slider defaults to 0 (no
    change); most take roughly -100..100, ``exposure`` is in stops (EV, ~-5..5),
    ``straighten`` is degrees. Edits apply in photographic order regardless of
    argument order, so you can pass several at once.

    Tone: exposure, contrast, highlights, shadows, whites, blacks, dehaze.
    Color: temperature (+warm / -cool), tint (+green / -magenta), vibrance
    (boosts muted tones first), saturation. Detail: clarity (midtone local
    contrast / "pop"), sharpen, denoise. Finish: grain, vignette (+darken /
    -lighten edges), straighten. ``auto`` = auto-contrast + grey-world white
    balance as a one-shot baseline. ``depth_of_field`` = {"focus": one of
    center|top|bottom|left|right, "strength": 0-100} keeps the focus region
    sharp and progressively blurs the rest (synthetic DoF / portrait background).

    Saves the result as a NEW artifact (the original is never touched) and
    returns its ``artifact_id`` + ``local_path`` + the list of ops that ran --
    Read the local_path to see it, and iterate. Set ``send`` true to also queue
    it to the Discord thread.
    """
    ops: dict[str, Any] = {
        "exposure": exposure,
        "contrast": contrast,
        "highlights": highlights,
        "shadows": shadows,
        "whites": whites,
        "blacks": blacks,
        "temperature": temperature,
        "tint": tint,
        "vibrance": vibrance,
        "saturation": saturation,
        "clarity": clarity,
        "sharpen": sharpen,
        "denoise": denoise,
        "dehaze": dehaze,
        "grain": grain,
        "vignette": vignette,
        "straighten": straighten,
        "depth_of_field": depth_of_field,
        "auto": auto,
    }
    return _run_json(
        lambda: _image_edit(
            source=source,
            label=label,
            ops=ops,
            tags=tags,
            work_item_id=work_item_id,
            send=send,
        )
    )


def _image_edit(
    *,
    source: str,
    label: str | None,
    ops: dict[str, Any],
    tags: list[str] | None,
    work_item_id: str | None,
    send: bool,
) -> dict[str, Any]:
    from io import BytesIO

    from PIL import Image

    from tasque2.imaging import edit_image

    clean_tags = _string_list(tags)
    if send and "discord_upload" not in clean_tags:
        clean_tags.append("discord_upload")
    with session_scope() as session:
        src_path, src_id = _resolve_image_source(session, source)
        with Image.open(src_path) as image:
            edited, applied = edit_image(image, ops)
        buffer = BytesIO()
        edited.save(buffer, format="JPEG", quality=92)
        data = buffer.getvalue()
        title_stem = _slug(label) if label else src_path.stem
        artifact = ArtifactStore().write_bytes(
            session,
            kind="image",
            title=f"{title_stem}-edited.jpg",
            content=data,
            suffix=".jpg",
            content_type="image/jpeg",
            tags=clean_tags,
            work_item_id=_optional_string(work_item_id),
            source_kind="image_edit",
            source_id=src_id,
        )
        return {
            "ok": True,
            "artifact_id": artifact.id,
            "local_path": artifact.local_path,
            "applied": applied,
            "dimensions": list(edited.size),
            "queued_for_discord": bool(send),
            "hint": "Read local_path to view it; add artifact_id to produces.discord_upload_artifact_ids to send.",
        }


def photoshop_status(intent: str) -> str:
    """Check whether Adobe Photoshop is reachable for a heavy edit (photoshop_edit).

    Windows only, and Photoshop must already be OPEN. Returns availability plus
    version and open-document count. If it's not available, use image_edit
    instead or ask him to open Photoshop — never assume it's there.
    """
    from tasque2 import photoshop as _ps

    return _run_json(lambda: {"ok": True, "photoshop": _ps.status()}, intent=intent)


def photoshop_edit(
    source: str,
    script: str,
    label: str | None = None,
    quality: int = 11,
    tags: list[str] | None = None,
    work_item_id: str | None = None,
    send: bool = False,
) -> str:
    """Run a real Photoshop (ExtendScript/JSX) edit on a photo — the heavy escalation above image_edit.

    Use this only for edits image_edit's darkroom can't do: true lens blur /
    background separation, Camera Raw grade, dodge/burn or frequency-separation
    retouch, compositing, recorded actions. For routine exposure/contrast/white-
    balance/crop, image_edit is faster and needs nothing running.

    REQUIRES Photoshop to be open (check `photoshop_status` first; fall back to
    image_edit if it's closed). ``script`` is an ExtendScript BODY with the
    opened document bound to ``doc`` — e.g. ``doc.activeLayer.applyLensBlur(...)``
    or Action-Manager ``executeAction(...)`` calls; do not open/save/close, the
    wrapper does that and never overwrites the original. ``quality`` is the JPEG
    export quality (0-12). Saves the result as a NEW artifact and returns its
    ``artifact_id`` + ``local_path``; Read it to check the result. Keep edits
    believable — the playbook still bans over-retouched / fake-looking photos.
    """
    return _run_json(
        lambda: _photoshop_edit(
            source=source,
            script=script,
            label=label,
            quality=quality,
            tags=tags,
            work_item_id=work_item_id,
            send=send,
        )
    )


def _photoshop_edit(
    *,
    source: str,
    script: str,
    label: str | None,
    quality: int,
    tags: list[str] | None,
    work_item_id: str | None,
    send: bool,
) -> dict[str, Any]:
    import tempfile

    from tasque2 import photoshop as ps

    clean_tags = _string_list(tags)
    if send and "discord_upload" not in clean_tags:
        clean_tags.append("discord_upload")
    with session_scope() as session:
        src_path, src_id = _resolve_image_source(session, source)
        handle = tempfile.NamedTemporaryFile(suffix=".jpg", delete=False)
        handle.close()
        tmp = Path(handle.name)
        try:
            ps.edit_file(src_path, tmp, script, quality=quality)
            title_stem = _slug(label) if label else src_path.stem
            artifact = ArtifactStore().capture_file(
                session,
                path=tmp,
                kind="image",
                title=f"{title_stem}-ps.jpg",
                tags=clean_tags,
                work_item_id=_optional_string(work_item_id),
                source_kind="photoshop_edit",
                source_id=src_id,
            )
        finally:
            try:
                tmp.unlink()
            except OSError:
                pass
        return {
            "ok": True,
            "artifact_id": artifact.id,
            "local_path": artifact.local_path,
            "queued_for_discord": bool(send),
            "hint": "Read local_path to view it; add artifact_id to produces.discord_upload_artifact_ids to send.",
        }


def _resolve_image_source(session: Any, source: str) -> tuple[Path, str | None]:
    """Resolve an image source given as either an artifact id or a local file path."""
    text = _required(source, "source")
    artifact = session.get(Artifact, text)
    if artifact is not None:
        path = Path(artifact.local_path)
        if not path.is_file():
            raise FileNotFoundError(f"Artifact {text} has no file on disk: {path}")
        return path, artifact.id
    path = Path(text).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(f"source is neither a known artifact id nor an existing file: {source}")
    return path, None


def _crop_to_png_bytes(path: Path, box: Any, *, normalized: bool) -> tuple[bytes, tuple[int, int]]:
    from io import BytesIO

    from PIL import Image

    if not isinstance(box, (list, tuple)) or len(box) != 4:
        raise ValueError("box must be [left, top, right, bottom].")
    with Image.open(path) as image:
        if image.mode not in ("RGB", "RGBA"):
            image = image.convert("RGB")
        width, height = image.size
        left, top, right, bottom = (float(value) for value in box)
        if normalized:
            left, right = left * width, right * width
            top, bottom = top * height, bottom * height
        crop = (
            max(0, int(round(left))),
            max(0, int(round(top))),
            min(width, int(round(right))),
            min(height, int(round(bottom))),
        )
        if crop[2] <= crop[0] or crop[3] <= crop[1]:
            raise ValueError(f"empty crop box {crop} for a {width}x{height} image.")
        cropped = image.crop(crop)
        buffer = BytesIO()
        cropped.save(buffer, format="PNG")
        return buffer.getvalue(), cropped.size


def image_crop(
    source: str,
    box: list[float],
    normalized: bool = False,
    label: str | None = None,
    tags: list[str] | None = None,
    work_item_id: str | None = None,
    discord_upload: bool = False,
) -> str:
    """Crop a rectangle out of an image and store the crop as a new artifact.

    ``source`` is a local file path or an existing artifact id (e.g. a closet photo the user sent).
    ``box`` is ``[left, top, right, bottom]`` in pixels, or 0-1 fractions of width/height when
    ``normalized`` is true. Returns the new artifact id + local_path; Read that path to view the
    crop. Set ``discord_upload`` true to also queue it for sending to the user.
    """
    return _run_json(
        lambda: _image_crop(
            source=source,
            box=box,
            normalized=normalized,
            label=label,
            tags=tags,
            work_item_id=work_item_id,
            discord_upload=discord_upload,
        )
    )


def _image_crop(
    *,
    source: str,
    box: Any,
    normalized: bool,
    label: str | None,
    tags: list[str] | None,
    work_item_id: str | None,
    discord_upload: bool,
) -> dict[str, Any]:
    clean_tags = _string_list(tags)
    if discord_upload and "discord_upload" not in clean_tags:
        clean_tags.append("discord_upload")
    with session_scope() as session:
        src_path, src_id = _resolve_image_source(session, source)
        data, size = _crop_to_png_bytes(src_path, box, normalized=normalized)
        title = f"{_slug(label or src_path.stem)}.png"
        artifact = ArtifactStore().write_bytes(
            session,
            kind="image_crop",
            title=title,
            content=data,
            suffix=".png",
            content_type="image/png",
            tags=clean_tags,
            work_item_id=_optional_string(work_item_id),
            source_kind="image_crop",
            source_id=src_id,
        )
        return {
            "ok": True,
            "artifact_id": artifact.id,
            "local_path": artifact.local_path,
            "width": size[0],
            "height": size[1],
            "label": label,
            "source_artifact_id": src_id,
        }


def image_fetch(
    url: str,
    label: str | None = None,
    tags: list[str] | None = None,
    work_item_id: str | None = None,
) -> str:
    """Download an image from a URL and store it as an artifact so you can SEE it.

    Use this when researching what to buy: after WebSearch/WebFetch finds a product, fetch its
    image here and Read the returned local_path to see what the item actually looks like before
    recommending it. Returns the artifact id + local_path.
    """
    return _run_json(
        lambda: _image_fetch(url=url, label=label, tags=tags, work_item_id=work_item_id)
    )


def _image_fetch(
    *,
    url: str,
    label: str | None,
    tags: list[str] | None,
    work_item_id: str | None,
) -> dict[str, Any]:
    import mimetypes

    import httpx

    target = _required(url, "url")
    with httpx.Client(follow_redirects=True, timeout=30.0) as client:
        response = client.get(target, headers={"User-Agent": "Mozilla/5.0 (tasque2-stylist)"})
        response.raise_for_status()
        content_type = (response.headers.get("content-type") or "").split(";")[0].strip().lower()
        data = response.content
    if not content_type.startswith("image/"):
        raise ValueError(f"URL did not return an image (content-type={content_type or 'unknown'}).")
    if len(data) > 25 * 1024 * 1024:
        raise ValueError("image is larger than 25MB.")
    extension = mimetypes.guess_extension(content_type) or ".img"
    with session_scope() as session:
        artifact = ArtifactStore().write_bytes(
            session,
            kind="web_image",
            title=f"{_slug(label or 'web-image')}{extension}",
            content=data,
            suffix=extension,
            content_type=content_type,
            tags=_string_list(tags),
            work_item_id=_optional_string(work_item_id),
            source_kind="image_fetch",
            source_id=target[:240],
        )
        return {
            "ok": True,
            "artifact_id": artifact.id,
            "local_path": artifact.local_path,
            "content_type": content_type,
            "size_bytes": len(data),
            "url": target,
        }


def image_save(
    source: str,
    label: str | None = None,
    tags: list[str] | None = None,
    box: list[float] | None = None,
    normalized: bool = False,
    kind: str = "image",
    notes: str | None = None,
    work_item_id: str | None = None,
    send: bool = False,
) -> str:
    """Store an image as a durable artifact you can find and send later.

    ``source`` is a local file path or an existing artifact id (e.g. a photo the user sent, or a
    fetched web image). Optionally crop to a region with ``box`` = ``[left, top, right, bottom]``
    (``normalized`` true for 0-1 fractions of width/height). ``label`` sets a human title; ``tags``
    make it findable later with ``image_find`` -- use any scheme you like to group related images.
    Returns ``artifact_id`` + ``local_path`` + ``tags``. Set ``send`` true to also queue it to the
    user's Discord thread.
    """
    return _run_json(
        lambda: _image_save(
            source=source,
            label=label,
            tags=tags,
            box=box,
            normalized=normalized,
            kind=kind,
            notes=notes,
            work_item_id=work_item_id,
            send=send,
        )
    )


def _image_save(
    *,
    source: str,
    label: str | None,
    tags: list[str] | None,
    box: Any,
    normalized: bool,
    kind: str,
    notes: str | None,
    work_item_id: str | None,
    send: bool,
) -> dict[str, Any]:
    clean_tags = _string_list(tags)
    if send and "discord_upload" not in clean_tags:
        clean_tags.append("discord_upload")
    resolved_kind = _optional_string(kind) or "image"
    with session_scope() as session:
        src_path, src_id = _resolve_image_source(session, source)
        title_stem = _slug(label) if label else src_path.stem
        if box is not None:
            data, size = _crop_to_png_bytes(src_path, box, normalized=normalized)
            artifact = ArtifactStore().write_bytes(
                session,
                kind=resolved_kind,
                title=f"{title_stem}.png",
                content=data,
                suffix=".png",
                content_type="image/png",
                tags=clean_tags,
                work_item_id=_optional_string(work_item_id),
                source_kind="image_save",
                source_id=src_id,
            )
            dimensions: list[int] | None = list(size)
        else:
            artifact = ArtifactStore().capture_file(
                session,
                path=src_path,
                kind=resolved_kind,
                title=f"{title_stem}{src_path.suffix}",
                tags=clean_tags,
                work_item_id=_optional_string(work_item_id),
                source_kind="image_save",
                source_id=src_id,
            )
            dimensions = None
        return {
            "ok": True,
            "artifact_id": artifact.id,
            "local_path": artifact.local_path,
            "label": label,
            "tags": clean_tags,
            "dimensions": dimensions,
            "notes": notes,
            "queued_for_discord": bool(send),
            "hint": (
                "Find this later with image_find; to send it add artifact_id to "
                "produces.discord_upload_artifact_ids."
            ),
        }


def image_find(
    query: str | None = None,
    tags: list[str] | None = None,
    kind: str | None = None,
    limit: int = 20,
) -> str:
    """Find images you stored earlier with ``image_save``.

    ``tags`` matches images carrying ALL of the given tags; ``query`` is a free-text substring over
    title / tags / path. Returns ``artifact_id``, ``label``, ``local_path``, ``tags`` and
    ``content_type`` for each match -- Read a ``local_path`` to view it, or add an ``artifact_id`` to
    ``produces.discord_upload_artifact_ids`` to send it.
    """
    return _run_json(lambda: _image_find(query=query, tags=tags, kind=kind, limit=limit))


def _image_find(
    *,
    query: str | None,
    tags: list[str] | None,
    kind: str | None,
    limit: int,
) -> dict[str, Any]:
    with session_scope() as session:
        rows = ArtifactService(session).list_artifacts(
            kind=_optional_string(kind),
            tag=_string_list(tags) or None,
            query=_optional_string(query),
            limit=_limit(limit),
        )
        items = [
            {
                "artifact_id": artifact.id,
                "local_path": artifact.local_path,
                "label": artifact.title,
                "content_type": artifact.content_type,
                "tags": artifact.tags or [],
            }
            for artifact in rows
        ]
        return {"ok": True, "count": len(items), "items": items}


def image_send(
    artifact_id: str | None = None,
    tags: list[str] | None = None,
    query: str | None = None,
    kind: str | None = None,
    work_item_id: str | None = None,
    limit: int = 10,
) -> str:
    """Queue stored image(s) to be sent to the user in the Discord thread.

    Identify by an explicit ``artifact_id``, or by ``tags`` / ``query`` (same matching as
    ``image_find``). Tags the artifact(s) for upload and returns their ids. IMPORTANT: also add the
    returned ids to ``produces.discord_upload_artifact_ids`` when you submit -- that is what actually
    delivers them.
    """
    return _run_json(
        lambda: _image_send(
            artifact_id=artifact_id,
            tags=tags,
            query=query,
            kind=kind,
            work_item_id=work_item_id,
            limit=limit,
        )
    )


def _image_send(
    *,
    artifact_id: str | None,
    tags: list[str] | None,
    query: str | None,
    kind: str | None,
    work_item_id: str | None,
    limit: int,
) -> dict[str, Any]:
    clean_tags = _string_list(tags)
    resolved_query = _optional_string(query)
    with session_scope() as session:
        service = ArtifactService(session)
        if artifact_id:
            targets = [service.get_artifact(_required(artifact_id, "artifact_id"))]
        elif clean_tags or resolved_query:
            targets = service.list_artifacts(
                kind=_optional_string(kind),
                tag=clean_tags or None,
                query=resolved_query,
                limit=_limit(limit),
            )
        else:
            raise ValueError("provide artifact_id, tags, or query.")
        if not targets:
            return {"ok": False, "error": "no matching images.", "artifact_ids": []}
        resolved_work_item = _optional_string(work_item_id)
        artifact_ids: list[str] = []
        for artifact in targets:
            current = list(artifact.tags or [])
            if "discord_upload" not in current:
                current.append("discord_upload")
                artifact.tags = current
            if resolved_work_item and artifact.work_item_id is None:
                artifact.work_item_id = resolved_work_item
            artifact_ids.append(artifact.id)
        session.flush()
        return {
            "ok": True,
            "artifact_ids": artifact_ids,
            "hint": "Add artifact_ids to produces.discord_upload_artifact_ids on submit to deliver them.",
        }


def image_compose(
    sources: list[str],
    labels: list[str] | None = None,
    columns: int | None = None,
    kind: str = "collage",
    label: str | None = None,
    tags: list[str] | None = None,
    work_item_id: str | None = None,
    send: bool = False,
) -> str:
    """Tile several images into one flat-lay collage (a grid on a white background) and store it.

    ``sources`` is a list of image paths or artifact ids (e.g. product photos from ``image_fetch`` or
    wardrobe shots); ``labels`` optionally captions each tile. Returns the new artifact id + local_path
    -- Read that path to actually see and judge the composed look. Set ``send`` true to also queue it to
    the user's Discord thread.
    """
    return _run_json(
        lambda: _image_compose(
            sources=sources,
            labels=labels,
            columns=columns,
            kind=kind,
            label=label,
            tags=tags,
            work_item_id=work_item_id,
            send=send,
        )
    )


def _image_compose(
    *,
    sources: Any,
    labels: Any,
    columns: Any,
    kind: str,
    label: str | None,
    tags: list[str] | None,
    work_item_id: str | None,
    send: bool,
) -> dict[str, Any]:
    from io import BytesIO
    from math import ceil, sqrt

    from PIL import Image, ImageDraw, ImageFont

    if not isinstance(sources, (list, tuple)) or not sources:
        raise ValueError("sources must be a non-empty list of image paths or artifact ids.")
    captions = [str(c) for c in labels] if isinstance(labels, (list, tuple)) else []
    clean_tags = _string_list(tags)
    if send and "discord_upload" not in clean_tags:
        clean_tags.append("discord_upload")
    cell, gap = 400, 20
    caption_h = 30 if captions else 0
    with session_scope() as session:
        tiles = []
        for source in sources:
            path, _ = _resolve_image_source(session, str(source))
            with Image.open(path) as image:
                tile = image.convert("RGB")
                tile.thumbnail((cell, cell))
                tiles.append(tile.copy())
        count = len(tiles)
        cols = max(1, int(columns)) if columns else max(1, ceil(sqrt(count)))
        rows = ceil(count / cols)
        cell_w, cell_h = cell + gap, cell + caption_h + gap
        canvas = Image.new("RGB", (gap + cols * cell_w, gap + rows * cell_h), (255, 255, 255))
        draw = ImageDraw.Draw(canvas)
        try:
            font = ImageFont.load_default()
        except Exception:
            font = None
        for index, tile in enumerate(tiles):
            row, col = divmod(index, cols)
            x = gap + col * cell_w + (cell - tile.width) // 2
            y = gap + row * cell_h + (cell - tile.height) // 2
            canvas.paste(tile, (x, y))
            if captions and index < len(captions) and font is not None:
                draw.text(
                    (gap + col * cell_w, gap + row * cell_h + cell + 6),
                    captions[index][:42],
                    fill=(60, 60, 60),
                    font=font,
                )
        buffer = BytesIO()
        canvas.save(buffer, format="PNG")
        artifact = ArtifactStore().write_bytes(
            session,
            kind=kind,
            title=f"{_slug(label or 'collage')}.png",
            content=buffer.getvalue(),
            suffix=".png",
            content_type="image/png",
            tags=clean_tags,
            work_item_id=_optional_string(work_item_id),
            source_kind="image_compose",
        )
        return {
            "ok": True,
            "artifact_id": artifact.id,
            "local_path": artifact.local_path,
            "tiles": count,
            "width": canvas.width,
            "height": canvas.height,
            "queued_for_discord": bool(send),
        }


def _content_source_id(content: str) -> str:
    digest = hashlib.sha256(str(content).encode("utf-8")).hexdigest()[:24]
    return f"text:{digest}"


def _truncate(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    return value[: max(0, limit - 15)] + "\n[truncated]"
