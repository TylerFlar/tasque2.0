"""The core MCP tools. Each public function becomes one tool: its name, signature and
docstring are the schema the model sees."""

from tasque2.mcp.tools.artifacts import artifact_capture_file, artifact_get, artifact_list, artifact_read_text
from tasque2.mcp.tools.discord import discord_history
from tasque2.mcp.tools.images import image_compose, image_crop, image_fetch, image_find, image_save, image_send
from tasque2.mcp.tools.memory import (
    memory_archive,
    memory_create,
    memory_delete,
    memory_get,
    memory_get_canonical,
    memory_ingest_artifact,
    memory_ingest_text,
    memory_list,
    memory_recall,
    memory_update,
    memory_upsert_canonical,
)
from tasque2.mcp.tools.schedules import (
    schedule_create_work,
    schedule_delete,
    schedule_fire_now,
    schedule_get,
    schedule_list,
    schedule_set_enabled,
    schedule_update,
)
from tasque2.mcp.tools.system import submit_worker_result, system_status, weather_now
from tasque2.mcp.tools.work import work_cancel, work_enqueue, work_events, work_get, work_list, work_retry
from tasque2.mcp.tools.workflows import workflow_list, workflow_start

CORE_TOOLS = (
    memory_recall,
    memory_list,
    memory_get,
    memory_get_canonical,
    memory_create,
    memory_upsert_canonical,
    memory_update,
    memory_archive,
    memory_delete,
    memory_ingest_text,
    memory_ingest_artifact,
    artifact_list,
    artifact_get,
    artifact_read_text,
    artifact_capture_file,
    image_fetch,
    image_crop,
    image_compose,
    image_save,
    image_find,
    image_send,
    work_enqueue,
    work_list,
    work_get,
    work_events,
    work_cancel,
    work_retry,
    schedule_create_work,
    schedule_list,
    schedule_get,
    schedule_update,
    schedule_set_enabled,
    schedule_delete,
    schedule_fire_now,
    workflow_list,
    workflow_start,
    discord_history,
    weather_now,
    system_status,
    submit_worker_result,
)

__all__ = ["CORE_TOOLS", *(tool.__name__ for tool in CORE_TOOLS)]
