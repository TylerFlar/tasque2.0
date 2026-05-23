from __future__ import annotations

from tasque2.mcp import tools


def build_server():
    """Build the Tasque stdio MCP server.

    Imports FastMCP lazily so normal Tasque imports do not require MCP unless the
    server is actually used.
    """
    from mcp.server.fastmcp import FastMCP

    mcp = FastMCP(
        "tasque2",
        instructions=(
            "Local-first Tasque tools for workers. Use read tools with an intent string before "
            "acting; use memory/artifact/work tools for durable state instead of hand-building "
            "database JSON. Use normal work items and workflows for orchestration. "
            "End provider runs by calling "
            "submit_worker_result exactly once with the result_token from the prompt. "
            "Mutations return {ok:true,...} or "
            "{ok:false,error:...} JSON strings."
        ),
    )

    mcp.tool()(tools.memory_search)
    mcp.tool()(tools.memory_search_any)
    mcp.tool()(tools.memory_list)
    mcp.tool()(tools.memory_get)
    mcp.tool()(tools.memory_get_canonical)
    mcp.tool()(tools.memory_create)
    mcp.tool()(tools.memory_upsert_canonical)
    mcp.tool()(tools.memory_supersede)
    mcp.tool()(tools.memory_archive)
    mcp.tool()(tools.memory_ingest_text)
    mcp.tool()(tools.memory_ingest_artifact)
    mcp.tool()(tools.memory_ingest_pending)
    mcp.tool()(tools.todo_write)
    mcp.tool()(tools.ask_user)

    mcp.tool()(tools.artifact_list)
    mcp.tool()(tools.artifact_get)
    mcp.tool()(tools.artifact_read_text)
    mcp.tool()(tools.artifact_capture_file)
    mcp.tool()(tools.artifact_archive)

    mcp.tool()(tools.work_enqueue)
    mcp.tool()(tools.work_list)
    mcp.tool()(tools.work_get)
    mcp.tool()(tools.work_events)
    mcp.tool()(tools.work_pause)
    mcp.tool()(tools.work_resume)
    mcp.tool()(tools.work_cancel)
    mcp.tool()(tools.work_retry)

    mcp.tool()(tools.schedule_create_work)
    mcp.tool()(tools.schedule_list)

    mcp.tool()(tools.workflow_list)
    mcp.tool()(tools.workflow_start)

    mcp.tool()(tools.system_status)
    mcp.tool()(tools.submit_worker_result)
    mcp.tool()(tools.submit_result)
    return mcp


def run_stdio() -> None:
    """Run the Tasque MCP server over stdio."""
    from tasque2.migrations import upgrade_database

    upgrade_database()
    build_server().run("stdio")


__all__ = ["build_server", "run_stdio"]
