from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from rich.markup import escape

from tasque2.artifacts import ArtifactService, ArtifactStore
from tasque2.cli._common import PlainTable, app, cli_session_scope, console, echo, existing_file, fail
from tasque2.memory import MemoryService
from tasque2.memory.ingest import MemoryIngestService
from tasque2.models import Memory
from tasque2.text import one_line


@app.command("memory-add")
def memory_add(
    content: Annotated[str, typer.Argument()],
    namespace: Annotated[str, typer.Option("--namespace", "-n")] = "global",
    kind: Annotated[str, typer.Option("--kind", "-k")] = "note",
    tag: Annotated[list[str] | None, typer.Option("--tag")] = None,
    importance: Annotated[int | None, typer.Option("--importance", min=1, max=5)] = None,
    ttl_days: Annotated[int | None, typer.Option("--ttl-days")] = None,
    pinned: Annotated[bool, typer.Option("--pinned")] = False,
) -> None:
    """Record one memory."""
    with cli_session_scope() as session:
        memory = MemoryService(session).create_memory(
            namespace=namespace,
            kind=kind,
            content=content,
            tags=tag or [],
            source_kind="cli",
            importance=importance,
            ttl_days=ttl_days,
            pinned=pinned,
        )
        console.print(memory.id)


@app.command("memory-search")
def memory_search(
    query: Annotated[str, typer.Argument()],
    namespace: Annotated[str | None, typer.Option("--namespace", "-n")] = None,
    tag: Annotated[list[str] | None, typer.Option("--tag")] = None,
    limit: Annotated[int, typer.Option("--limit")] = 10,
) -> None:
    """Recall the memories most relevant to a query."""
    with cli_session_scope() as session:
        table = PlainTable("Score", "Id", "Namespace", "Kind", "Content")
        for entry in MemoryService(session).recall(query=query, namespace=namespace, tags=tag or [], limit=limit):
            memory = entry.memory
            table.add_row(
                f"{entry.score:.3f}", memory.id, memory.namespace, memory.kind, one_line(memory.content, limit=160)
            )
        console.print(table)


@app.command("memory-show")
def memory_show(
    memory_id: Annotated[str | None, typer.Argument(help="Memory id.")] = None,
    namespace: Annotated[
        str | None, typer.Option("--namespace", "-n", help="With --key: a canonical document.")
    ] = None,
    key: Annotated[str | None, typer.Option("--key")] = None,
) -> None:
    """Print one memory, by id or by canonical namespace and key."""
    with cli_session_scope() as session:
        if memory_id:
            memory = session.get(Memory, memory_id)
        elif namespace and key:
            memory = MemoryService(session).get_canonical(namespace=namespace, canonical_key=key)
        else:
            raise typer.BadParameter("Give a memory id, or --namespace with --key.")
        if memory is None:
            raise fail("No such memory.")
        label = escape(f"{memory.namespace}/{memory.canonical_key or memory.kind}")
        console.print(f"[bold]{label}[/bold] {memory.id}", soft_wrap=True)
        echo(memory.content)


@app.command("memory-archive")
def memory_archive(memory_ids: Annotated[list[str], typer.Argument(help="One or more memory ids.")]) -> None:
    """Archive memories: they leave search and worker packets but stay on record."""
    with cli_session_scope() as session:
        service = MemoryService(session)
        for memory_id in memory_ids:
            service.archive_memory(memory_id)
        console.print(f"archived {len(memory_ids)} memor{'y' if len(memory_ids) == 1 else 'ies'}")


@app.command("memory-delete")
def memory_delete(
    memory_ids: Annotated[list[str], typer.Argument(help="One or more memory ids.")],
    yes: Annotated[bool, typer.Option("--yes", help="Confirm permanent deletion.")] = False,
) -> None:
    """Delete memories permanently."""
    if not yes:
        raise fail(f"This permanently deletes {len(memory_ids)} memories; pass --yes to confirm.")
    with cli_session_scope() as session:
        service = MemoryService(session)
        for memory_id in memory_ids:
            service.delete_memory(memory_id)
        console.print(f"deleted {len(memory_ids)}")


@app.command("memory-ingest-text")
def memory_ingest_text(
    path: Annotated[Path, typer.Argument(help="Text file.")],
    namespace: Annotated[str, typer.Option("--namespace", "-n")] = "global",
    title: Annotated[str | None, typer.Option("--title")] = None,
    tag: Annotated[list[str] | None, typer.Option("--tag")] = None,
) -> None:
    """Make a text file searchable as a summary memory plus chunks."""
    source = existing_file(path, option="path")
    with cli_session_scope() as session:
        result = MemoryIngestService(session).ingest_text(
            namespace=namespace,
            title=title or source.name,
            content=source.read_text(encoding="utf-8", errors="replace"),
            source_kind="cli_file",
            source_id=str(source),
            tags=tag or [],
        )
        console.print(f"ingested {len(result.memory_ids)} memories")


@app.command("memory-embed")
def memory_embed(
    namespace: Annotated[str | None, typer.Option("--namespace", "-n")] = None,
    limit: Annotated[int, typer.Option("--limit")] = 500,
) -> None:
    """Compute embeddings for memories that have none yet."""
    with cli_session_scope() as session:
        console.print(f"embedded {MemoryService(session).embed_missing(namespace=namespace, limit=limit)}")


@app.command("memory-prune")
def memory_prune(
    older_than_days: Annotated[int, typer.Option("--older-than-days", help="Archived longer ago than this.")] = 90,
    limit: Annotated[int, typer.Option("--limit")] = 500,
    yes: Annotated[bool, typer.Option("--yes", help="Confirm permanent deletion.")] = False,
) -> None:
    """Delete memories archived more than --older-than-days ago (archived rows are otherwise kept)."""
    if not yes:
        raise fail(
            f"This permanently deletes up to {limit} memories archived over {older_than_days} days ago; "
            "pass --yes to confirm."
        )
    with cli_session_scope() as session:
        pruned = MemoryService(session).prune_superseded(older_than_days=older_than_days, limit=limit)
        console.print(f"deleted {pruned}")


@app.command("artifact-list")
def artifact_list(
    query: Annotated[str | None, typer.Argument()] = None,
    kind: Annotated[str | None, typer.Option("--kind")] = None,
    tag: Annotated[list[str] | None, typer.Option("--tag")] = None,
    work_item_id: Annotated[str | None, typer.Option("--work-item-id")] = None,
    include_archived: Annotated[bool, typer.Option("--include-archived")] = False,
    limit: Annotated[int, typer.Option("--limit", "-n")] = 20,
) -> None:
    """List artifacts."""
    with cli_session_scope() as session:
        rows = ArtifactService(session).list_artifacts(
            query=query,
            kind=kind,
            tag=tag or [],
            work_item_id=work_item_id,
            include_archived=include_archived,
            limit=limit,
        )
        table = PlainTable("Id", "Kind", "Size", "Title", "Path")
        for artifact in rows:
            table.add_row(
                artifact.id, artifact.kind, str(artifact.size_bytes or ""), artifact.title, artifact.local_path
            )
        console.print(table)


@app.command("artifact-capture")
def artifact_capture(
    path: Annotated[Path, typer.Argument()],
    kind: Annotated[str, typer.Option("--kind")] = "file",
    title: Annotated[str | None, typer.Option("--title")] = None,
    tag: Annotated[list[str] | None, typer.Option("--tag")] = None,
    work_item_id: Annotated[str | None, typer.Option("--work-item-id")] = None,
) -> None:
    """Copy a local file into artifact storage."""
    with cli_session_scope() as session:
        artifact = ArtifactStore().capture_file(
            session,
            path=existing_file(path, option="path"),
            kind=kind,
            title=title,
            tags=tag or [],
            work_item_id=work_item_id,
            source_kind="cli",
            source_id=str(path),
        )
        console.print(artifact.id)


@app.command("artifact-archive")
def artifact_archive(artifact_id: Annotated[str, typer.Argument()]) -> None:
    """Archive an artifact's record; the file stays on disk."""
    with cli_session_scope() as session:
        console.print(f"{ArtifactService(session).archive_artifact(artifact_id).id}: archived")
