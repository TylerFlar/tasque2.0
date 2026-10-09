"""Workshop commands: the config repository, releases, and the Workshop's pause and undo."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import typer

from tasque2.cli._common import app, cli_session_scope, echo, fail


@app.command("data-repo-init")
def data_repo_init(
    dry_run: Annotated[bool, typer.Option("--dry-run", help="List what would be tracked.")] = False,
) -> None:
    """Make the data directory a private config repository (templates, workflows, lanes, doctrine)."""
    from tasque2.ops.datarepo import init

    outcome = init(dry_run=dry_run)
    if dry_run:
        for name in outcome.get("would_track") or []:
            echo(name)
        echo(f"[dry-run] {len(outcome.get('would_track') or [])} file(s) would be tracked")
        return
    state = "created" if outcome["created"] else "already a config repository"
    echo(f"{state}: {len(outcome['tracked'])} file(s) tracked")


@app.command("data-snapshot")
def data_snapshot(reason: Annotated[str, typer.Argument(help="Why, for the commit message.")] = "snapshot") -> None:
    """Commit what changed in the live config (and the doctrine export) since the last snapshot."""
    from tasque2.ops.datarepo import is_repo, snapshot

    if not is_repo():
        raise fail("The data directory is not a config repository yet: run data-repo-init.")
    head = snapshot(reason)
    echo(f"committed {head}" if head else "nothing changed")


@app.command("release-apply")
def release_apply(
    plan: Annotated[Path, typer.Option("--plan", help="A release plan written by the Workshop.")],
    undo: Annotated[bool, typer.Option("--undo", help="Reverse the plan instead.")] = False,
) -> None:
    """Land a release with this code: migrations, doctrine, changed lanes keys, workflows, its database
    script. The respawn runs it after switching the code; a rehearsal runs it on a database copy."""
    from tasque2.migrations import schema_status, upgrade_database
    from tasque2.ops.release import apply_config, load_plan

    if not plan.is_file():
        raise fail(f"No release plan at {plan}")
    loaded = load_plan(plan)
    if not undo:
        if loaded.migrations and not loaded.revision_before:
            # Kept for an undo, which downgrades to it with the code that has the migrations.
            loaded.revision_before = list(schema_status().current_revisions)
            data = json.loads(plan.read_text(encoding="utf-8"))
            data["revision_before"] = loaded.revision_before
            plan.write_text(json.dumps(data, indent=1), encoding="utf-8")
        upgrade_database()
    with cli_session_scope() as session:
        report = apply_config(session, loaded, undo=undo)
    conflicts = [entry for entry in report["doctrine"] if entry["status"] in ("conflict", "over_budget")]
    echo(json.dumps(report, default=str))
    if conflicts:
        raise fail(f"{len(conflicts)} doctrine document(s) could not land: {conflicts[0]['document']}")


@app.command("workshop-change")
def workshop_change(
    request: Annotated[str, typer.Argument(help="What to change, as you would say it in the Workshop thread.")],
    thread: Annotated[str | None, typer.Option("--thread", help="The thread its cards and report go to.")] = None,
) -> None:
    """Start a Workshop change: planned, built, checked and released like one asked for in Discord."""
    from tasque2.workshop.pipeline import start_change

    with cli_session_scope() as session:
        run = start_change(session, request=request, origin="user", thread_id=thread, author="cli")
        echo(f"change {run.input['change_id']} started (run {run.id})")


@app.command("workshop-status")
def workshop_status() -> None:
    """What the Workshop is doing: changes in progress, releases waiting and recent."""
    from tasque2.workshop.pipeline import status_text

    with cli_session_scope() as session:
        echo(status_text(session).replace("**", ""))


@app.command("workshop-pause")
def workshop_pause(reason: Annotated[str, typer.Argument(help="Why, shown in the status.")] = "paused by hand") -> None:
    """Stop the Workshop starting changes on its own and shipping without a tap."""
    from tasque2.workshop.pipeline import pause

    pause(reason)
    echo("paused")


@app.command("workshop-resume")
def workshop_resume() -> None:
    """Let the Workshop go on."""
    from tasque2.workshop.pipeline import resume

    echo("resumed" if resume() else "it was not paused")


@app.command("workshop-rollback")
def workshop_rollback(
    release: Annotated[str | None, typer.Argument(help="A release id; the newest live one when left out.")] = None,
) -> None:
    """Undo a Workshop release (the newest by default), like its Undo button."""
    from tasque2.workshop.pipeline import undo_by_id, undo_latest

    with cli_session_scope() as session:
        echo(undo_by_id(session, release) if release else undo_latest(session))


@app.command("release-check")
def release_check_command() -> None:
    """What new code must do before it counts as healthy: extensions load, the MCP server builds, the
    schema is current."""
    from tasque2.ops.release import release_check

    problems = release_check()
    for problem in problems:
        echo(problem)
    if problems:
        raise typer.Exit(1)
    echo("ok")
