from __future__ import annotations

import time
from typing import Annotated

import typer

from tasque2.cli._common import PlainTable, app, cli_session_scope, cli_span, console, fail
from tasque2.daemon import control


@app.command("daemon")
def daemon(
    force: Annotated[bool, typer.Option("--force", help="Start even if another daemon looks alive.")] = False,
    discord: Annotated[bool, typer.Option("--discord/--no-discord", help="Run the Discord bot too.")] = True,
    max_claims: Annotated[int | None, typer.Option("--max-claims", help="Work items claimed per tick.")] = None,
) -> None:
    """Run the daemon: schedules, workflows, workers, retention, and Discord."""
    from tasque2.daemon import DaemonAlreadyRunning, serve

    try:
        serve(force=force, discord=discord, max_claims=max_claims)
    except DaemonAlreadyRunning as exc:
        raise fail(str(exc)) from None


@app.command("daemon-status")
def daemon_status() -> None:
    """Show whether a daemon is alive, when it last ticked, and what it is running."""
    state = control.read_state()
    with cli_session_scope() as session:
        last_schedule_tick = control.latest_schedule_tick(session)
    table = PlainTable("Field", "Value")
    if state is None:
        table.add_row("state file", "none")
    else:
        table.add_row("pid", str(state.pid))
        table.add_row("version", state.version or "")
        table.add_row("started", state.started_at.isoformat() if state.started_at else "")
        table.add_row("last tick", state.last_tick_at.isoformat() if state.last_tick_at else "")
        table.add_row("alive", str(state.is_fresh()))
        table.add_row("in flight", str(state.in_flight))
        table.add_row("draining", str(state.draining))
    table.add_row("schedules evaluated", last_schedule_tick.isoformat() if last_schedule_tick else "never")
    table.add_row("drain requested", str(control.drain_requested()))
    console.print(table)


@app.command("daemon-stop")
def daemon_stop(
    wait: Annotated[bool, typer.Option("--wait/--no-wait", help="Wait until the daemon has exited.")] = True,
    timeout: Annotated[int, typer.Option("--timeout", help="Seconds to wait.")] = 3600,
) -> None:
    """Ask the daemon to finish the work in flight, claim nothing new, and exit."""
    control.request_drain()
    console.print("Drain requested.")
    if not wait:
        return
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        state = control.read_state()
        if state is None or not state.is_fresh():
            console.print("Daemon stopped.")
            return
        console.print(f"Waiting: {state.in_flight} run(s) in flight", end="\r")
        time.sleep(5)
    raise fail("Timed out waiting for the daemon to stop; the drain request stays in place.")


@app.command("tick")
def tick(
    max_claims: Annotated[int | None, typer.Option("--max-claims")] = None,
    claim: Annotated[bool, typer.Option("--claim/--no-claim", help="Run ready work as part of the tick.")] = True,
    force: Annotated[bool, typer.Option("--force", help="Tick even while a daemon is alive.")] = False,
) -> None:
    """Run one tick in the foreground: recover, schedule, advance workflows, run ready work."""
    from tasque2.daemon import DaemonTick

    with cli_span("tick"), cli_session_scope() as session:
        reason = control.live_daemon_reason()
        if reason is not None and not force:
            raise fail(f"A daemon is alive ({reason}); it already ticks. Pass --force to tick anyway.")
        result = DaemonTick().run(session, max_claims=max_claims, claim=claim)
        console.print(result.describe() or "Nothing to do.")
