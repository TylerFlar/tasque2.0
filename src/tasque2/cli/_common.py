from __future__ import annotations

import json
import os
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import typer
from rich.console import Console
from rich.markup import escape
from rich.table import Table
from rich.text import Text
from sqlalchemy.orm import Session

from tasque2.config import get_settings
from tasque2.db import session_scope

app = typer.Typer(no_args_is_help=True, help="Tasque: scheduled and on-demand model-backed work.")
console = Console(emoji=False)


class PlainTable(Table):
    """A table whose string cells print as written instead of being read as console markup."""

    def add_row(self, *renderables: Any, **kwargs: Any) -> None:
        super().add_row(*(Text(cell) if isinstance(cell, str) else cell for cell in renderables), **kwargs)


@contextmanager
def cli_session_scope() -> Iterator[Session]:
    """A committed session on an up-to-date schema.

    An unknown id or an invalid value raised inside the block rolls the session back and
    ends the command with its message instead of a traceback.
    """
    from tasque2.migrations import MigrationError, upgrade_database

    try:
        upgrade_database()
        with session_scope() as session:
            yield session
    except (KeyError, ValueError, MigrationError) as exc:
        raise fail(error_message(exc)) from None


def error_message(exc: BaseException) -> str:
    if isinstance(exc, KeyError) and exc.args:
        return str(exc.args[0])
    return str(exc)


@contextmanager
def cli_span(command: str) -> Iterator[None]:
    """Trace a command that creates or runs work, so that work joins the command's trace."""
    from tasque2.logs import configure_logging
    from tasque2.telemetry import configure_telemetry, flush_telemetry, span

    configure_logging(os.environ.get("TASQUE2_LOG_LEVEL") or "WARNING")
    configure_telemetry("cli")
    try:
        with span(f"tasque.cli {command}", attributes={"tasque.cli.command": command}):
            yield
    finally:
        flush_telemetry()


def parse_json_object(raw: str | None, *, option: str) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except ValueError as exc:
        raise typer.BadParameter(f"{option} is not valid JSON: {exc}") from None
    if not isinstance(parsed, dict):
        raise typer.BadParameter(f"{option} must be a JSON object.")
    return parsed


def existing_file(path: Path, *, option: str) -> Path:
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise typer.BadParameter(f"{option}: file does not exist: {resolved}")
    return resolved


def model_profile(value: str) -> str | None:
    try:
        return get_settings().normalize_model_profile(value)
    except ValueError as exc:
        raise typer.BadParameter(str(exc), param_hint="'--profile'") from None


def runtime_contract(
    *, profile: str | None, model: str | None = None, extra: dict[str, Any] | None = None
) -> dict[str, Any]:
    contract = dict(extra or {})
    if profile and (normalized := model_profile(profile)):
        contract["model_profile"] = normalized
    if model:
        contract["model"] = model
    return contract


def emit_json(data: Any) -> None:
    console.file.write(json.dumps(data, indent=2, sort_keys=True, default=str) + "\n")


def echo(text: str) -> None:
    """Print text exactly as written: no markup, highlighting, emoji codes or wrapping."""
    console.file.write(text if text.endswith("\n") else f"{text}\n")


def fail(message: str, *, code: int = 1) -> typer.Exit:
    console.print(f"[red]{escape(message)}[/red]")
    return typer.Exit(code=code)
