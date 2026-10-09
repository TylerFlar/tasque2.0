"""The ``tasque2`` command line."""

from tasque2.cli import (  # noqa: F401 - registers commands
    admin,
    daemon,
    memory,
    ops,
    schedules,
    work,
    workflows,
    workshop,
)
from tasque2.cli._common import app, cli_session_scope, console

__all__ = ["app", "cli_session_scope", "console"]
