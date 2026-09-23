"""A local observability stack the daemon brings up before its telemetry starts.

With ``TASQUE2_TELEMETRY_STACK`` naming a docker compose file (the bundled one is
``deploy/observability/docker-compose.yml``), ``tasque2 daemon`` makes sure Docker is running
(starting Docker Desktop when it is installed and stopped), runs ``docker compose up -d``, points
the OTLP exporters at the stack unless an endpoint is already set, and waits until the stack
answers. The workers and the MCP server inherit the endpoint from the daemon. With
``TASQUE2_TELEMETRY_STACK_OPEN=true`` the dashboard opens in a browser when the daemon runs in an
interactive terminal.

Nothing here stops the daemon: when a step fails it logs why and the daemon runs without the stack.
"""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
import webbrowser
from collections.abc import Callable
from pathlib import Path

from tasque2.config import Settings, get_settings

logger = logging.getLogger(__name__)

DEFAULT_ENDPOINT = "http://localhost:4318"
_NO_WINDOW = getattr(subprocess, "CREATE_NO_WINDOW", 0)


def ensure_telemetry_stack(
    settings: Settings | None = None,
    *,
    interactive: bool | None = None,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> bool:
    """Bring the configured stack up and point telemetry at it; True when it is running."""
    settings = settings or get_settings()
    compose = (settings.telemetry_stack or "").strip()
    if not compose:
        return False
    path = Path(compose).expanduser()
    if not path.is_absolute():
        path = settings.resolved_project_dir / path
    if not path.is_file():
        logger.warning("Observability stack: %s not found; running without it", path)
        return False
    deadline = clock() + max(10, settings.telemetry_stack_timeout_seconds)

    if not docker_ready():
        if not start_docker_desktop():
            logger.warning("Observability stack: Docker is not running and Docker Desktop was not found")
            return False
        logger.info("Observability stack: starting Docker Desktop")
        while not docker_ready():
            if clock() > deadline:
                logger.warning("Observability stack: Docker did not start in time; running without it")
                return False
            sleep(3)

    logger.info("Observability stack: docker compose up (%s)", path)
    error = compose_up(path, timeout=max(30.0, deadline - clock()))
    if error:
        logger.warning("Observability stack: docker compose failed (%s); running without it", error)
        return False

    from tasque2.telemetry.setup import export_dotenv_otel_settings

    export_dotenv_otel_settings()
    endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "").strip() or DEFAULT_ENDPOINT
    os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"] = endpoint
    dashboard = settings.telemetry_stack_dashboard
    while not (answers(endpoint.rstrip("/") + "/v1/traces") and answers(dashboard)):
        if clock() > deadline:
            logger.warning("Observability stack: started but not answering yet; telemetry exports once it is up")
            return True
        sleep(2)
    logger.info("Observability stack ready: traces, metrics and logs at %s", dashboard)
    if settings.telemetry_stack_open and (sys.stderr.isatty() if interactive is None else interactive):
        webbrowser.open(dashboard)
    return True


def docker_ready() -> bool:
    """True when the docker CLI reaches a running engine."""
    if shutil.which("docker") is None:
        return False
    try:
        result = subprocess.run(
            ["docker", "info", "--format", "{{.ServerVersion}}"],
            capture_output=True,
            text=True,
            timeout=20,
            creationflags=_NO_WINDOW,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    return result.returncode == 0


def start_docker_desktop() -> bool:
    """Launch Docker Desktop where it is installed; False when there is nothing to launch."""
    if sys.platform == "win32":
        app = Path(os.environ.get("ProgramFiles", r"C:\Program Files")) / "Docker" / "Docker" / "Docker Desktop.exe"
        if not app.is_file():
            return False
        os.startfile(app)  # noqa: S606 - a fixed local application path
        return True
    if sys.platform == "darwin":
        return subprocess.run(["open", "-a", "Docker"], capture_output=True).returncode == 0
    return False


def compose_up(path: Path, *, timeout: float) -> str | None:
    """Run ``docker compose up -d`` for the file; the error text, or None when it worked."""
    try:
        result = subprocess.run(
            ["docker", "compose", "-f", str(path), "up", "-d"],
            capture_output=True,
            text=True,
            timeout=timeout,
            creationflags=_NO_WINDOW,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return str(exc)
    if result.returncode != 0:
        lines = (result.stderr or result.stdout or "").strip().splitlines()
        return lines[-1] if lines else f"exit code {result.returncode}"
    return None


def answers(url: str) -> bool:
    """True when anything answers HTTP at the URL, an error status included."""
    try:
        with urllib.request.urlopen(url, timeout=3):  # noqa: S310 - local endpoints from settings
            return True
    except urllib.error.HTTPError:
        return True
    except (OSError, ValueError):
        return False
