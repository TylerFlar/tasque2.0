"""Subprocess execution for provider CLIs.

The prompt goes over stdin (large prompts would exceed the Windows command-line limit),
stdout and stderr are drained on reader threads, and the result inbox is polled: once the
worker has submitted its result, the whole process tree is terminated, since anything the
agent does after submitting is not part of the work item.
"""

from __future__ import annotations

import logging
import os
import signal
import subprocess
import threading
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)

RESULT_POLL_SECONDS = 0.5
TERMINATION_GRACE_SECONDS = 5.0


@dataclass(frozen=True)
class ProcessResult:
    returncode: int | None
    stdout: str
    stderr: str
    terminated_after_result: bool
    launch_error: str | None = None


def run_process(
    argv: Sequence[str],
    *,
    stdin_text: str,
    cwd: str | None,
    env: dict[str, str],
    result_ready: Callable[[], bool] | None = None,
    exit_grace_seconds: float = 0.0,
) -> ProcessResult:
    """Run ``argv`` to completion, or until ``result_ready`` reports a submitted result.

    After a result arrives the process gets ``exit_grace_seconds`` to finish on its own
    (flushing its final events and telemetry) before the tree is terminated.
    """
    kwargs: dict[str, Any] = {
        "stdin": subprocess.PIPE,
        "stdout": subprocess.PIPE,
        "stderr": subprocess.PIPE,
        "cwd": cwd,
        "env": env,
        "text": True,
        "encoding": "utf-8",
        "errors": "replace",
    }
    if os.name == "nt":
        kwargs["creationflags"] = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
    else:
        kwargs["start_new_session"] = True
    try:
        process = subprocess.Popen(list(argv), **kwargs)
    except OSError as exc:
        reason = "command line too long" if getattr(exc, "winerror", None) == 206 else "command not found"
        return ProcessResult(None, "", str(exc), False, launch_error=f"Provider {reason}: {argv[0]}")

    stdout_parts: list[str] = []
    stderr_parts: list[str] = []
    readers = [_drain(process.stdout, stdout_parts), _drain(process.stderr, stderr_parts)]
    writer = _feed(process, stdin_text)

    terminated_after_result = False
    while process.poll() is None:
        if result_ready is not None and _result_arrived(result_ready):
            deadline = time.monotonic() + max(0.0, exit_grace_seconds)
            while process.poll() is None and time.monotonic() < deadline:
                time.sleep(RESULT_POLL_SECONDS)
            if process.poll() is None:
                terminated_after_result = True
                terminate_process_tree(process)
            break
        time.sleep(RESULT_POLL_SECONDS)

    returncode = _wait(process)
    for reader in readers:
        if reader is not None:
            reader.join(timeout=TERMINATION_GRACE_SECONDS)
    if writer is not None:
        writer.join(timeout=1.0)
    return ProcessResult(returncode, "".join(stdout_parts), "".join(stderr_parts), terminated_after_result)


def terminate_process_tree(process: subprocess.Popen[Any]) -> None:
    if process.poll() is not None:
        return
    if os.name == "nt":
        subprocess.run(
            ["taskkill", "/PID", str(process.pid), "/T", "/F"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        return
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        return
    except OSError:
        process.kill()


def _result_arrived(result_ready: Callable[[], bool]) -> bool:
    """A failed poll counts as not yet: raising here would orphan the running process."""
    try:
        return bool(result_ready())
    except Exception:  # noqa: BLE001 - the next poll tries again
        logger.warning("Result inbox poll failed", exc_info=True)
        return False


def _wait(process: subprocess.Popen[Any]) -> int | None:
    try:
        return process.wait(timeout=TERMINATION_GRACE_SECONDS)
    except subprocess.TimeoutExpired:
        terminate_process_tree(process)
        try:
            return process.wait(timeout=TERMINATION_GRACE_SECONDS)
        except subprocess.TimeoutExpired:
            return process.poll()


def _drain(stream: Any, buffer: list[str]) -> threading.Thread | None:
    if stream is None:
        return None

    def read() -> None:
        try:
            buffer.append(stream.read() or "")
        except OSError as exc:
            buffer.append(f"\n[provider stream read failed: {exc}]\n")

    thread = threading.Thread(target=read, daemon=True)
    thread.start()
    return thread


def _feed(process: subprocess.Popen[Any], text: str) -> threading.Thread | None:
    if process.stdin is None:
        return None

    def write() -> None:
        try:
            process.stdin.write(text)
            process.stdin.close()
        except OSError:
            return

    thread = threading.Thread(target=write, daemon=True)
    thread.start()
    return thread
