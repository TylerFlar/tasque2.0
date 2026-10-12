"""Speech to text for Discord voice messages, on this machine.

A voice message is transcribed before it is routed, and its words go on as typed text does. The speech model
(faster-whisper, on the processor) runs in a child process, ``python -m tasque2.discord.voice``, so a crash, a
hang or a bad clip never reaches the daemon, and no model stays in its memory. The child reads the clip on
stdin and writes the words on stdout as UTF-8; on any error it writes the reason on stderr and exits 1.
``fetch`` downloads the model once, ahead of the first message.

Only the child imports faster-whisper; this module imports the standard library alone.
"""

from __future__ import annotations

import argparse
import io
import logging
import os
import subprocess
import sys

logger = logging.getLogger(__name__)

MODULE = "tasque2.discord.voice"
TRANSCRIBE_TIMEOUT_SECONDS = 10 * 60
FETCH_TIMEOUT_SECONDS = 60 * 60
MAX_CPU_THREADS = 8
_NO_WINDOW = getattr(subprocess, "CREATE_NO_WINDOW", 0)


class VoiceError(RuntimeError):
    """A clip could not be transcribed: the child failed, did not answer in time, or could not start."""


def transcribe(data: bytes, *, model: str, language: str | None = None) -> str:
    """The words spoken in an audio clip, stripped: empty when nothing was said. Raises ``VoiceError``."""
    command = [sys.executable, "-m", MODULE, "--model", model]
    if language:
        command += ["--language", language]
    return _child(command, data, TRANSCRIBE_TIMEOUT_SECONDS).strip()


def fetch(model: str) -> None:
    """Download the model once, so the first voice message need not wait for it; a failure logs a warning."""
    try:
        _child([sys.executable, "-m", MODULE, "--model", model, "--fetch"], b"", FETCH_TIMEOUT_SECONDS)
    except VoiceError as exc:
        logger.warning("Could not download the speech model %s: %s", model, exc)


def _child(command: list[str], clip: bytes, timeout: float) -> str:
    """What the child wrote on stdout; ``VoiceError`` with the last line of its stderr when it fails."""
    try:
        done = subprocess.run(command, input=clip, capture_output=True, timeout=timeout, creationflags=_NO_WINDOW)
    except subprocess.TimeoutExpired as exc:
        raise VoiceError(f"no answer within {timeout:g} seconds") from exc
    except OSError as exc:
        raise VoiceError(f"could not start: {exc}") from exc
    if done.returncode != 0:
        lines = done.stderr.decode("utf-8", errors="replace").strip().splitlines()
        raise VoiceError(lines[-1] if lines else f"exit code {done.returncode}")
    return done.stdout.decode("utf-8", errors="replace")


def cpu_threads() -> int:
    """Half the logical processors, at most ``MAX_CPU_THREADS``: the rest of the machine stays responsive."""
    return max(1, min(MAX_CPU_THREADS, (os.cpu_count() or 2) // 2))


def main(argv: list[str] | None = None) -> int:
    """The child: transcribe the clip on stdin, or with ``--fetch`` download the model, then exit."""
    parser = argparse.ArgumentParser(prog=f"python -m {MODULE}", description="Speech to text for one clip.")
    parser.add_argument("--model", required=True)
    parser.add_argument("--language", default="")
    parser.add_argument("--fetch", action="store_true", help="download the model and exit")
    args = parser.parse_args(argv)
    try:
        if args.fetch:
            from faster_whisper.utils import download_model

            download_model(args.model)
            return 0
        from faster_whisper import WhisperModel

        clip = sys.stdin.buffer.read()
        # The processor on purpose: no GPU libraries to install, and the GPU stays free.
        whisper = WhisperModel(args.model, device="cpu", compute_type="int8", cpu_threads=cpu_threads())
        segments, _ = whisper.transcribe(
            io.BytesIO(clip), language=args.language or None, vad_filter=True, condition_on_previous_text=False
        )
        words = " ".join(text for segment in segments if (text := segment.text.strip()))
    except Exception as exc:  # noqa: BLE001 - every failure is the parent's to report
        print(f"{type(exc).__name__}: {' '.join(str(exc).split())}", file=sys.stderr)
        return 1
    sys.stdout.buffer.write(words.encode("utf-8"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
