"""Speech to text for voice messages: the child process the daemon starts, and the child itself."""

from __future__ import annotations

import io
import logging
import subprocess
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

from tasque2.discord import voice

CHILD = [sys.executable, "-m", "tasque2.discord.voice"]


class _Run:
    """``subprocess.run`` faked: each call is recorded, and answered as told."""

    def __init__(
        self, *, returncode: int = 0, stdout: bytes = b"", stderr: bytes = b"", error: Exception | None = None
    ) -> None:
        self.answer = subprocess.CompletedProcess([], returncode, stdout=stdout, stderr=stderr)
        self.error = error
        self.calls: list[dict[str, Any]] = []

    def __call__(self, command: list[str], **options: Any) -> subprocess.CompletedProcess:
        self.calls.append({"command": command, **options})
        if self.error is not None:
            raise self.error
        return self.answer


def _fake_run(monkeypatch: pytest.MonkeyPatch, **answer: Any) -> _Run:
    run = _Run(**answer)
    monkeypatch.setattr(voice.subprocess, "run", run)
    return run


def test_a_clip_is_transcribed_in_a_child_process(monkeypatch: pytest.MonkeyPatch) -> None:
    run = _fake_run(monkeypatch, stdout=b" Two eggs and toast. \r\n")

    assert voice.transcribe(b"OggS clip", model="large-v3-turbo", language="en") == "Two eggs and toast."

    [call] = run.calls
    assert call["command"] == [*CHILD, "--model", "large-v3-turbo", "--language", "en"]
    assert call["input"] == b"OggS clip"
    assert call["capture_output"] is True
    assert call["timeout"] == voice.TRANSCRIBE_TIMEOUT_SECONDS == 600
    assert call["creationflags"] == getattr(subprocess, "CREATE_NO_WINDOW", 0)


def test_without_a_language_the_model_detects_it(monkeypatch: pytest.MonkeyPatch) -> None:
    run = _fake_run(monkeypatch)

    assert voice.transcribe(b"OggS clip", model="tiny", language=None) == ""
    assert run.calls[0]["command"] == [*CHILD, "--model", "tiny"]


def test_a_failed_child_raises_the_last_line_it_wrote(monkeypatch: pytest.MonkeyPatch) -> None:
    _fake_run(monkeypatch, returncode=1, stderr=b"a warning first\nRuntimeError: Invalid data found\n")
    with pytest.raises(voice.VoiceError, match=r"^RuntimeError: Invalid data found$"):
        voice.transcribe(b"not audio", model="tiny")

    _fake_run(monkeypatch, returncode=3, stdout=b"half an answer")
    with pytest.raises(voice.VoiceError, match=r"^exit code 3$"):
        voice.transcribe(b"OggS clip", model="tiny")


def test_a_child_that_hangs_or_cannot_start_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    _fake_run(monkeypatch, error=subprocess.TimeoutExpired(CHILD, 600))
    with pytest.raises(voice.VoiceError, match="no answer within 600 seconds"):
        voice.transcribe(b"OggS clip", model="tiny")

    _fake_run(monkeypatch, error=FileNotFoundError("python is gone"))
    with pytest.raises(voice.VoiceError, match="could not start: python is gone"):
        voice.transcribe(b"OggS clip", model="tiny")


def test_fetch_downloads_the_model_in_the_child(monkeypatch: pytest.MonkeyPatch, caplog) -> None:
    run = _fake_run(monkeypatch)

    with caplog.at_level(logging.WARNING, logger="tasque2.discord.voice"):
        voice.fetch("large-v3-turbo")

    [call] = run.calls
    assert call["command"] == [*CHILD, "--model", "large-v3-turbo", "--fetch"]
    assert call["timeout"] == voice.FETCH_TIMEOUT_SECONDS == 3600
    assert caplog.records == []


def test_a_failed_fetch_logs_one_warning(monkeypatch: pytest.MonkeyPatch, caplog) -> None:
    _fake_run(monkeypatch, returncode=1, stderr=b"ConnectError: the network is unreachable\n")

    with caplog.at_level(logging.WARNING, logger="tasque2.discord.voice"):
        voice.fetch("large-v3-turbo")

    [record] = caplog.records
    assert record.levelno == logging.WARNING
    assert record.getMessage() == (
        "Could not download the speech model large-v3-turbo: ConnectError: the network is unreachable"
    )


def test_the_model_takes_half_the_processors_and_at_most_eight(monkeypatch: pytest.MonkeyPatch) -> None:
    for count, threads in ((32, 8), (12, 6), (1, 1), (None, 1)):
        monkeypatch.setattr(voice.os, "cpu_count", lambda count=count: count)
        assert voice.cpu_threads() == threads


# --- the child ----------------------------------------------------------------------------------------------


@pytest.fixture()
def whisper(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """faster-whisper faked: what the model was built with and heard, and the segments it answers."""
    seen: dict[str, Any] = {"segments": [" Two eggs", "  ", " and toast with crème fraîche. "], "fetched": []}

    class WhisperModel:
        def __init__(self, model: str, **options: Any) -> None:
            seen["built"] = (model, options)

        def transcribe(self, audio: io.BytesIO, **options: Any):
            seen["clip"], seen["options"] = audio.read(), options
            if isinstance(seen["segments"], Exception):
                raise seen["segments"]
            return (SimpleNamespace(text=text) for text in seen["segments"]), SimpleNamespace(language="en")

    package, utils = ModuleType("faster_whisper"), ModuleType("faster_whisper.utils")
    package.WhisperModel = WhisperModel
    utils.download_model = seen["fetched"].append
    monkeypatch.setitem(sys.modules, "faster_whisper", package)
    monkeypatch.setitem(sys.modules, "faster_whisper.utils", utils)
    return seen


def _child(monkeypatch: pytest.MonkeyPatch, argv: list[str], clip: bytes = b"") -> tuple[int, bytes, str]:
    """Run the child's ``main`` with ``clip`` on stdin; its exit code, stdout and stderr."""
    stdout, stderr = io.BytesIO(), io.StringIO()
    monkeypatch.setattr(sys, "stdin", SimpleNamespace(buffer=io.BytesIO(clip)))
    monkeypatch.setattr(sys, "stdout", SimpleNamespace(buffer=stdout))
    monkeypatch.setattr(sys, "stderr", stderr)
    code = voice.main(argv)
    return code, stdout.getvalue(), stderr.getvalue()


def test_the_child_writes_the_words_it_heard_as_utf8(monkeypatch: pytest.MonkeyPatch, whisper) -> None:
    code, out, err = _child(monkeypatch, ["--model", "large-v3-turbo", "--language", "en"], b"OggS clip")

    assert (code, err) == (0, "")
    assert out == "Two eggs and toast with crème fraîche.".encode()
    assert whisper["built"] == (
        "large-v3-turbo",
        {"device": "cpu", "compute_type": "int8", "cpu_threads": voice.cpu_threads()},
    )
    assert whisper["clip"] == b"OggS clip"
    assert whisper["options"] == {"language": "en", "vad_filter": True, "condition_on_previous_text": False}
    assert whisper["fetched"] == []


def test_the_child_lets_the_model_detect_the_language_and_hears_silence_as_nothing(
    monkeypatch: pytest.MonkeyPatch, whisper
) -> None:
    whisper["segments"] = []

    assert _child(monkeypatch, ["--model", "tiny"], b"OggS clip") == (0, b"", "")
    assert whisper["options"]["language"] is None


def test_the_child_downloads_the_model_with_fetch(monkeypatch: pytest.MonkeyPatch, whisper) -> None:
    assert _child(monkeypatch, ["--model", "large-v3-turbo", "--fetch"]) == (0, b"", "")
    assert whisper["fetched"] == ["large-v3-turbo"]
    assert "built" not in whisper


def test_the_child_exits_1_with_the_reason_on_one_line(monkeypatch: pytest.MonkeyPatch, whisper) -> None:
    whisper["segments"] = RuntimeError("Invalid data found\nwhen processing input")

    assert _child(monkeypatch, ["--model", "tiny"], b"not audio") == (
        1,
        b"",
        "RuntimeError: Invalid data found when processing input\n",
    )


def test_the_child_exits_1_when_faster_whisper_is_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "faster_whisper", None)

    code, out, err = _child(monkeypatch, ["--model", "tiny"], b"OggS clip")

    assert (code, out) == (1, b"")
    assert err.startswith("ModuleNotFoundError: ") and "faster_whisper" in err


def test_the_child_runs_as_a_module(monkeypatch: pytest.MonkeyPatch) -> None:
    """The real child, started as the daemon starts it; it never reaches the model."""
    monkeypatch.setenv("PYTHONPATH", str(Path(voice.__file__).parents[2]))

    assert "--fetch" in voice._child([*CHILD, "--help"], b"", 60)
    with pytest.raises(voice.VoiceError, match="the following arguments are required: --model"):
        voice._child(CHILD, b"", 60)
