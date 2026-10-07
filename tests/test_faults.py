from __future__ import annotations

import json
import logging
from datetime import timedelta
from pathlib import Path

from tasque2.logs import configure_logging, reset_logging
from tasque2.models import utc_now
from tasque2.ops.faults import (
    FaultLedgerHandler,
    fault_entry,
    fault_summary,
    faults_path,
    read_faults,
    recurring,
)


def _raise_in_here() -> None:
    raise ValueError("bad value in a digest")


def _log_caught(logger: logging.Logger) -> None:
    try:
        _raise_in_here()
    except ValueError:
        logger.exception("Digest failed")


def _record(logger_name: str = "tasque2.test") -> logging.LogRecord:
    captured: list[logging.LogRecord] = []

    class Capture(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            captured.append(record)

    logger = logging.getLogger(logger_name)
    handler = Capture(level=logging.ERROR)
    logger.addHandler(handler)
    try:
        _log_caught(logger)
    finally:
        logger.removeHandler(handler)
    return captured[0]


def test_a_caught_exception_becomes_a_ledger_entry_with_its_traceback_and_frame(isolated: Path, monkeypatch) -> None:
    # Code outside Tasque and its extensions is named by where it logged ...
    assert fault_entry(_record())["frame"]["function"] == "_log_caught"
    # ... code inside is named by the deepest frame of its own, where the error was raised.
    monkeypatch.setattr("tasque2.ops.faults._owned_roots", lambda: [Path(__file__).resolve().parent])
    entry = fault_entry(_record())

    assert entry["exc_type"] == "ValueError"
    assert entry["exc_message"] == "bad value in a digest"
    assert entry["message"] == "Digest failed"
    assert "ValueError: bad value in a digest" in entry["traceback"]
    # The innermost frame is where the error was raised, not where it was logged.
    assert entry["frame"]["function"] == "_raise_in_here"
    assert len(entry["signature"]) == 12


def test_the_same_fault_keeps_its_signature_and_a_different_place_does_not(isolated: Path) -> None:
    first, second = fault_entry(_record()), fault_entry(_record())
    other_logger = fault_entry(_record("tasque2.other"))

    assert first["signature"] == second["signature"]
    assert other_logger["signature"] != first["signature"]


def test_configure_logging_sends_errors_to_the_ledger_but_not_warnings(isolated: Path) -> None:
    configure_logging("INFO")
    try:
        logger = logging.getLogger("tasque2.ledger")
        logger.warning("just a warning")
        _log_caught(logger)
    finally:
        reset_logging()

    entries = list(read_faults())
    assert [entry["message"] for entry in entries] == ["Digest failed"]
    assert faults_path().is_file()


def test_the_handler_never_raises_into_the_code_that_logged(isolated: Path, monkeypatch) -> None:
    handler = FaultLedgerHandler()
    monkeypatch.setattr("tasque2.ops.faults.faults_path", lambda: Path("Z:/nowhere/that/exists/faults.jsonl"))
    monkeypatch.setattr(logging, "raiseExceptions", False)

    handler.emit(_record())  # no exception escapes


def test_summary_groups_by_signature_and_marks_recurring_faults(isolated: Path) -> None:
    path = faults_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    now = utc_now()
    lines = []
    for offset_days, signature in ((0, "aaaaaaaaaaaa"), (1, "aaaaaaaaaaaa"), (0, "bbbbbbbbbbbb"), (30, "cccccccccccc")):
        lines.append(
            json.dumps(
                {
                    "at": (now - timedelta(days=offset_days)).isoformat(),
                    "logger": "tasque2.x",
                    "signature": signature,
                    "exc_type": "KeyError",
                    "frame": {"file": "tasque2/x.py", "function": "f"},
                    "message": "boom",
                }
            )
        )
    path.write_text("\n".join(lines) + "\nnot json\n", encoding="utf-8")

    summary = fault_summary(days=7, now=now)

    by_signature = {group["signature"]: group for group in summary}
    assert set(by_signature) == {"aaaaaaaaaaaa", "bbbbbbbbbbbb"}  # the 30-day-old one is outside the window
    assert by_signature["aaaaaaaaaaaa"]["count"] == 2
    assert by_signature["aaaaaaaaaaaa"]["days"] == 2
    assert recurring(by_signature["aaaaaaaaaaaa"])
    assert not recurring(by_signature["bbbbbbbbbbbb"])
    assert summary[0]["signature"] == "aaaaaaaaaaaa"


def test_a_full_ledger_rotates_and_both_files_are_read(isolated: Path, monkeypatch) -> None:
    monkeypatch.setattr("tasque2.ops.faults.MAX_BYTES", 10)
    handler = FaultLedgerHandler()
    handler.emit(_record())
    handler.emit(_record())

    assert faults_path().with_name("faults.1.jsonl").is_file()
    assert len(list(read_faults())) == 2
