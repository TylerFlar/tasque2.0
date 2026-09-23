from __future__ import annotations

import re

import pytest

from tasque2.text import compress_text, one_line, truncate


def test_compress_text_keeps_short_text_whole() -> None:
    assert compress_text("  hello\r\nworld  ") == "hello\nworld"
    assert compress_text("") == ""


def test_compress_text_strips_ansi_and_shortens_uuids() -> None:
    text = "\x1b[31mfailed\x1b[0m for 123e4567-e89b-12d3-a456-426614174000"

    assert compress_text(text) == "failed for 123e4567..."


def test_compress_text_collapses_repeated_lines() -> None:
    assert compress_text("same\nsame\nsame\nother") == "same\nother"
    assert compress_text("\n".join(["ping", "pong"] * 5)).splitlines() == [
        "ping",
        "pong",
        "ping",
        "pong",
        "ping",
        "pong",
        "[repeated line suppressed: ping]",
        "[repeated line suppressed: pong]",
    ]


def test_compress_text_keeps_the_head_and_tail_of_many_lines() -> None:
    text = "\n".join(f"line {index}" for index in range(100))

    lines = compress_text(text, preserve_lines=10).splitlines()

    assert lines[:5] == [f"line {index}" for index in range(5)]
    assert lines[5] == "[omitted 90 middle line(s)]"
    assert lines[6:] == [f"line {index}" for index in range(95, 100)]


@pytest.mark.parametrize("max_chars", [80, 160, 300, 2000])
def test_compress_text_splits_its_budget_between_head_and_tail(max_chars: int) -> None:
    text = "".join(chr(ord("a") + index % 26) for index in range(5000))

    compact = compress_text(text, max_chars=max_chars)

    head, marker, tail = compact.split("\n")
    assert len(compact) <= max_chars
    assert text.startswith(head) and text.endswith(tail)
    assert abs(len(head) - len(tail)) <= 1
    assert len(head) >= (max_chars - 40) // 2
    assert re.fullmatch(r"\[omitted \d+ character\(s\)\]", marker)
    assert int(marker.split()[1]) == len(text) - len(head) - len(tail)


def test_one_line_joins_lines_within_the_limit() -> None:
    assert one_line("first line\n\n  second   line\n") == "first line second line"
    preview = one_line("Start of the note. " + "filler " * 100 + "End of the note.", limit=160)
    assert len(preview) <= 160
    assert preview.startswith("Start of the note.")
    assert preview.endswith("End of the note.")


def test_truncate_marks_cut_text() -> None:
    assert truncate("short", 10) == "short"
    assert truncate("x" * 50, 20) == "x" * 8 + "\n[truncated]"
    assert len(truncate("x" * 50, 20)) == 20
