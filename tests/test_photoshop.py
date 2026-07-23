from __future__ import annotations

from pathlib import Path

import pytest

from tasque2 import photoshop


def test_jsx_string_escapes() -> None:
    assert photoshop._jsx_string("a\\b") == "a\\\\b"
    assert photoshop._jsx_string('say "hi"') == 'say \\"hi\\"'


def test_edit_file_builds_script_and_clamps_quality(tmp_path: Path) -> None:
    src = tmp_path / "in.png"
    src.write_bytes(b"x")
    out = tmp_path / "out.jpg"
    out.write_bytes(b"y")  # pre-exist so the post-run is_file() check passes
    captured: dict[str, str] = {}

    def fake_runner(script: str) -> str:
        captured["script"] = script
        return "ok"

    result = photoshop.edit_file(
        src, out, "doc.flatten();", quality=99, runner=fake_runner
    )
    assert result["output_path"] == str(out)
    script = captured["script"]
    assert "doc.flatten();" in script  # body injected
    assert photoshop._jsx_string(str(src)) in script  # source path escaped in
    assert photoshop._jsx_string(str(out)) in script
    assert "opts.quality = 12;" in script  # 99 clamped to 12
    assert "SaveOptions.DONOTSAVECHANGES" in script  # never overwrites original


def test_edit_file_raises_on_jsx_error(tmp_path: Path) -> None:
    src = tmp_path / "in.png"
    src.write_bytes(b"x")
    with pytest.raises(photoshop.PhotoshopError, match="boom"):
        photoshop.edit_file(
            src, tmp_path / "o.jpg", "bad();", runner=lambda _s: "ERROR: boom"
        )


def test_edit_file_missing_source(tmp_path: Path) -> None:
    with pytest.raises(photoshop.PhotoshopError, match="No file"):
        photoshop.edit_file(tmp_path / "nope.png", tmp_path / "o.jpg", "x", runner=lambda _s: "ok")


def test_edit_file_missing_output_raises(tmp_path: Path) -> None:
    src = tmp_path / "in.png"
    src.write_bytes(b"x")
    # Runner returns ok but writes no file -> we must not report false success.
    with pytest.raises(photoshop.PhotoshopError, match="no output file"):
        photoshop.edit_file(src, tmp_path / "missing.jpg", "x", runner=lambda _s: "ok")


class _FakeDocs:
    Count = 2


class _FakeApp:
    Name = "Adobe Photoshop"
    Version = "27.8.0"
    Documents = _FakeDocs()


def test_status_available(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(photoshop, "get_app", lambda: _FakeApp())
    status = photoshop.status()
    assert status["available"] is True
    assert status["version"] == "27.8.0"
    assert status["open_documents"] == 2


def test_status_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    def _raise() -> object:
        raise photoshop.PhotoshopError("Photoshop is not running.")

    monkeypatch.setattr(photoshop, "get_app", _raise)
    status = photoshop.status()
    assert status["available"] is False
    assert "not running" in status["reason"]


class _RecordApp:
    def __init__(self) -> None:
        self.flagged: list[str] = []

    def _FlagAsMethod(self, name: str) -> None:
        self.flagged.append(name)

    def DoJavascript(self, script: str) -> str:
        return f"ran:{script[:3]}"


def test_run_jsx_flags_method_and_returns(monkeypatch: pytest.MonkeyPatch) -> None:
    app = _RecordApp()
    monkeypatch.setattr(photoshop, "get_app", lambda: app)
    out = photoshop.run_jsx("hello world")
    assert out == "ran:hel"
    assert "DoJavascript" in app.flagged


class _BoomApp:
    def _FlagAsMethod(self, name: str) -> None:
        pass

    def DoJavascript(self, script: str) -> str:
        raise RuntimeError("com boom")


def test_run_jsx_wraps_com_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(photoshop, "get_app", lambda: _BoomApp())
    with pytest.raises(photoshop.PhotoshopError, match="com boom"):
        photoshop.run_jsx("x")
