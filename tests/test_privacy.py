from __future__ import annotations

import subprocess
from pathlib import Path

import pytest
from typer.testing import CliRunner

from tasque2.cli import app
from tasque2.ops.privacy import denylist, install_hook, scan_range, scan_text

# The samples are put together at run time, so this file never trips the scan it tests.
EMAIL = "jane.doe" + "@" + "gmail.com"
OTHER_EMAIL = "pat" + "@" + "corp.io"
LONG_ID = "1234567890" + "123456789"
PHONES = "call (555) 555" + "-0123 or 555-555" + "-0123"
WINDOWS_HOME = "C:" + "\\Users\\jane\\Documents"
POSIX_HOMES = "/ho" + "me/jane/.config and /Us" + "ers/jane/x"


def _kinds(text: str, terms: list[str] | None = None) -> list[str]:
    return [finding.kind for finding in scan_text(text, terms=terms)]


def test_personal_details_are_found_and_placeholders_are_not() -> None:
    assert _kinds(f"contact {EMAIL}") == ["email"]
    assert _kinds("Co-Authored-By: Claude <noreply@anthropic.com>, see user@example.com") == []
    assert _kinds(f"thread {LONG_ID} posted") == ["long_number"]
    assert _kinds(PHONES) == ["phone", "phone"]
    assert _kinds(f"saved under {WINDOWS_HOME}") == ["user_path"]
    assert _kinds(POSIX_HOMES) == ["user_path", "user_path"]
    assert _kinds(r"C:\Users\<name>\x and /home/$USER and C:\Users\Public") == []
    assert _kinds("version 2026-10-06 21:21:36, 1791288000.0 seconds, sha 4f2a9c") == []
    assert _kinds("Lunch at the Usual Place", terms=["usual place"]) == ["denylisted"]


def test_the_denylist_skips_comments_and_blank_lines(tmp_path: Path) -> None:
    path = tmp_path / "deny.txt"
    path.write_text("# names\nJane Doe\n\n  Acme Bank  \n", encoding="utf-8")
    assert denylist(path) == ["Jane Doe", "Acme Bank"]


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True, check=True).stdout.strip()


@pytest.fixture()
def repo(tmp_path: Path) -> Path:
    root = tmp_path / "repo"
    root.mkdir()
    _git(root, "init", "-q")
    _git(root, "config", "user.email", "tests@example.com")
    _git(root, "config", "user.name", "Tests")
    (root / "old.txt").write_text(f"already public: {EMAIL}\n", encoding="utf-8")
    _git(root, "add", ".")
    _git(root, "commit", "-q", "-m", "first")
    return root


def test_only_what_the_range_adds_is_scanned(repo: Path) -> None:
    base = _git(repo, "rev-parse", "HEAD")
    (repo / "new.py").write_text(f"x = 1\nOWNER = '{OTHER_EMAIL}'\n", encoding="utf-8")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", f"Add owner for thread {LONG_ID}")

    findings = scan_range(repo, base, "HEAD", terms=[])

    assert {(finding.kind, finding.match) for finding in findings} == {
        ("email", OTHER_EMAIL),
        ("long_number", LONG_ID),
    }
    assert any(finding.where == "new.py:2" for finding in findings)
    assert not any("jane.doe" in finding.match for finding in findings)  # already public, not re-flagged


def test_a_detail_added_then_removed_in_the_range_is_still_found(repo: Path) -> None:
    base = _git(repo, "rev-parse", "HEAD")
    (repo / "notes.py").write_text(f"OWNER = '{OTHER_EMAIL}'\n", encoding="utf-8")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "add the owner")
    (repo / "notes.py").write_text("OWNER = None\n", encoding="utf-8")
    _git(repo, "commit", "-q", "-am", "drop the owner")

    findings = scan_range(repo, base, "HEAD", terms=[])

    assert [(finding.kind, finding.match, finding.where) for finding in findings] == [
        ("email", OTHER_EMAIL, "notes.py:1")
    ]


def test_the_command_fails_on_findings_and_passes_when_clean(repo: Path) -> None:
    base = _git(repo, "rev-parse", "HEAD")
    (repo / "clean.py").write_text("print('hello')\n", encoding="utf-8")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "clean change")
    clean = CliRunner().invoke(app, ["privacy-scan", "--repo", str(repo), "--base", base])
    assert clean.exit_code == 0, clean.output

    (repo / "dirty.py").write_text(f"HOME = r'{WINDOWS_HOME}'\n", encoding="utf-8")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "dirty change")
    dirty = CliRunner().invoke(app, ["privacy-scan", "--repo", str(repo), "--base", base])
    assert dirty.exit_code == 1
    assert "user_path" in dirty.output


def test_the_hook_is_installed_in_front_of_push(repo: Path) -> None:
    path = install_hook(repo)
    assert path.name == "pre-push"
    text = path.read_text(encoding="utf-8")
    assert "privacy-scan --base" in text and "\r\n" not in text
