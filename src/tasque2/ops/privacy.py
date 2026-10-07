"""Keep personal data out of a public repository: scan what a push would publish.

``scan_range`` reads the lines each commit in a range adds and the commits' messages, and reports
anything that looks personal: an email address, a long numeric id (a chat or account id), a phone
number, a path under a user's home folder, or a term from the gitignored denylist
``data/privacy-denylist.txt`` (one term per line, ``#`` comments). Each commit is read on its own,
because a push publishes every commit: a detail one commit adds and a later one removes is still
found. Only added lines are read, so history that is already public never blocks a push.
``tasque2 privacy-hook-install`` puts the scan in front of every ``git push`` from the repository.
"""

from __future__ import annotations

import re
import subprocess
from dataclasses import dataclass
from pathlib import Path

from tasque2.config import get_settings

DENYLIST_FILE = "privacy-denylist.txt"
ZERO_SHA = "0" * 40
HUNK = re.compile(r"^@@ -\d+(?:,\d+)? \+(\d+)")
PATTERNS = {
    "email": re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}"),
    "long_number": re.compile(r"(?<![\w.:/-])\d{15,22}(?![\w.])"),
    "phone": re.compile(r"(?<![\w-])(?:\+?1[\s.-])?\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4}(?![\w-])"),
    "user_path": re.compile(r"(?i)(?:\b[A-Z]:[\\/]+Users[\\/]+|/Users/|/home/)(?![<{$%]|Public\b|Default\b)[\w.-]+"),
}
ALLOWED_EMAIL_DOMAINS = ("example.com", "example.org", "example.net", "anthropic.com", "users.noreply.github.com")
HOOK = """#!/bin/sh
# Installed by `tasque2 privacy-hook-install`: refuse a push that would publish personal data.
root="$(git rev-parse --show-toplevel)"
scanner="$root/.venv/Scripts/tasque2.exe"
[ -x "$scanner" ] || scanner="$root/.venv/bin/tasque2"
status=0
while read local_ref local_sha remote_ref remote_sha; do
  [ "$local_sha" = "0000000000000000000000000000000000000000" ] && continue
  if [ "$remote_sha" = "0000000000000000000000000000000000000000" ]; then
    base="$(git merge-base "$local_sha" origin/main 2>/dev/null || git rev-list --max-parents=0 "$local_sha")"
  else
    base="$remote_sha"
  fi
  "$scanner" privacy-scan --base "$base" --head "$local_sha" || status=1
done
exit $status
"""


@dataclass(frozen=True)
class Finding:
    kind: str
    match: str
    where: str
    line: str


def denylist(path: Path | None = None) -> list[str]:
    path = path or get_settings().resolved_data_dir / DENYLIST_FILE
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return []
    return [line.strip() for line in lines if line.strip() and not line.lstrip().startswith("#")]


def scan_text(text: str, *, where: str = "", terms: list[str] | None = None) -> list[Finding]:
    findings: list[Finding] = []
    lowered_terms = [(term, term.lower()) for term in terms or []]
    for number, line in enumerate(text.splitlines(), start=1):
        place = f"{where}:{number}" if where else str(number)
        for kind, pattern in PATTERNS.items():
            for match in pattern.finditer(line):
                value = match.group(0)
                if kind == "email" and value.lower().split("@", 1)[1].endswith(ALLOWED_EMAIL_DOMAINS):
                    continue
                findings.append(Finding(kind, value, place, line.strip()[:200]))
        lowered = line.lower()
        for term, needle in lowered_terms:
            if needle in lowered:
                findings.append(Finding("denylisted", term, place, line.strip()[:200]))
    return findings


def _git(repo: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", "-C", str(repo), *args], capture_output=True, text=True, encoding="utf-8", errors="replace", check=True
    )
    return completed.stdout


def _added_lines(diff: str) -> list[tuple[str, str]]:
    """``(file:line, text)`` for each line a unified diff adds."""
    added: list[tuple[str, str]] = []
    current, number = "", 0
    for line in diff.splitlines():
        if line.startswith("+++ "):
            current = line[6:] if line.startswith("+++ b/") else line[4:]
        elif line.startswith("@@"):
            hunk = HUNK.match(line)
            number = int(hunk.group(1)) if hunk else 0
        elif line.startswith("+"):
            added.append((f"{current}:{number}", line[1:]))
            number += 1
    return added


def scan_range(repo: Path, base: str, head: str = "HEAD", *, terms: list[str] | None = None) -> list[Finding]:
    """What the commits in ``base..head`` add (each commit's diff, and its message) that looks personal."""
    terms = denylist() if terms is None else terms
    findings: list[Finding] = []
    for sha in _git(repo, "rev-list", "--reverse", f"{base}..{head}").split():
        diff = _git(repo, "show", "--format=", "--first-parent", "--unified=0", "--no-color", sha)
        for where, text in _added_lines(diff):
            for finding in scan_text(text, terms=terms):
                found = Finding(finding.kind, finding.match, where, finding.line)
                if found not in findings:
                    findings.append(found)
        message = _git(repo, "log", "-1", "--format=%B", sha)
        findings.extend(scan_text(message, where=f"commit {sha[:8]}", terms=terms))
    return findings


def install_hook(repo: Path) -> Path:
    hooks = Path(_git(repo, "rev-parse", "--git-path", "hooks").strip())
    if not hooks.is_absolute():
        hooks = repo / hooks
    hooks.mkdir(parents=True, exist_ok=True)
    path = hooks / "pre-push"
    path.write_text(HOOK, encoding="utf-8", newline="\n")
    path.chmod(0o755)
    return path
