"""The Workshop's guard: a PreToolUse hook that keeps a Workshop run inside its own folder.

Workshop runs edit Tasque itself, and worker runs skip the CLI's permission prompts, so this hook is the
wall: Claude Code runs it before every tool call with the call as JSON on stdin, and an exit status of 2
refuses the call (the reason goes back to the model on stderr). It runs from the live checkout's path
(``python <live>/src/tasque2/workshop/guard.py``), so a run can never edit the guard that checks it, and it
imports only the standard library.

Refused, in every mode:
- writing a file outside the run's root (``--root``: the change's worktree folder, or a planning run's
  scratch folder);
- any command naming the live checkout or the live data directory, or ``.env``;
- git commands that move or publish branches (push, checkout, switch, reset, rebase, worktree, stash,
  branch deletion), the daemon's own commands, stopping processes, scheduled tasks, and installing
  packages;
- a command that climbs out of the folder with ``..`` (a relative path would hide where it writes);
- in a build: fetching from or searching the web (a planning run may research).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from typing import Any

WRITE_TOOLS = ("Write", "Edit", "MultiEdit", "NotebookEdit")
SHELL_TOOLS = ("Bash", "PowerShell")
WEB_TOOLS = ("WebFetch", "WebSearch")
DENIED_COMMANDS = (
    (r"\bgit\b[^\n|;&]*\b(push|checkout|switch|reset|rebase|worktree|stash)\b", "git that moves or publishes branches"),
    (r"\bgit\b[^\n|;&]*\bbranch\b[^\n|;&]*\s-(d|D|-delete)\b", "deleting a branch"),
    (
        # the tasque2 command itself (bare, by its path, quoted or not, or python -m), then the subcommand
        r"(?<![\w.-])tasque2(\.exe|\.cli)?(\.__main__)?[\"']?\s+[\"']?"
        r"(daemon|daemon-stop|daemon-restart|schedule-fire-now|release-apply)(?![\w-])",
        "the daemon's commands",
    ),
    (r"\b(taskkill|stop-process|kill|pkill|schtasks)\b", "stopping processes or scheduling tasks"),
    (r"\b(pip|pip3)\s+install\b|\buv\s+(add|remove|pip|sync)\b", "installing packages"),
    (r"(^|[\s'\"/\\])\.env\b", "the .env file"),
    (r"(^|[\s'\"=/\\])\.\.([/\\\s'\"]|$)", "climbing out of your folder with .."),
)


def _norm(path: str) -> str:
    text = str(path).replace("\\", "/").rstrip("/")
    return text.lower() if os.name == "nt" else text


def _inside(path: str, root: str) -> bool:
    candidate, base = _norm(os.path.abspath(path)), _norm(os.path.abspath(root))
    return candidate == base or candidate.startswith(base + "/")


def _names_path(command: str, path: str, *, allowed: list[str]) -> bool:
    """True when ``command`` names ``path`` (or anything under it) other than through an ``allowed`` path
    (the run's root, or an executable it may run, such as the live environment's Python)."""
    text, target = _norm(command), _norm(os.path.abspath(path))
    keeps = [_norm(os.path.abspath(item)) for item in allowed]
    for match in re.finditer(re.escape(target), text):
        tail = text[match.end() : match.end() + 1]
        if tail not in ("", "/", '"', "'", " ", "\n", ";", "&", "|", ")"):
            continue  # a longer name that only starts the same (tasque2.0-workshop)
        if any(keep.startswith(target) and text[match.start() :].startswith(keep) for keep in keeps):
            continue
        return True
    return False


def decide(
    call: dict[str, Any], *, root: str, live: str, data: str, mode: str, allow: list[str] | None = None
) -> str | None:
    """Why the call is refused, or None."""
    tool = str(call.get("tool_name") or "")
    args = call.get("tool_input") or {}
    if tool in WRITE_TOOLS:
        target = str(args.get("file_path") or args.get("notebook_path") or "")
        if not target or not _inside(target, root):
            return f"writes stay inside {root}"
    if tool in SHELL_TOOLS:
        command = str(args.get("command") or "")
        for place in (live, data):
            if _names_path(command, place, allowed=[root, *(allow or [])]):
                return "the live checkout and its data are off limits; work in your own folder"
        for pattern, what in DENIED_COMMANDS:
            if re.search(pattern, command, flags=re.IGNORECASE):
                return f"{what} is not allowed in a Workshop run"
    if tool in WEB_TOOLS and mode == "build":
        return "a build works from what is in its folder; research belongs in the plan"
    return None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True)
    parser.add_argument("--live", required=True)
    parser.add_argument("--data", required=True)
    parser.add_argument("--mode", choices=("build", "plan"), default="build")
    parser.add_argument("--allow", action="append", default=[], help="an executable the run may name")
    args = parser.parse_args(argv)
    try:
        call = json.loads(sys.stdin.read() or "{}")
    except ValueError:
        print("the guard could not read the tool call", file=sys.stderr)
        return 2
    reason = decide(call, root=args.root, live=args.live, data=args.data, mode=args.mode, allow=args.allow)
    if reason:
        print(f"Refused by the Workshop guard: {reason}.", file=sys.stderr)
        return 2
    return 0


def hook_settings(
    *, python: str, guard: str, root: str, live: str, data: str, mode: str, allow: list[str] | None = None
) -> dict[str, Any]:
    """The ``--settings`` entry that puts this guard in front of every tool call."""
    parts = [python, guard, "--root", root, "--live", live, "--data", data, "--mode", mode]
    for item in allow or []:
        parts += ["--allow", item]
    command = " ".join(f'"{part}"' for part in parts)
    return {"hooks": {"PreToolUse": [{"matcher": "*", "hooks": [{"type": "command", "command": command}]}]}}


if __name__ == "__main__":
    raise SystemExit(main())
