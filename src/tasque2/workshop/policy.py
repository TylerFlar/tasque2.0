"""What a change needs before it goes live: a tier, from who started it and what it touches.

The owner's rules (2026-10-09): one set of rules for everything, as malleable as possible.

- ``auto`` ships on its own (a report with an undo; at most ``AUTO_PER_DAY`` a day): a fix that comes
  with a reproducing test, and the owner's own ask that touches only private code, templates or
  doctrine and raises no questions;
- ``tap`` waits for one tap on a card: what the Workshop proposes on its own that changes behaviour
  (rather than fixing it), anything in the public core repository (it is pushed), migrations and new
  dependencies;
- ``plan`` needs the plan approved, then a tap: redesigns and multi-part changes, a new or retired lane,
  anything touching the owner's goals or standing rules, any request with open questions, anything a
  model filed rather than the owner, and the Workshop's own checks and machinery.

The tier is computed here, never taken from a model, and it is checked again against the real diff after
the build: it can only go up.
"""

from __future__ import annotations

AUTO, TAP, PLAN = "auto", "tap", "plan"
ORDER = {AUTO: 0, TAP: 1, PLAN: 2}
AUTO_PER_DAY = 3
ORIGINS = ("user", "fault", "workshop", "model")
KINDS = ("fix", "tweak", "feature", "redesign")
# The Workshop's own machinery: a change to any of it is a plan, whoever asks.
SELF = (
    "src/tasque2/workshop/",
    "src/tasque2/ops/privacy.py",
    "src/tasque2/ops/repair.py",
    "src/tasque2/ops/release.py",
    "src/tasque2/ops/rehearse.py",
    "src/tasque2/ops/worktree.py",
    "src/tasque2/ops/datarepo.py",
    "src/tasque2/daemon/respawn.py",
    "src/tasque2/daemon/restart.py",
    "doctrine/global/tasque_workshop.md",
    "doctrine/global/tasque_design.md",
    "work-templates/workshop/",
)


def higher(*tiers: str) -> str:
    return max(tiers, key=lambda tier: ORDER.get(tier, ORDER[PLAN]))


def classify(*, origin: str, kind: str, touches: list[str] | set[str], questions: int = 0) -> tuple[str, str]:
    """The tier a planned change needs, and why."""
    touched = set(touches)
    if origin == "model":
        return PLAN, "a model filed it, not the owner"
    if questions:
        return PLAN, "it has open questions"
    if kind == "redesign":
        return PLAN, "a redesign"
    if touched & {"goals", "standing_rules", "lanes", "self"}:
        return PLAN, "it touches " + ", ".join(sorted(touched & {"goals", "standing_rules", "lanes", "self"}))
    if "core" in touched:
        return TAP, "it changes the public core repository"
    if touched & {"migrations", "dependencies"}:
        return TAP, "it changes " + ", ".join(sorted(touched & {"migrations", "dependencies"}))
    if kind == "fix" and origin in ("fault", "user", "workshop"):
        return AUTO, "a fix with a test that reproduces it"
    if origin == "user" and kind == "tweak":
        return AUTO, "the owner's own small ask"
    return TAP, "it changes how Tasque behaves"


def touches_from_diff(files: dict[str, list[str]]) -> set[str]:
    """What a built change really touches, from its files per repository ("core", an extension, "data")."""
    touched: set[str] = set()
    for repo, names in files.items():
        for name in names:
            path = name.replace("\\", "/")
            full = f"{'' if repo in ('core', 'data') else f'extensions/{repo}/'}{path}"
            if repo == "core":
                touched.add("core")
            elif repo == "data":
                touched.add("doctrine" if path.startswith("doctrine/") else "data")
                if path == "lanes.json":
                    touched.add("schedules")
            else:
                touched.add("extension")
            if "/migrations/" in f"/{path}" or path.startswith("alembic/"):
                touched.add("migrations")
            if path in ("pyproject.toml", "uv.lock"):
                touched.add("dependencies")
            if any(full.startswith(entry) or path.startswith(entry) for entry in SELF):
                touched.add("self")
    return touched


def final_tier(planned: str, *, origin: str, kind: str, files: dict[str, list[str]]) -> tuple[str, str]:
    """The tier after the build: the planned one, raised (never lowered) by what the diff really touches."""
    actual, why = classify(origin=origin, kind=kind, touches=touches_from_diff(files))
    tier = higher(planned, actual)
    return tier, why if tier == actual else "as planned"
