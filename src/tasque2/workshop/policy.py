"""What a change needs before it goes live, and whether the user hears of it: decided here, never by a run.

The owner's rules (2026-10-10): the user sees only what is theirs to decide.

- ``auto`` ships on its own, with an undo: fixes, tweaks, the owner's own asks, core code, doctrine,
  migrations, internal tools. The Workshop's own changes (filed by it, not asked for) ship at most
  ``AUTO_PER_DAY`` a day; one more waits for the next day.
- ``plan`` shows a pitch first (Approve builds and ships it): open questions, a redesign, the owner's goals
  or standing rules, a lane added or retired, a new dependency, the Workshop's safety net (``SELF``), and
  something the Workshop thought up itself that would change what the owner sees or does (``routine``; a
  fix never counts). While the Workshop is paused, every change shows its pitch first.

One tap at the end is rare (``late_tap``): a build that touches what its plan did not name (the safety net,
a dependency), or a change built while the owner paused the Workshop.

The tier is computed here, never taken from a model, and it is checked again against the real diff after
the build: it can only go up.
"""

from __future__ import annotations

AUTO, PLAN = "auto", "plan"
ORDER = {AUTO: 0, PLAN: 1}
AUTO_PER_DAY = 3
ORIGINS = ("user", "fault", "workshop", "model")
# The Workshop's own changes: what it filed itself, or another run did, rather than the owner asking.
OWN_ORIGINS = ("workshop", "fault", "model")
KINDS = ("fix", "tweak", "feature", "redesign")
# What is the owner's call, whoever asks and whatever the kind, and how the card names it.
THEIRS = {
    "goals": "your goals",
    "standing_rules": "your standing rules",
    "lanes": "a lane added or retired",
    "dependencies": "a new package",
    "self": "the Workshop's safety net",
}
# The Workshop's safety net: what needs the owner's OK, the checks before a release, the guard on its runs,
# the privacy scan and its denylist, the rehearsal, and its own rule documents. A change to any of these
# shows its pitch first, whoever asks; a build that touches one unplanned waits for a tap.
SELF = (
    "src/tasque2/workshop/policy.py",
    "src/tasque2/workshop/verify.py",
    "src/tasque2/workshop/guard.py",
    "src/tasque2/ops/privacy.py",
    "src/tasque2/ops/rehearse.py",
    "privacy-denylist.txt",
    "doctrine/global/tasque_workshop.md",
    "doctrine/global/tasque_design.md",
)
# What a diff can show that a plan should have named.
UNPLANNABLE = ("self", "dependencies")


def higher(*tiers: str) -> str:
    return max(tiers, key=lambda tier: ORDER.get(tier, ORDER[PLAN]))


def own(origin: str) -> bool:
    """Whether a change is the Workshop's own rather than the owner's ask: held to ``AUTO_PER_DAY``, and
    shipped quietly unless its pitch was approved."""
    return origin in OWN_ORIGINS


def classify(
    *, origin: str, kind: str, touches: list[str] | set[str], questions: int = 0, paused: bool = False
) -> tuple[str, str]:
    """The tier a planned change needs, and why."""
    touched = set(touches)
    if paused:
        return PLAN, "the Workshop is paused"
    if questions:
        return PLAN, "it has open questions"
    if kind == "redesign":
        return PLAN, "a redesign"
    theirs = [THEIRS[name] for name in THEIRS if name in touched]
    if theirs:
        return PLAN, "it touches " + ", ".join(theirs)
    if own(origin) and kind != "fix" and "routine" in touched:
        return PLAN, "the Workshop's own idea, and it would change your day"
    if kind == "fix":
        return AUTO, "a fix"
    if origin == "user":
        return AUTO, "your own ask"
    return AUTO, "nothing in it is your call"


def touches_from_diff(files: dict[str, list[str]]) -> set[str]:
    """What a built change really touches, from its files per repository ("core", an extension, "data")."""
    touched: set[str] = set()
    for repo, names in files.items():
        for name in names:
            path = name.replace("\\", "/")
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
            if repo in ("core", "data") and path in SELF:
                touched.add("self")
    return touched


def final_tier(planned: str, *, origin: str, kind: str, files: dict[str, list[str]]) -> tuple[str, str]:
    """The tier after the build: the planned one, raised (never lowered) by what the diff really touches."""
    actual, why = classify(origin=origin, kind=kind, touches=touches_from_diff(files))
    tier = higher(planned, actual)
    return tier, why if tier == actual else "as planned"


def late_tap(
    planned_touches: list[str] | set[str], files: dict[str, list[str]], *, paused_since_plan: bool
) -> str | None:
    """Why a built change needs one tap after all, or None: its diff touches what its plan did not name, or
    the owner paused the Workshop while it was built."""
    unnamed = [
        THEIRS[name] for name in UNPLANNABLE if name in touches_from_diff(files) and name not in set(planned_touches)
    ]
    if unnamed:
        return "its plan did not say it touches " + ", ".join(unnamed)
    if paused_since_plan:
        return "you paused the Workshop while it was built"
    return None
