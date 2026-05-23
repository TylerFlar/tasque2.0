from __future__ import annotations

from pathlib import Path


def read_template_file(path_value: str | Path, *, base_dir: Path | None = None) -> str:
    """Read a UTF-8 work template from disk, resolving relative paths from base_dir."""
    path = Path(path_value).expanduser()
    if not path.is_absolute():
        path = (base_dir or Path.cwd()) / path
    path = path.resolve()
    if not path.exists():
        raise ValueError(f"Template file does not exist: {path}")
    if not path.is_file():
        raise ValueError(f"Template path is not a file: {path}")
    return path.read_text(encoding="utf-8").strip()
