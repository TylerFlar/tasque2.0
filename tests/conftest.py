from __future__ import annotations

from pathlib import Path

import pytest

from tasque2.db import create_schema, reset_engine


@pytest.fixture()
def fresh_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    db_path = tmp_path / "tasque2.sqlite3"
    monkeypatch.setenv("TASQUE2_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("TASQUE2_DB_PATH", str(db_path))
    reset_engine()
    create_schema()
    yield db_path
    reset_engine()
