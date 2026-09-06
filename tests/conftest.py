from __future__ import annotations

from pathlib import Path

import pytest

from crashsafe.config import Settings


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(
        state_dir=tmp_path,
        engine_db=tmp_path / "engine.db",
        tool_db=tmp_path / "tools.db",
        tool_base_url="http://127.0.0.1:1",
        poll_interval_seconds=0.01,
        request_timeout_seconds=1.0,
        max_attempts=4,
        base_backoff_seconds=0.01,
        max_backoff_seconds=0.1,
    )
