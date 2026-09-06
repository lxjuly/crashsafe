from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

DEFAULT_API_HOST = "127.0.0.1"
DEFAULT_API_PORT = 8000
DEFAULT_TOOL_PORT = 8001
DEFAULT_POLL_INTERVAL_SECONDS = 0.2
DEFAULT_REQUEST_TIMEOUT_SECONDS = 10.0
DEFAULT_MAX_ATTEMPTS = 8
DEFAULT_BASE_BACKOFF_SECONDS = 0.25
DEFAULT_MAX_BACKOFF_SECONDS = 10.0
DEFAULT_RETRY_AFTER_SECONDS = 1.0
DEFAULT_FLAKY_RATE = 0.35
DEBUG_COMMIT_DELAY_SECONDS = 30.0


@dataclass(frozen=True)
class Settings:
    state_dir: Path
    engine_db: Path
    tool_db: Path
    tool_base_url: str
    poll_interval_seconds: float
    request_timeout_seconds: float
    max_attempts: int
    base_backoff_seconds: float
    max_backoff_seconds: float

    @classmethod
    def from_env(cls) -> Settings:
        state_dir = Path(os.getenv("CRASHSAFE_STATE_DIR", ".crashsafe")).resolve()
        tool_port = int(os.getenv("CRASHSAFE_TOOL_PORT", str(DEFAULT_TOOL_PORT)))
        return cls(
            state_dir=state_dir,
            engine_db=Path(os.getenv("CRASHSAFE_ENGINE_DB", state_dir / "engine.db")),
            tool_db=Path(os.getenv("CRASHSAFE_TOOL_DB", state_dir / "tools.db")),
            tool_base_url=os.getenv("CRASHSAFE_TOOL_URL", f"http://{DEFAULT_API_HOST}:{tool_port}"),
            poll_interval_seconds=float(
                os.getenv("CRASHSAFE_POLL_INTERVAL", str(DEFAULT_POLL_INTERVAL_SECONDS))
            ),
            request_timeout_seconds=float(
                os.getenv("CRASHSAFE_REQUEST_TIMEOUT", str(DEFAULT_REQUEST_TIMEOUT_SECONDS))
            ),
            max_attempts=int(os.getenv("CRASHSAFE_MAX_ATTEMPTS", str(DEFAULT_MAX_ATTEMPTS))),
            base_backoff_seconds=float(
                os.getenv("CRASHSAFE_BASE_BACKOFF", str(DEFAULT_BASE_BACKOFF_SECONDS))
            ),
            max_backoff_seconds=float(
                os.getenv("CRASHSAFE_MAX_BACKOFF", str(DEFAULT_MAX_BACKOFF_SECONDS))
            ),
        )
