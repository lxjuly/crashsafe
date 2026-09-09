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
DEFAULT_WORKER_COUNT = 1
DEFAULT_LEASE_TTL_SECONDS = 2.0
DEFAULT_LEASE_RENEW_INTERVAL_SECONDS = 0.5
DEFAULT_TOOL_KEY = "mock-tool"


@dataclass(frozen=True)
class Settings:
    state_dir: Path
    engine_db: Path
    ledger_db: Path
    tool_base_url: str
    poll_interval_seconds: float
    request_timeout_seconds: float
    max_attempts: int
    base_backoff_seconds: float
    max_backoff_seconds: float
    worker_count: int = DEFAULT_WORKER_COUNT
    lease_ttl_seconds: float = DEFAULT_LEASE_TTL_SECONDS
    lease_renew_interval_seconds: float = DEFAULT_LEASE_RENEW_INTERVAL_SECONDS
    tool_key: str = DEFAULT_TOOL_KEY

    @classmethod
    def from_env(cls) -> Settings:
        state_dir = Path(os.getenv("CRASHSAFE_STATE_DIR", ".crashsafe")).resolve()
        ledger_path = os.getenv("CRASHSAFE_LEDGER_DB") or os.getenv("CRASHSAFE_TOOL_DB")
        tool_port = int(os.getenv("CRASHSAFE_TOOL_PORT", str(DEFAULT_TOOL_PORT)))
        worker_count = int(os.getenv("CRASHSAFE_WORKERS", str(DEFAULT_WORKER_COUNT)))
        lease_ttl = float(os.getenv("CRASHSAFE_LEASE_TTL", str(DEFAULT_LEASE_TTL_SECONDS)))
        renew_interval = float(
            os.getenv(
                "CRASHSAFE_LEASE_RENEW_INTERVAL",
                str(DEFAULT_LEASE_RENEW_INTERVAL_SECONDS),
            )
        )
        if worker_count < 1:
            raise ValueError("CRASHSAFE_WORKERS must be at least 1")
        if lease_ttl <= 0:
            raise ValueError("CRASHSAFE_LEASE_TTL must be positive")
        if renew_interval <= 0 or renew_interval >= lease_ttl:
            raise ValueError("lease renewal interval must be positive and less than the TTL")
        return cls(
            state_dir=state_dir,
            engine_db=Path(os.getenv("CRASHSAFE_ENGINE_DB", state_dir / "engine.db")),
            ledger_db=Path(ledger_path) if ledger_path else state_dir / "ledger.db",
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
            worker_count=worker_count,
            lease_ttl_seconds=lease_ttl,
            lease_renew_interval_seconds=renew_interval,
            tool_key=os.getenv("CRASHSAFE_TOOL_KEY", DEFAULT_TOOL_KEY),
        )
