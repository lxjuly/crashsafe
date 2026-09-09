from __future__ import annotations

import argparse
import logging
import os
import signal
import threading
import time
from pathlib import Path
from types import FrameType
from typing import Optional

from crashsafe.config import Settings
from crashsafe.engine import HttpToolGateway, WorkflowEngine
from crashsafe.models import WorkflowRunStatus
from crashsafe.storage import SQLiteStorage, WorkflowRunNotFoundError

logger = logging.getLogger(__name__)


class Worker:
    def __init__(self, engine: WorkflowEngine, poll_interval_seconds: float) -> None:
        self.engine = engine
        self.poll_interval_seconds = poll_interval_seconds
        self._drain_requested = threading.Event()

    def request_stop(self, signum: int, frame: Optional[FrameType]) -> None:
        del frame
        logger.info("received signal %d; draining after the current attempt", signum)
        self._drain_requested.set()

    def run_forever(self) -> None:
        while not self._drain_requested.is_set():
            outcome = self.engine.run_once()
            if not outcome.did_work:
                self._drain_requested.wait(self.poll_interval_seconds)
        logger.info("drain complete; worker stopped accepting steps")

    def run_until_terminal(self, run_id: str, timeout_seconds: float) -> int:
        deadline = time.monotonic() + timeout_seconds
        while not self._drain_requested.is_set() and time.monotonic() < deadline:
            run = self.engine.storage.get_workflow_run(run_id)
            if run.status == WorkflowRunStatus.COMPLETED:
                return 0
            if run.status == WorkflowRunStatus.FAILED:
                return 2
            outcome = self.engine.run_once()
            if not outcome.did_work:
                self._drain_requested.wait(self.poll_interval_seconds)
        return 3


def build_worker() -> Worker:
    settings = Settings.from_env()
    storage = SQLiteStorage(settings.engine_db)
    gateway = HttpToolGateway(settings.tool_base_url, settings.request_timeout_seconds)
    return Worker(
        WorkflowEngine(
            storage=storage,
            gateway=gateway,
            settings=settings,
            worker_id=os.getenv("CRASHSAFE_WORKER_ID"),
        ),
        settings.poll_interval_seconds,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Crashsafe workflow worker")
    parser.add_argument("--once", action="store_true", help="run at most one eligible step")
    parser.add_argument("--until-terminal", metavar="RUN_ID")
    parser.add_argument("--timeout", type=float, default=30.0)
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    worker = build_worker()
    signal.signal(signal.SIGTERM, worker.request_stop)
    signal.signal(signal.SIGINT, worker.request_stop)
    pid_file = os.getenv("CRASHSAFE_WORKER_PID_FILE")
    if pid_file:
        path = Path(pid_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(str(os.getpid()), encoding="utf-8")
    args = parse_args()
    try:
        if args.once:
            worker.engine.run_once()
            return
        if args.until_terminal:
            try:
                raise SystemExit(worker.run_until_terminal(args.until_terminal, args.timeout))
            except WorkflowRunNotFoundError:
                logger.error("workflow run %s does not exist", args.until_terminal)
                raise SystemExit(4) from None
        worker.run_forever()
    finally:
        if pid_file:
            Path(pid_file).unlink(missing_ok=True)


if __name__ == "__main__":
    main()
