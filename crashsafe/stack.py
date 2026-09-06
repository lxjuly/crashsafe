from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from types import FrameType
from typing import Optional

from crashsafe.config import Settings


class StackSupervisor:
    def __init__(self) -> None:
        settings = Settings.from_env()
        settings.state_dir.mkdir(parents=True, exist_ok=True)
        self.environment = os.environ.copy()
        self.environment.setdefault(
            "CRASHSAFE_WORKER_PID_FILE", str(settings.state_dir / "worker.pid")
        )
        self.processes: dict[str, subprocess.Popen[bytes]] = {}
        self.stopping = False

    def start(self) -> None:
        self.processes["tool"] = self._spawn("crashsafe.mock_tool")
        self.processes["api"] = self._spawn("crashsafe.api")
        self.processes["worker"] = self._spawn("crashsafe.worker")
        print("Crashsafe is running:", flush=True)
        print("  API/docs: http://127.0.0.1:8000/docs", flush=True)
        print("  mock ledger: http://127.0.0.1:8001/ledger", flush=True)
        print(f"  worker PID: {self.processes['worker'].pid}", flush=True)
        print("Press Ctrl-C to stop. A SIGKILLed worker is restarted.", flush=True)

    def _spawn(self, module: str) -> subprocess.Popen[bytes]:
        return subprocess.Popen([sys.executable, "-m", module], env=self.environment)

    def request_stop(self, signum: int, frame: Optional[FrameType]) -> None:
        del signum, frame
        self.stopping = True

    def monitor(self) -> None:
        while not self.stopping:
            for name, process in list(self.processes.items()):
                return_code = process.poll()
                if return_code is None:
                    continue
                if name == "worker":
                    print(f"worker exited ({return_code}); restarting", flush=True)
                    self.processes[name] = self._spawn("crashsafe.worker")
                    continue
                print(f"{name} exited unexpectedly ({return_code})", file=sys.stderr)
                self.stopping = True
            time.sleep(0.2)

    def stop(self) -> None:
        for process in self.processes.values():
            if process.poll() is None:
                process.terminate()
        deadline = time.monotonic() + 5.0
        for process in self.processes.values():
            remaining = deadline - time.monotonic()
            try:
                process.wait(timeout=max(remaining, 0.0))
            except subprocess.TimeoutExpired:
                process.kill()
        print("Crashsafe stopped.", flush=True)


def main() -> None:
    supervisor = StackSupervisor()
    signal.signal(signal.SIGINT, supervisor.request_stop)
    signal.signal(signal.SIGTERM, supervisor.request_stop)
    supervisor.start()
    try:
        supervisor.monitor()
    finally:
        supervisor.stop()


if __name__ == "__main__":
    main()
