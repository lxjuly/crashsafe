#!/usr/bin/env python3
# ruff: noqa: E501
"""Browser console for recording the required live Crashsafe demonstration."""

from __future__ import annotations

import contextlib
import json
import os
import re
import shlex
import shutil
import signal
import subprocess
import sys
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Optional

import httpx

ROOT = Path(__file__).resolve().parents[1]
STATE = ROOT / ".crashsafe" / "video-live"
API_URL = "http://127.0.0.1:8020"
TOOL_URL = "http://127.0.0.1:8021"
CONSOLE_PORT = 8099

PAGE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Crashsafe — Live Failure Demo</title>
<style>
  :root { color-scheme: dark; }
  body { margin: 0; background: #090d12; color: #d8e1ea; font: 20px/1.45 ui-monospace, SFMono-Regular, Menlo, monospace; }
  main { max-width: 1120px; margin: 28px auto; padding: 0 28px; }
  h1 { margin: 0 0 6px; color: #72e5a6; font-size: 34px; }
  .claim { color: #9db0c4; margin-bottom: 20px; }
  #screen { min-height: 590px; max-height: 680px; overflow-y: auto; white-space: pre-wrap; background: #0e151d; border: 1px solid #293747; border-radius: 10px; padding: 22px; box-shadow: 0 12px 45px #0008; }
  .prompt { display: flex; align-items: center; margin-top: 16px; background: #111a23; border: 1px solid #33465a; border-radius: 8px; padding: 12px 16px; }
  .symbol { color: #72e5a6; margin-right: 12px; }
  input { flex: 1; color: #fff; background: transparent; border: 0; outline: 0; font: inherit; }
  .hint { margin-top: 10px; color: #7f94a8; font-size: 16px; }
</style>
</head>
<body><main>
  <h1>Crashsafe — live crash recovery</h1>
  <div class="claim">One worker · real SIGKILL · durable history · one charge effect</div>
  <div id="screen">Ready. This console controls real local Crashsafe processes.\n\nNext: make run</div>
  <form id="form" class="prompt"><span class="symbol">crashsafe $</span><input id="command" autocomplete="off" autofocus></form>
  <div class="hint">Commands: make run · curl -X POST /workflows · wait-for-charge · kill -9 &lt;pid&gt; · show recovery · architecture decision</div>
</main>
<script>
const form = document.getElementById('form');
const input = document.getElementById('command');
const screen = document.getElementById('screen');
async function pause(milliseconds) {
  await fetch(`/pause/${milliseconds}`, {method: 'POST'});
}
async function execute(command) {
  screen.textContent += `\n\ncrashsafe $ ${command}\n`;
  screen.scrollTop = screen.scrollHeight;
  const response = await fetch('/command', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({command})});
  const result = await response.json();
  screen.textContent += result.output;
  screen.scrollTop = screen.scrollHeight;
  return result.output;
}
form.addEventListener('submit', async (event) => {
  event.preventDefault();
  const command = input.value.trim();
  if (!command) return;
  input.value = '';
  await execute(command);
  input.focus();
});
async function autoDemo() {
  screen.textContent = 'Recording ready. Starting the live demo in 4 seconds...';
  await pause(4000);
  await execute('make run');
  await pause(1400);
  await execute('curl -X POST /workflows');
  await pause(1400);
  const committed = await execute('wait-for-charge');
  await pause(1800);
  const match = committed.match(/Next: kill -9 (\\d+)/);
  if (!match) throw new Error('worker PID was not reported');
  await execute(`kill -9 ${match[1]}`);
  await pause(1800);
  await execute('show recovery');
  await pause(2200);
  await execute('architecture decision');
}
if (location.pathname === '/auto') autoDemo();
</script>
</body></html>
"""


class LiveDemo:
    def __init__(self) -> None:
        self.stack: Optional[subprocess.Popen[bytes]] = None
        self.workflow_id: Optional[str] = None
        self.worker_pid: Optional[int] = None

    def execute(self, command: str) -> str:
        try:
            if command == "make run":
                return self.start()
            if command == "curl -X POST /workflows":
                return self.create_workflow()
            if command == "wait-for-charge":
                return self.wait_for_charge()
            if command.startswith("kill -9 "):
                return self.kill_worker(command)
            if command == "show recovery":
                return self.show_recovery()
            if command == "architecture decision":
                return self.architecture()
            return "Unknown command. Use one of the commands shown below the console."
        except Exception as exc:
            return f"ERROR: {exc}"

    def start(self) -> str:
        if self.stack is not None and self.stack.poll() is None:
            return "Crashsafe is already running."
        if STATE.exists():
            shutil.rmtree(STATE)
        STATE.mkdir(parents=True)
        environment = os.environ.copy()
        environment.update(
            {
                "CRASHSAFE_STATE_DIR": str(STATE),
                "CRASHSAFE_API_PORT": "8020",
                "CRASHSAFE_TOOL_PORT": "8021",
                "CRASHSAFE_TOOL_URL": TOOL_URL,
                "CRASHSAFE_FLAKY_RATE": "0",
                "CRASHSAFE_DELAY_AFTER_TOOL_COMMIT": "charge",
                "CRASHSAFE_COMMIT_SIGNAL_FILE": str(STATE / "charge-committed.signal"),
                "CRASHSAFE_COMMIT_DELAY": "300",
                "CRASHSAFE_REQUEST_TIMEOUT": "600",
                "CRASHSAFE_WORKER_PID_FILE": str(STATE / "worker.pid"),
                "LOG_LEVEL": "WARNING",
            }
        )
        self.stack = subprocess.Popen(
            [sys.executable, "-m", "crashsafe.stack"],
            cwd=ROOT,
            env=environment,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self._wait_http(f"{API_URL}/healthz")
        self._wait_http(f"{TOOL_URL}/healthz")
        self.worker_pid = self._read_worker_pid()
        return (
            "Started workflow API, independent mock tool, and ONE worker.\n"
            f"worker pid: {self.worker_pid}\n"
            "engine db: .crashsafe/video-live/engine.db\n"
            "tool db:   .crashsafe/video-live/tools.db\n\n"
            "Next: curl -X POST /workflows"
        )

    def create_workflow(self) -> str:
        response = httpx.post(
            f"{API_URL}/workflows",
            json={
                "customer_id": "video-customer",
                "amount_cents": 4200,
                "email": "video@example.com",
            },
            timeout=2,
        )
        response.raise_for_status()
        workflow: dict[str, Any] = response.json()
        self.workflow_id = str(workflow["id"])
        step = workflow["steps"][0]
        return (
            "HTTP 201 Created\n"
            f"workflow: {self.workflow_id}\n"
            "steps: charge -> provision -> notify\n"
            f"stable charge key: {step['operation_key']}\n\n"
            "The worker is now calling charge.\n"
            "Next: wait-for-charge"
        )

    def wait_for_charge(self) -> str:
        signal_file = STATE / "charge-committed.signal"
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and not signal_file.exists():
            time.sleep(0.05)
        if not signal_file.exists():
            raise RuntimeError("charge did not commit")
        self.worker_pid = self._read_worker_pid()
        ledger = httpx.get(f"{TOOL_URL}/ledger", timeout=2).json()
        return (
            "MOCK TOOL TRANSACTION COMMITTED. Response is deliberately paused.\n"
            f"durable ledger: {json.dumps(ledger)}\n"
            "engine still has an unmatched StepAttemptStarted.\n\n"
            f"Next: kill -9 {self.worker_pid}"
        )

    def kill_worker(self, command: str) -> str:
        parts = shlex.split(command)
        if len(parts) != 3 or parts[:2] != ["kill", "-9"]:
            raise RuntimeError("expected: kill -9 <pid>")
        requested_pid = int(parts[2])
        current_pid = self._read_worker_pid()
        if requested_pid != current_pid:
            raise RuntimeError(f"current worker pid is {current_pid}")
        os.kill(current_pid, signal.SIGKILL)
        deadline = time.monotonic() + 5
        replacement_pid = current_pid
        while time.monotonic() < deadline:
            with contextlib.suppress(OSError, ValueError):
                replacement_pid = self._read_worker_pid()
            if replacement_pid != current_pid:
                break
            time.sleep(0.05)
        if replacement_pid == current_pid:
            raise RuntimeError("supervisor did not replace the worker")
        self.worker_pid = replacement_pid
        return (
            f"worker {current_pid} exited from SIGKILL.\n"
            f"supervisor started replacement worker {replacement_pid}.\n"
            "No in-memory worker state was preserved.\n\n"
            "Next: show recovery"
        )

    def show_recovery(self) -> str:
        if self.workflow_id is None:
            raise RuntimeError("create a workflow first")
        deadline = time.monotonic() + 15
        workflow: dict[str, Any] = {}
        while time.monotonic() < deadline:
            workflow = httpx.get(
                f"{API_URL}/workflows/{self.workflow_id}", timeout=2
            ).json()
            if workflow["status"] == "completed":
                break
            time.sleep(0.05)
        if workflow.get("status") != "completed":
            raise RuntimeError("workflow did not complete after restart")
        events: list[dict[str, Any]] = httpx.get(
            f"{API_URL}/workflows/{self.workflow_id}/events", timeout=2
        ).json()
        audit = httpx.get(
            f"{API_URL}/workflows/{self.workflow_id}/audit", timeout=2
        ).json()
        ledger = httpx.get(f"{TOOL_URL}/ledger", timeout=2).json()
        charge_id = workflow["steps"][0]["id"]
        attempts = [
            {
                "sequence": event["sequence"],
                "attempt": event["attempt"],
                "key": event["payload"]["operation_key"],
            }
            for event in events
            if event["event_type"] == "StepAttemptStarted" and event["step_id"] == charge_id
        ]
        keys = {attempt["key"] for attempt in attempts}
        if len(attempts) < 2 or len(keys) != 1 or ledger.get("charges") != 1:
            raise RuntimeError(
                "expected at least two requests with one stable key and one charge"
            )
        return (
            f"workflow status: {workflow['status']}\n"
            f"charge attempt history: {json.dumps(attempts, indent=2)}\n"
            f"event -> projection audit consistent: {audit['consistent']}\n"
            f"durable ledger: {json.dumps(ledger)}\n\n"
            "PASS: two at-least-once requests, one charge side effect.\n\n"
            "Next: architecture decision"
        )

    @staticmethod
    def architecture() -> str:
        return (
            "MAJOR DECISION — separate durable intent from external effect\n\n"
            "1. engine.db appends StepAttemptStarted BEFORE network I/O.\n"
            "2. The event and scheduling projection commit atomically.\n"
            "3. The HTTP call happens outside that transaction.\n"
            "4. An unmatched attempt means OUTCOME UNKNOWN, so recovery retries.\n"
            "5. The stable key bridges that gap; tools.db atomically deduplicates.\n\n"
            "Guarantee: durable recovery + at-least-once requests.\n"
            "Exactly-once effects require the external tool's idempotency contract."
        )

    @staticmethod
    def _wait_http(url: str) -> None:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            try:
                if httpx.get(url, timeout=0.2).is_success:
                    return
            except httpx.RequestError:
                pass
            time.sleep(0.05)
        raise RuntimeError(f"timed out waiting for {url}")

    @staticmethod
    def _read_worker_pid() -> int:
        return int((STATE / "worker.pid").read_text(encoding="utf-8"))

    def stop(self) -> None:
        if self.stack is None or self.stack.poll() is not None:
            return
        self.stack.terminate()
        try:
            self.stack.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.stack.kill()
            self.stack.wait(timeout=5)


DEMO = LiveDemo()


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path == "/auto":
            self._stream_auto_demo()
            return
        if self.path not in {"/", "/manual"}:
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        body = PAGE.encode()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _stream_auto_demo(self) -> None:
        head = PAGE.split("<script>", maxsplit=1)[0].replace(
            "Ready. This console controls real local Crashsafe processes.\n\nNext: make run",
            "Recording ready. Starting the live demo in 4 seconds...",
        )
        head += "<script>const screen=document.getElementById('screen');</script>"
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(head.encode())
        self.wfile.flush()
        time.sleep(4)

        def show(command: str, output: str) -> None:
            addition = f"\n\ncrashsafe $ {command}\n{output}"
            script = (
                "<script>screen.textContent += "
                f"{json.dumps(addition)};"
                "screen.scrollTop=screen.scrollHeight;</script>"
            )
            self.wfile.write(script.encode())
            self.wfile.flush()

        show("make run", DEMO.execute("make run"))
        time.sleep(1.4)
        show("curl -X POST /workflows", DEMO.execute("curl -X POST /workflows"))
        time.sleep(1.4)
        committed = DEMO.execute("wait-for-charge")
        show("wait-for-charge", committed)
        time.sleep(1.8)
        match = re.search(r"Next: kill -9 (\d+)", committed)
        if match is None:
            raise RuntimeError("worker PID was not reported")
        kill_command = f"kill -9 {match.group(1)}"
        show(kill_command, DEMO.execute(kill_command))
        time.sleep(1.8)
        show("show recovery", DEMO.execute("show recovery"))
        time.sleep(2.2)
        show("architecture decision", DEMO.execute("architecture decision"))
        self.wfile.write(b"</body></html>")
        self.wfile.flush()

    def do_POST(self) -> None:
        if self.path.startswith("/pause/"):
            milliseconds = min(int(self.path.removeprefix("/pause/")), 5_000)
            time.sleep(milliseconds / 1_000)
            body = b"{}"
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if self.path != "/command":
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        length = int(self.headers.get("Content-Length", "0"))
        request = json.loads(self.rfile.read(length))
        body = json.dumps({"output": DEMO.execute(str(request["command"]))}).encode()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        del format, args


def main() -> None:
    server = ThreadingHTTPServer(("127.0.0.1", CONSOLE_PORT), Handler)
    print(f"video demo console: http://127.0.0.1:{CONSOLE_PORT}", flush=True)
    try:
        server.serve_forever()
    finally:
        DEMO.stop()
        server.server_close()


if __name__ == "__main__":
    main()
