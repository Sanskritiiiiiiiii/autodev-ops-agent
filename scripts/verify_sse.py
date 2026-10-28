"""Module 6B verification: SSE endpoint, SSE client, Rich renderer."""
import os
import asyncio
import http.server
import io
import json
import socket
import sys
import threading
import urllib.error
import urllib.request
import uuid
from pathlib import Path

import redis.asyncio as aioredis
import uvicorn

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
os.environ.pop("AUTODEV_API_KEY", None)  # the in-process test server runs unauthenticated

from rich.console import Console  # noqa: E402

import app.api.stream as stream_module  # noqa: E402
from app.api.main import app as fastapi_app  # noqa: E402
from app.cli.render import EventRenderer, exit_code_for  # noqa: E402
from app.cli.sse_client import iter_sse, stream_job  # noqa: E402
from app.config import REDIS_HOST, REDIS_PORT  # noqa: E402
from app.queue.events import events_key, publish_event  # noqa: E402
from app.queue.jobs import enqueue_job, job_key  # noqa: E402

failures: list[str] = []
TEST_STREAM = f"autodev:test:{uuid.uuid4().hex[:8]}:unused"


def check(name: str, cond: bool, detail: str = "") -> None:
    print(f"[{'PASS' if cond else 'FAIL'}] {name}" + (f"  {detail}" if detail and not cond else ""))
    if not cond:
        failures.append(name)


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def collect(url: str, last_event_id: str | None = None) -> list:
    return list(iter_sse(url, last_event_id, 20.0))


def status_of(url: str, headers: dict | None = None) -> int:
    req = urllib.request.Request(url, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status
    except urllib.error.HTTPError as e:
        return e.code


def content_type_of(url: str) -> str:
    with urllib.request.urlopen(url, timeout=15) as resp:
        ctype = resp.headers.get_content_type()
        resp.read()
        return ctype


# ---------------- fake SSE servers for client reconnect tests ----------------
class FlakyHandler(http.server.BaseHTTPRequestHandler):
    seen: list = []

    def do_GET(self):
        FlakyHandler.seen.append(self.headers.get("Last-Event-ID"))
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.end_headers()
        if len(FlakyHandler.seen) == 1:
            self.wfile.write(b'id: 1-0\nevent: step\ndata: {"node": "analyze_log"}\n\n')
        else:
            self.wfile.write(b': ping\r\n\r\nid: 2-0\r\nevent: job_completed\r\ndata: {"result": null}\r\n\r\n')

    def log_message(self, *args):
        pass


class DeadHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.end_headers()

    def log_message(self, *args):
        pass


def start_fake(handler_cls):
    port = free_port()
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", port), handler_cls)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, f"http://127.0.0.1:{port}"


async def main() -> None:
    r = aioredis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    job_ids: list[str] = []

    async def new_job() -> str:
        jid = await enqueue_job(r, "sample_repo", "log", stream_key=TEST_STREAM)
        job_ids.append(jid)
        return jid

    port = free_port()
    server = uvicorn.Server(uvicorn.Config(fastapi_app, host="127.0.0.1", port=port, log_level="error"))
    server_task = asyncio.create_task(server.serve())
    saved = (stream_module.BLOCK_MS, stream_module.IDLE_TIMEOUT_S)
    try:
        for _ in range(100):
            if server.started:
                break
            await asyncio.sleep(0.1)
        check("API server started in-process", server.started)
        base = f"http://127.0.0.1:{port}"
        url = lambda jid: f"{base}/api/v1/triage/stream/{jid}"  # noqa: E731

        # ---------- replay ----------
        j1 = await new_job()
        await publish_event(r, j1, "job_started", {"attempt": 1})
        await publish_event(r, j1, "step", {"node": "analyze_log", "hypothesis": "h", "search_query": "q"})
        await publish_event(r, j1, "job_completed", {"result": {"evaluation_status": "passed", "attempts": 1}})
        ids = [i for i, _ in await r.xrange(events_key(j1))]
        evs = await asyncio.to_thread(collect, url(j1))
        check("replay: all events arrive in order, stream then closes",
              [e.event for e in evs] == ["job_started", "step", "job_completed"], str([e.event for e in evs]))
        check("replay: SSE ids equal Redis stream ids", [e.id for e in evs] == ids)
        check("replay: data round-trips as JSON", json.loads(evs[1].data)["node"] == "analyze_log")
        check("replay: content type is text/event-stream",
              await asyncio.to_thread(content_type_of, url(j1)) == "text/event-stream")

        # ---------- resume ----------
        evs = await asyncio.to_thread(collect, url(j1), ids[0])
        check("resume: only events after Last-Event-ID",
              [e.event for e in evs] == ["step", "job_completed"], str([e.event for e in evs]))

        # ---------- live delivery (connect before any event exists) ----------
        j2 = await new_job()
        collector = asyncio.create_task(asyncio.to_thread(collect, url(j2)))
        await asyncio.sleep(0.7)
        await publish_event(r, j2, "job_started", {"attempt": 1})
        await asyncio.sleep(0.2)
        await publish_event(r, j2, "step", {"node": "retrieve_code", "chunks": []})
        await asyncio.sleep(0.2)
        check("live: stream is still open before the terminal event", not collector.done())
        await publish_event(r, j2, "job_completed", {"result": None})
        evs = await asyncio.wait_for(collector, 15)
        check("live: events delivered in order, ending at the terminal event",
              [e.event for e in evs] == ["job_started", "step", "job_completed"], str([e.event for e in evs]))

        # ---------- failed job closes the stream too ----------
        j3 = await new_job()
        await publish_event(r, j3, "job_started", {"attempt": 1})
        await publish_event(r, j3, "job_failed", {"error": "boom"})
        evs = await asyncio.to_thread(collect, url(j3))
        check("job_failed is terminal and closes the stream",
              [e.event for e in evs] == ["job_started", "job_failed"], str([e.event for e in evs]))

        # ---------- error responses ----------
        check("unknown job -> 404", await asyncio.to_thread(status_of, url(uuid.uuid4().hex)) == 404)
        check("malformed job id -> 404", await asyncio.to_thread(status_of, url("not-a-job-id")) == 404)
        check("bad Last-Event-ID -> 400",
              await asyncio.to_thread(status_of, url(j1), {"Last-Event-ID": "abc"}) == 400)

        # ---------- idle timeout ----------
        stream_module.BLOCK_MS, stream_module.IDLE_TIMEOUT_S = 200, 1.0
        j4 = await new_job()
        await publish_event(r, j4, "job_started", {"attempt": 1})
        evs = await asyncio.to_thread(collect, url(j4))
        check("idle job ends with a timeout event",
              [e.event for e in evs] == ["job_started", "timeout"], str([e.event for e in evs]))
        stream_module.BLOCK_MS, stream_module.IDLE_TIMEOUT_S = saved

        # ---------- client: HTTP errors are not retried ----------
        try:
            await asyncio.to_thread(lambda: list(stream_job(base, uuid.uuid4().hex, retry_delay=0.01)))
            check("client: 404 raises HTTPError immediately", False)
        except urllib.error.HTTPError as e:
            check("client: 404 raises HTTPError immediately", e.code == 404)

        # ---------- client: reconnect with Last-Event-ID ----------
        srv, fake = start_fake(FlakyHandler)
        evs = await asyncio.to_thread(lambda: list(stream_job(fake, "x" * 32, retry_delay=0.05)))
        srv.shutdown()
        check("client: reconnects after a dropped stream and finishes",
              [e.event for e in evs] == ["step", "job_completed"], str([e.event for e in evs]))
        check("client: second request carried Last-Event-ID 1-0",
              FlakyHandler.seen == [None, "1-0"], str(FlakyHandler.seen))

        srv, fake = start_fake(DeadHandler)
        try:
            await asyncio.to_thread(
                lambda: list(stream_job(fake, "x" * 32, max_reconnects=2, retry_delay=0.01)))
            check("client: gives up after the reconnect limit", False)
        except ConnectionError:
            check("client: gives up after the reconnect limit", True)
        srv.shutdown()
    finally:
        stream_module.BLOCK_MS, stream_module.IDLE_TIMEOUT_S = saved
        server.should_exit = True
        try:
            await asyncio.wait_for(server_task, timeout=10)
        except asyncio.TimeoutError:
            pass
        await r.delete(TEST_STREAM, *[job_key(j) for j in job_ids], *[events_key(j) for j in job_ids])
        await r.aclose()


asyncio.run(main())

# ---------------- renderer (no network) ----------------
buf = io.StringIO()
renderer = EventRenderer(Console(file=buf, width=160, force_terminal=False, color_system=None))
SEQUENCE = [
    ("job_started", {"attempt": 1}),
    ("step", {"node": "analyze_log", "hypothesis": "Pool exhausted [ERR_DB_TIMEOUT].",
              "search_query": "ERR_DB_TIMEOUT acquire"}),
    ("step", {"node": "retrieve_code", "chunks": [
        {"qualified_name": "ConnectionPool.acquire", "chunk_type": "method",
         "file_path": "shop/db.py", "start_line": 19, "end_line": 26}]}),
    ("step", {"node": "draft_fix", "attempt": 1, "hypothesis": "h", "chunk_id": "shop/db.py::ConnectionPool.acquire@19",
              "new_code": "def acquire(self, timeout=5.0)\n    return self"}),
    ("step", {"node": "verify", "evaluation_status": "failed", "feedback": "SyntaxError: expected ':' (line 1)"}),
    ("step", {"node": "draft_fix", "attempt": 2, "hypothesis": "h", "chunk_id": "shop/db.py::ConnectionPool.acquire@19",
              "new_code": "def acquire(self, timeout=5.0):\n    return self"}),
    ("step", {"node": "verify", "evaluation_status": "passed", "feedback": None}),
    ("step", {"node": "output_diff", "diff": "--- a/shop/db.py\n+++ b/shop/db.py\n@@ -19,4 +19,4 @@\n"
                                              "-    def acquire(self, timeout=1.0):\n+    def acquire(self, timeout=5.0):"}),
    ("job_completed", {"result": {"evaluation_status": "passed", "attempts": 2, "verification_feedback": None}}),
]
for etype, edata in SEQUENCE:
    renderer.render(etype, edata)
text = buf.getvalue()
for needle, label in [
    ("Pool exhausted [ERR_DB_TIMEOUT].", "log text with square brackets renders safely"),
    ("ERR_DB_TIMEOUT acquire", "search query shown"),
    ("ConnectionPool.acquire", "retrieved chunk shown"),
    ("shop/db.py:19-26", "chunk location shown"),
    ("Draft fix (attempt 2)", "second draft shown"),
    ("Verification failed: SyntaxError", "failed verification with feedback shown"),
    ("Verification passed", "passed verification shown"),
    ("@@ -19,4 +19,4 @@", "diff hunk shown"),
    ("Verified fix found in 2 attempt(s)", "final result shown"),
]:
    check(f"renderer: {label}", needle in text)

buf2 = io.StringIO()
EventRenderer(Console(file=buf2, width=160, force_terminal=False, color_system=None)).render(
    "job_failed", {"error": "PermanentJobError(\"repo 'no_such_repo' has no indexed chunks\")"})
check("renderer: job_failed panel shows the error", "no_such_repo" in buf2.getvalue())

check("exit code: passed -> 0", exit_code_for("job_completed", {"result": {"evaluation_status": "passed"}}) == 0)
check("exit code: no fix -> 1", exit_code_for("job_completed", {"result": {"evaluation_status": "failed"}}) == 1)
check("exit code: job_failed -> 3", exit_code_for("job_failed", {"error": "x"}) == 3)
check("exit code: timeout -> 4", exit_code_for("timeout", {}) == 4)
check("exit code: non-terminal -> None", exit_code_for("step", {}) is None)

print("\nMODULE 6B: ALL CHECKS PASSED" if not failures else f"\nMODULE 6B: FAILED -> {failures}")
sys.exit(1 if failures else 0)