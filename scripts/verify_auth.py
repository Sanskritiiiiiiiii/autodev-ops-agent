"""Module 8C verification: API-key authentication."""
import asyncio
import contextlib
import io
import json
import logging
import os
import socket
import sys
import urllib.error
import urllib.request
import uuid
from pathlib import Path

import redis.asyncio as aioredis
import uvicorn

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
os.environ.pop("AUTODEV_API_KEY", None)  # start from a known state
logging.basicConfig(level=logging.ERROR)

from app.api.auth import MIN_KEY_LENGTH, check_api_key_config  # noqa: E402
from app.api.main import app as fastapi_app  # noqa: E402
from app.cli.sse_client import stream_job, submit_job  # noqa: E402
from app.cli.triage_cli import main as cli_main  # noqa: E402
from app.config import REDIS_HOST, REDIS_PORT  # noqa: E402
from app.queue.events import events_key, publish_event  # noqa: E402
from app.queue.jobs import enqueue_job, job_key  # noqa: E402

KEY = "k" * 8 + "Zq9-test-api-key-" + "x" * 8   # long enough, unmistakable
MARK = f"auth-test-{uuid.uuid4().hex}"
REPO = "auth_test_unindexed"                   # never indexed: no model or LLM call can happen
TEST_STREAM = f"autodev:test:{uuid.uuid4().hex[:8]}:unused"
failures: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    print(f"[{'PASS' if cond else 'FAIL'}] {name}" + (f"  {detail}" if detail and not cond else ""))
    if not cond:
        failures.append(name)


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def call(method: str, url: str, key: str | None = None, body: dict | None = None) -> tuple[int, str]:
    headers, data = {}, None
    if key is not None:
        headers["X-API-Key"] = key
    if body is not None:
        data, headers["Content-Type"] = json.dumps(body).encode(), "application/json"
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.status, resp.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "replace")


# ---------- startup validation (pure) ----------
check("config: unset -> auth disabled", check_api_key_config({}) is False)
check("config: whitespace only -> auth disabled", check_api_key_config({"AUTODEV_API_KEY": "   "}) is False)
check("config: sufficiently long key -> auth enabled",
      check_api_key_config({"AUTODEV_API_KEY": "k" * MIN_KEY_LENGTH}) is True)
try:
    check_api_key_config({"AUTODEV_API_KEY": "short"})
    check("config: too-short key refuses to start", False)
except RuntimeError as e:
    check("config: too-short key refuses to start", str(MIN_KEY_LENGTH) in str(e))


async def main() -> None:
    r = aioredis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    port = free_port()
    server = uvicorn.Server(uvicorn.Config(fastapi_app, host="127.0.0.1", port=port, log_level="error"))
    server_task = asyncio.create_task(server.serve())
    job_ids: list[str] = []

    async def find_jobs() -> list[str]:
        """Job ids whose payload carries this test's marker (works even if a worker consumed them)."""
        found = []
        async for key in r.scan_iter(match="autodev:job:*", count=200):
            if key.endswith(":events"):
                continue
            if await r.type(key) == "hash" and await r.hget(key, "log") == MARK:
                found.append(key.rsplit(":", 1)[1])
        return found

    async def cleanup_real(ids: list[str]) -> None:
        for jid in ids:
            async for entry_id, fields in _stream_entries():
                if fields.get("job_id") == jid:
                    await r.xdel("autodev:triage:jobs", entry_id)
            await r.delete(job_key(jid), events_key(jid))

    async def _stream_entries():
        for entry in await r.xrange("autodev:triage:jobs"):
            yield entry

    try:
        for _ in range(100):
            if server.started:
                break
            await asyncio.sleep(0.1)
        check("API server started in-process", server.started)
        base = f"http://127.0.0.1:{port}"
        submit_url = f"{base}/api/v1/triage/submit"
        payload = {"repo_name": REPO, "log": MARK}

        # a finished job with events, on an isolated stream (no real queue involved)
        jid = await enqueue_job(r, "sample_repo", "log", stream_key=TEST_STREAM)
        job_ids.append(jid)
        await publish_event(r, jid, "job_started", {"attempt": 1})
        await publish_event(r, jid, "job_completed", {"result": {"evaluation_status": "passed", "attempts": 1}})
        stream_url = f"{base}/api/v1/triage/stream/{jid}"

        # ---------- mode 1: no key configured (local development) ----------
        status, _ = await asyncio.to_thread(call, "POST", submit_url, None, payload)
        check("auth disabled: submit works without a key", status == 202, str(status))
        status, text = await asyncio.to_thread(call, "GET", stream_url)
        check("auth disabled: stream works without a key", status == 200 and "job_completed" in text, str(status))
        await cleanup_real(await find_jobs())

        # ---------- mode 2: key configured ----------
        os.environ["AUTODEV_API_KEY"] = KEY
        status, text = await asyncio.to_thread(call, "POST", submit_url, None, payload)
        check("auth on: submit without a key -> 401", status == 401, str(status))
        status, _ = await asyncio.to_thread(call, "POST", submit_url, "wrong-key-wrong-key-1", payload)
        check("auth on: submit with a wrong key -> 401", status == 401)
        status, _ = await asyncio.to_thread(call, "POST", submit_url, "", payload)
        check("auth on: submit with an empty key -> 401", status == 401)
        status, _ = await asyncio.to_thread(call, "POST", submit_url, KEY[:-1], payload)
        check("auth on: a prefix of the real key -> 401", status == 401)
        status, _ = await asyncio.to_thread(call, "POST", submit_url, KEY + "x", payload)
        check("auth on: the real key plus extra characters -> 401", status == 401)
        status, text = await asyncio.to_thread(call, "POST", submit_url, "wrong-key-wrong-key-1", payload)
        check("auth on: the 401 body is generic and never echoes a key",
              "invalid or missing API key" in text and "wrong-key" not in text and KEY not in text, text)
        check("auth on: none of the rejected submits enqueued anything", await find_jobs() == [])

        status, text = await asyncio.to_thread(call, "POST", submit_url, KEY, payload)
        ok_body = status == 202 and len(json.loads(text).get("job_id", "")) == 32
        check("auth on: submit with the correct key -> 202 and a job id", ok_body, f"{status} {text}")
        check("auth on: exactly the authorized submit was enqueued", len(await find_jobs()) == 1)
        await cleanup_real(await find_jobs())

        status, _ = await asyncio.to_thread(call, "GET", stream_url)
        check("auth on: stream without a key -> 401", status == 401)
        status, _ = await asyncio.to_thread(call, "GET", stream_url, "wrong-key-wrong-key-1")
        check("auth on: stream with a wrong key -> 401", status == 401)
        status, text = await asyncio.to_thread(call, "GET", stream_url, KEY)
        check("auth on: stream with the correct key replays the events",
              status == 200 and "job_started" in text and "job_completed" in text, str(status))
        status, _ = await asyncio.to_thread(call, "GET", f"{base}/api/v1/triage/stream/{uuid.uuid4().hex}")
        check("auth on: unknown job without a key -> 401, not 404 (no job-id probing)", status == 401, str(status))
        status, _ = await asyncio.to_thread(call, "GET", f"{base}/api/v1/triage/stream/{uuid.uuid4().hex}", KEY)
        check("auth on: unknown job with the key -> 404", status == 404)

        status, text = await asyncio.to_thread(call, "GET", f"{base}/openapi.json")
        check("auth on: openapi.json stays public and documents the X-API-Key scheme",
              status == 200 and "X-API-Key" in text and "apiKey" in text, str(status))

        # ---------- client library ----------
        events = await asyncio.to_thread(lambda: [e.event for e in stream_job(base, jid, api_key=KEY)])
        check("client: stream_job with the key works", events == ["job_started", "job_completed"], str(events))
        try:
            await asyncio.to_thread(lambda: list(stream_job(base, jid, retry_delay=0.01)))
            check("client: stream_job without the key raises 401 (not retried)", False)
        except urllib.error.HTTPError as e:
            check("client: stream_job without the key raises 401 (not retried)", e.code == 401)
        try:
            await asyncio.to_thread(submit_job, base, REPO, MARK)
            check("client: submit_job without the key raises 401", False)
        except urllib.error.HTTPError as e:
            check("client: submit_job without the key raises 401", e.code == 401)
        new_id = await asyncio.to_thread(submit_job, base, REPO, MARK, 10.0, KEY)
        check("client: submit_job with the key returns a job id", len(new_id) == 32)
        await cleanup_real(await find_jobs())

        # ---------- CLI (key comes from the environment only) ----------
        def run_cli(env: dict) -> tuple[int, str]:
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                code = cli_main(["--api", base, "watch", jid], environ=env)
            return code, buf.getvalue()

        code, out = await asyncio.to_thread(run_cli, {})
        check("cli: no key -> exit 4 with an authentication message",
              code == 4 and "Authentication failed" in out, f"{code} {out}")
        code, out = await asyncio.to_thread(run_cli, {"AUTODEV_API_KEY": "wrong-key-wrong-key-1"})
        check("cli: wrong key -> exit 4", code == 4)
        code, out = await asyncio.to_thread(run_cli, {"AUTODEV_API_KEY": KEY})
        check("cli: correct key -> exit 0 (verified fix) and renders the result",
              code == 0 and "Verified fix found" in out, f"{code} {out[:200]}")
    finally:
        os.environ.pop("AUTODEV_API_KEY", None)
        server.should_exit = True
        try:
            await asyncio.wait_for(server_task, timeout=10)
        except asyncio.TimeoutError:
            pass
        leftovers = await find_jobs()
        await cleanup_real(leftovers)
        await r.delete(TEST_STREAM, *[job_key(j) for j in job_ids], *[events_key(j) for j in job_ids])
        await r.aclose()


asyncio.run(main())
print("\nMODULE 8C: ALL CHECKS PASSED" if not failures else f"\nMODULE 8C: FAILED -> {failures}")
sys.exit(1 if failures else 0)