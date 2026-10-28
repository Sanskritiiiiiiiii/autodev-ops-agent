"""Module 4A verification: submit endpoint + Redis enqueue.
Requires the API running:  uvicorn app.api.main:app --port 8000
"""
import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import redis

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.config import JOB_KEY_PREFIX, REDIS_HOST, REDIS_PORT, STREAM_KEY  # noqa: E402

API = os.environ.get("API_URL", "http://localhost:8000")
URL = f"{API}/api/v1/triage/submit"
failures: list[str] = []

API_KEY = os.environ.get("AUTODEV_API_KEY")


def check(name: str, cond: bool, detail: str = "") -> None:
    print(f"[{'PASS' if cond else 'FAIL'}] {name}" + (f"  {detail}" if detail and not cond else ""))
    if not cond:
        failures.append(name)


def post(raw: bytes) -> tuple[int, dict | None]:
    headers = {"Content-Type": "application/json"}
    if API_KEY:
        headers["X-API-Key"] = API_KEY
    req = urllib.request.Request(URL, data=raw, method="POST", headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return e.code, None


payload = json.loads((ROOT / "scripts" / "sample_payload.json").read_text(encoding="utf-8"))
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

created_job_ids: list[str] = []
try:
    len_before = r.xlen(STREAM_KEY)

    # ---- valid submit ----
    t0 = time.perf_counter()
    code, body = post(json.dumps(payload).encode())
    elapsed_ms = (time.perf_counter() - t0) * 1000
    print(f"       submit latency: {elapsed_ms:.1f} ms")
    check("valid submit returns 202", code == 202, f"got {code}")
    ok_body = bool(body) and body.get("status") == "queued" and len(body.get("job_id", "")) == 32
    check("response has 32-char job_id and status=queued", ok_body, str(body))
    job_id = body["job_id"] if body else ""
    created_job_ids.append(job_id)

    # ---- Redis state ----
    h = r.hgetall(f"{JOB_KEY_PREFIX}{job_id}")
    check("job hash status == queued", h.get("status") == "queued", str(h.get("status")))
    check("job hash stores repo_name and exact log",
          h.get("repo_name") == payload["repo_name"] and h.get("log") == payload["log"])
    check("job hash has created_at", bool(h.get("created_at")))
    check("stream length grew by exactly 1", r.xlen(STREAM_KEY) == len_before + 1,
          f"{len_before} -> {r.xlen(STREAM_KEY)}")
    in_stream = [mid for mid, f in r.xrevrange(STREAM_KEY, count=50) if f.get("job_id") == job_id]
    check("job_id present in stream", len(in_stream) == 1)

    # ---- validation (nothing may be enqueued) ----
    len_mid = r.xlen(STREAM_KEY)
    check("empty log -> 422", post(json.dumps({"repo_name": "x", "log": ""}).encode())[0] == 422)
    check("missing repo_name -> 422", post(json.dumps({"log": "boom"}).encode())[0] == 422)
    check("invalid JSON -> 422", post(b"{not json")[0] == 422)
    check("rejected requests enqueued nothing", r.xlen(STREAM_KEY) == len_mid)

    # ---- ordering ----
    submitted = []
    for i in range(3):
        c, b = post(json.dumps({"repo_name": "sample_repo", "log": f"order test {i}"}).encode())
        submitted.append(b["job_id"])
        created_job_ids.append(b["job_id"])
    ours = set(submitted)
    streamed = [f["job_id"] for _, f in r.xrange(STREAM_KEY) if f.get("job_id") in ours]
    check("stream preserves submission order", streamed == submitted, f"{streamed} vs {submitted}")

except (urllib.error.URLError, ConnectionError) as e:
    print(f"[FAIL] could not reach API at {API}: {e}")
    failures.append("api unreachable")
finally:
    # cleanup: remove only what this script created
    for mid, f in r.xrange(STREAM_KEY):
        if f.get("job_id") in created_job_ids:
            r.xdel(STREAM_KEY, mid)
    for jid in created_job_ids:
        r.delete(f"{JOB_KEY_PREFIX}{jid}")

print("\nMODULE 4A: ALL CHECKS PASSED" if not failures else f"\nMODULE 4A: FAILED -> {failures}")
sys.exit(1 if failures else 0)