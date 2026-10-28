"""Module 8A verification: the running Docker Compose stack.
Needs: stack up, sample_repo indexed (see README steps), POSTGRES_PASSWORD set in this shell."""
import os
import sys
from pathlib import Path

import psycopg2
import redis

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.cli.sse_client import stream_job, submit_job  # noqa: E402
from app.config import JOB_TTL_S  # noqa: E402
from app.queue.events import events_key  # noqa: E402
from app.queue.jobs import job_key  # noqa: E402

API = os.environ.get("API_URL", "http://127.0.0.1:8000")
PG_PORT = int(os.environ.get("POSTGRES_HOST_PORT", "5433"))
PG_PASSWORD = os.environ.get("POSTGRES_PASSWORD")
PATCH_HOST_DIR = Path(os.environ.get("PATCHES_HOST_DIR", str(ROOT / "patches")))
API_KEY = os.environ.get("AUTODEV_API_KEY")
failures: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    print(f"[{'PASS' if cond else 'FAIL'}] {name}" + (f"  {detail}" if detail and not cond else ""))
    if not cond:
        failures.append(name)


if not PG_PASSWORD:
    sys.exit("Set POSTGRES_PASSWORD in this shell to the value from your .env file.")


def pg_connect(password: str):
    return psycopg2.connect(host="127.0.0.1", port=PG_PORT, user="autodev", password=password,
                            dbname="autodev", connect_timeout=5)


# ---- Postgres: password auth is enforced (trust auth is gone) ----
try:
    pg_connect(PG_PASSWORD).close()
    check("postgres accepts the configured password", True)
except psycopg2.OperationalError as e:
    check("postgres accepts the configured password", False, str(e))
try:
    pg_connect("definitely-wrong-password").close()
    check("postgres REJECTS a wrong password (no trust auth)", False)
except psycopg2.OperationalError as e:
    check("postgres REJECTS a wrong password (no trust auth)", "password authentication failed" in str(e), str(e))
conn = pg_connect(PG_PASSWORD)
with conn.cursor() as cur:
    cur.execute("SELECT count(*) FROM code_chunks WHERE repo_name = 'sample_repo'")
    chunks = cur.fetchone()[0]
conn.close()
check("sample_repo is indexed (run the index step first)", chunks > 0, str(chunks))

# ---- Redis: AOF persistence ----
r = redis.Redis(host="127.0.0.1", port=6379, decode_responses=True)
check("redis AOF persistence is on", r.config_get("appendonly").get("appendonly") == "yes")

# ---- one real job through the containers ----
log = (ROOT / "scripts" / "sample_error.log").read_text(encoding="utf-8")
job_id = submit_job(API, "sample_repo", log, api_key=API_KEY)
print(f"       submitted job {job_id}; waiting for the worker (real Groq call)...")
events = list(stream_job(API, job_id, read_timeout=90, api_key=API_KEY))
types = [e.event for e in events]
check("SSE stream through the API container ends with job_completed",
      types and types[0] == "job_started" and types[-1] == "job_completed", str(types))

import json  # noqa: E402

h = r.hgetall(job_key(job_id))
result = json.loads(h["result"]) if "result" in h else {}
check("job hash is completed with a stored result", h.get("status") == "completed" and bool(result))
check("agent reached a verdict", result.get("evaluation_status") in ("passed", "failed"))
check("job hash has an expiry (TTL)", 0 < r.ttl(job_key(job_id)) <= JOB_TTL_S, str(r.ttl(job_key(job_id))))
check("events stream has an expiry (TTL)", 0 < r.ttl(events_key(job_id)) <= 86_400)
check("acknowledged job was deleted from the queue stream",
      all(f.get("job_id") != job_id for _, f in r.xrange("autodev:triage:jobs")))
if result.get("evaluation_status") == "passed" and result.get("patch_path"):
    pf = PATCH_HOST_DIR / Path(result["patch_path"]).name
    check("container wrote the .patch into the bind-mounted patches folder", pf.is_file(), str(pf))
    check("patch path inside the container is under /data/patches", result["patch_path"].startswith("/data/patches/"))
else:
    print("       (agent did not produce a verified fix this run; patch checks skipped)")

print("\nMODULE 8A (stack): ALL CHECKS PASSED" if not failures else f"\nMODULE 8A (stack): FAILED -> {failures}")
sys.exit(1 if failures else 0)