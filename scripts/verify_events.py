"""Module 6A verification: per-job event streams."""
import asyncio
import sys
import uuid
from collections import defaultdict
from pathlib import Path

import redis.asyncio as aioredis

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.config import REDIS_HOST, REDIS_PORT  # noqa: E402
from app.db.chunk_store import init_schema, init_vector_index, replace_repo_chunks  # noqa: E402
from app.db.connection import get_connection  # noqa: E402
from app.ingestion.ast_chunker import ingest_repo  # noqa: E402
from app.queue.events import TERMINAL_TYPES, events_key, parse_event  # noqa: E402
from app.queue.jobs import enqueue_job, job_key  # noqa: E402
from app.queue.triage_handler import make_triage_handler  # noqa: E402
from app.queue.worker import Worker  # noqa: E402
from app.search.dense_search import embed_missing_chunks  # noqa: E402
from app.search.embedder import Embedder  # noqa: E402
from app.search.reranker import Reranker  # noqa: E402

TAG = uuid.uuid4().hex[:8]
STREAM_A, STREAM_B, GROUP = f"autodev:test:{TAG}:a", f"autodev:test:{TAG}:b", "test-workers"
REPO = "sample_repo"
BASE_LOG = (
    "Traceback (most recent call last):\n"
    '  File "shop/db.py", line 23, in acquire\n'
    "shop.db.DatabaseTimeout: ERR_DB_TIMEOUT"
)
LOG_HAPPY, LOG_MISSING = (f"{BASE_LOG}\n# case: {c}" for c in ("happy", "missing"))
BAD_SYNTAX = "def acquire(self, timeout=5.0)\n    return self\n"
GOOD = (
    "def acquire(self, timeout=5.0):\n"
    "    deadline = time.monotonic() + timeout\n"
    "    while self.in_use >= self.max_size:\n"
    "        if time.monotonic() > deadline:\n"
    "            raise DatabaseTimeout(ERR_DB_TIMEOUT)\n"
    "        time.sleep(0.01)\n"
    "    self.in_use += 1\n"
    "    return self\n"
)
VOCAB = {"job_started", "step", "job_retrying", "job_completed", "job_failed"}
failures: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    print(f"[{'PASS' if cond else 'FAIL'}] {name}" + (f"  {detail}" if detail and not cond else ""))
    if not cond:
        failures.append(name)


class ScriptedLLM:
    def __init__(self):
        self.calls: dict[str, int] = defaultdict(int)

    def analyze(self, log):
        return {"hypothesis": "Connection pool exhausted.", "search_query": "ERR_DB_TIMEOUT"}

    def draft_fix(self, log, hypothesis, chunks, feedback):
        marker = log.rsplit("\n", 1)[-1]
        n = self.calls[marker]
        self.calls[marker] += 1
        target = next(c for c in chunks if c["qualified_name"] == "ConnectionPool.acquire")
        return {"hypothesis": hypothesis, "chunk_id": target["chunk_id"],
                "new_code": [BAD_SYNTAX, GOOD][min(n, 1)]}


async def wait_until(cond, timeout: float) -> bool:
    end = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < end:
        if await cond():
            return True
        await asyncio.sleep(0.2)
    return False


async def read_events(r, job_id: str) -> list[tuple[str, dict]]:
    return [parse_event(f) for _, f in await r.xrange(events_key(job_id))]


async def wait_terminal(r, job_id: str, timeout: float) -> bool:
    async def cond() -> bool:
        return any(t in TERMINAL_TYPES for t, _ in await read_events(r, job_id))
    return await wait_until(cond, timeout)


async def stop_worker(stop: asyncio.Event, task: asyncio.Task) -> None:
    stop.set()
    try:
        await asyncio.wait_for(task, timeout=10)
    except asyncio.TimeoutError:
        pass


async def main() -> None:
    print("Loading models and indexing sample_repo...")
    embedder, reranker = Embedder(), Reranker()
    conn = get_connection()
    try:
        init_schema(conn)
        init_vector_index(conn)
        replace_repo_chunks(conn, REPO, ingest_repo(ROOT / "sample_repo").chunks)
        embed_missing_chunks(conn, REPO, embedder)
    finally:
        conn.close()

    r = aioredis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    job_ids: list[str] = []
    try:
        # ---------- Part B: retry events with a fast flaky handler ----------
        calls_b: dict[str, int] = defaultdict(int)

        async def flaky(job_id: str, job: dict) -> None:
            calls_b[job_id] += 1
            if calls_b[job_id] == 1:
                raise RuntimeError("transient")

        worker_b = Worker(r, flaky, "test-b", stream_key=STREAM_B, group=GROUP, block_ms=100,
                          claim_idle_ms=500, reclaim_interval_s=0.2, job_timeout_s=0.3, max_deliveries=3)
        await worker_b.setup()
        jb = await enqueue_job(r, REPO, "flaky", stream_key=STREAM_B)
        job_ids.append(jb)
        stop_b = asyncio.Event()
        task_b = asyncio.create_task(worker_b.run(stop_b))
        check("flaky: terminal event arrives", await wait_terminal(r, jb, 15))
        evs = await read_events(r, jb)
        types = [t for t, _ in evs]
        check("flaky: started -> retrying -> started -> completed",
              types == ["job_started", "job_retrying", "job_started", "job_completed"], str(types))
        check("flaky: retry event carries attempt and error",
              evs[1][1].get("attempt") == 1 and "RuntimeError" in evs[1][1].get("error", ""))
        check("flaky: second start is attempt 2", evs[2][1].get("attempt") == 2)
        check("flaky: plain handler completes with result None", evs[3][1].get("result", "x") is None)
        await stop_worker(stop_b, task_b)

        # ---------- Part A: real agent path ----------
        llm = ScriptedLLM()
        worker_a = Worker(r, make_triage_handler(r, llm, embedder, reranker), "test-a",
                          stream_key=STREAM_A, group=GROUP, block_ms=100, claim_idle_ms=120_000,
                          reclaim_interval_s=1.0, job_timeout_s=60.0, max_deliveries=3)
        await worker_a.setup()
        happy = await enqueue_job(r, REPO, LOG_HAPPY, stream_key=STREAM_A)
        missing = await enqueue_job(r, "no_such_repo", LOG_MISSING, stream_key=STREAM_A)
        job_ids += [happy, missing]
        stop_a = asyncio.Event()
        task_a = asyncio.create_task(worker_a.run(stop_a))

        check("happy: terminal event arrives", await wait_terminal(r, happy, 120))
        check("missing: terminal event arrives", await wait_terminal(r, missing, 120))

        evs = await read_events(r, happy)
        types = [t for t, _ in evs]
        steps = [d for t, d in evs if t == "step"]
        nodes = [s["node"] for s in steps]
        check("happy: event types are started, 7 steps, completed",
              types == ["job_started"] + ["step"] * 7 + ["job_completed"], str(types))
        check("happy: step nodes follow the graph incl. one retry",
              nodes == ["analyze_log", "retrieve_code", "draft_fix", "verify",
                        "draft_fix", "verify", "output_diff"], str(nodes))
        check("happy: only known event types", {t for t in types} <= VOCAB)
        check("happy: analyze step carries hypothesis and query",
              steps[0].get("search_query") == "ERR_DB_TIMEOUT" and bool(steps[0].get("hypothesis")))
        chunks = steps[1].get("chunks", [])
        check("happy: retrieve step lists chunks without source text",
              "ConnectionPool.acquire" in [c["qualified_name"] for c in chunks]
              and all("content" not in c for c in chunks))
        check("happy: draft steps numbered 1 and 2",
              [steps[2]["attempt"], steps[4]["attempt"]] == [1, 2] and "def acquire" in steps[4]["new_code"])
        check("happy: first verify failed with SyntaxError, second passed",
              steps[3]["evaluation_status"] == "failed" and str(steps[3]["feedback"]).startswith("SyntaxError")
              and steps[5]["evaluation_status"] == "passed" and steps[5]["feedback"] is None)
        check("happy: diff step has the real hunk header", "@@ -19,4 +19,4 @@" in steps[6]["diff"])
        res = evs[-1][1].get("result") or {}
        check("happy: completed event carries the full result",
              res.get("evaluation_status") == "passed" and res.get("attempts") == 2
              and res.get("diff_preview") == steps[6]["diff"])
        check("happy: events stream has an expiry set", 0 < await r.ttl(events_key(happy)) <= 86_400)
        check("happy: job hash status is completed", await r.hget(job_key(happy), "status") == "completed")

        evs = await read_events(r, missing)
        types = [t for t, _ in evs]
        check("missing repo: started then failed, no retries", types == ["job_started", "job_failed"], str(types))
        check("missing repo: failure names the repo", "no_such_repo" in evs[-1][1].get("error", ""))
        await stop_worker(stop_a, task_a)
    finally:
        keys = [STREAM_A, STREAM_B] + [job_key(j) for j in job_ids] + [events_key(j) for j in job_ids]
        await r.delete(*keys)
        await r.aclose()


asyncio.run(main())
print("\nMODULE 6A: ALL CHECKS PASSED" if not failures else f"\nMODULE 6A: FAILED -> {failures}")
sys.exit(1 if failures else 0)