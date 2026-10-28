"""Module 5C verification: graph wired into the worker."""
import asyncio
import json
import sys
import time
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
from app.queue.jobs import enqueue_job, job_key  # noqa: E402
from app.queue.triage_handler import make_triage_handler  # noqa: E402
from app.queue.worker import Worker  # noqa: E402
from app.search.dense_search import embed_missing_chunks  # noqa: E402
from app.search.embedder import Embedder  # noqa: E402
from app.search.reranker import Reranker  # noqa: E402

STREAM = f"autodev:test:{uuid.uuid4().hex[:8]}:jobs"
GROUP = "test-workers"
REPO = "sample_repo"
BASE_LOG = (
    "Traceback (most recent call last):\n"
    '  File "shop/db.py", line 23, in acquire\n'
    "shop.db.DatabaseTimeout: ERR_DB_TIMEOUT"
)
LOG_HAPPY, LOG_EXHAUST, LOG_MISSING = (f"{BASE_LOG}\n# case: {c}" for c in ("happy", "exhaust", "missing"))

BAD_SYNTAX = "def acquire(self, timeout=5.0)\n    return self\n"
WRONG_NAME = "def other(self):\n    return 1\n"
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

failures: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    print(f"[{'PASS' if cond else 'FAIL'}] {name}" + (f"  {detail}" if detail and not cond else ""))
    if not cond:
        failures.append(name)


class ScriptedLLM:
    def __init__(self, drafts_by_marker: dict[str, list[str]]):
        self.drafts = drafts_by_marker
        self.calls: dict[str, int] = defaultdict(int)
        self.analyzed: list[str] = []

    @staticmethod
    def _marker(log: str) -> str:
        return log.rsplit("\n", 1)[-1]

    def analyze(self, log: str) -> dict:
        self.analyzed.append(self._marker(log))
        return {"hypothesis": "Connection pool exhausted.", "search_query": "ERR_DB_TIMEOUT"}

    def draft_fix(self, log, hypothesis, chunks, feedback):
        m = self._marker(log)
        n = self.calls[m]
        self.calls[m] += 1
        drafts = self.drafts[m]
        target = next(c for c in chunks if c["qualified_name"] == "ConnectionPool.acquire")
        return {"hypothesis": hypothesis, "chunk_id": target["chunk_id"],
                "new_code": drafts[min(n, len(drafts) - 1)]}


async def wait_until(cond, timeout: float) -> bool:
    end = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < end:
        if await cond():
            return True
        await asyncio.sleep(0.2)
    return False


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
    llm = ScriptedLLM({
        "# case: happy": [BAD_SYNTAX, GOOD],
        "# case: exhaust": [WRONG_NAME],
    })
    inner = make_triage_handler(r, llm, embedder, reranker)

    ticks = [0]
    timings: dict[str, tuple[float, int]] = {}
    handler_calls: dict[str, int] = defaultdict(int)

    async def ticker() -> None:
        while True:
            await asyncio.sleep(0.05)
            ticks[0] += 1

    async def handler(job_id: str, job: dict) -> None:
        marker = job["log"].rsplit("\n", 1)[-1]
        handler_calls[marker] += 1
        t0, k0 = time.perf_counter(), ticks[0]
        try:
            await inner(job_id, job)
        finally:
            timings[marker] = (time.perf_counter() - t0, ticks[0] - k0)

    worker = Worker(r, handler, consumer="test-worker", stream_key=STREAM, group=GROUP,
                    block_ms=100, claim_idle_ms=120_000, reclaim_interval_s=1.0,
                    job_timeout_s=60.0, max_deliveries=3)
    ids: dict[str, str] = {}
    tick_task = asyncio.create_task(ticker())
    stop = asyncio.Event()
    worker_task = None
    try:
        await worker.setup()
        ids["happy"] = await enqueue_job(r, REPO, LOG_HAPPY, stream_key=STREAM)
        ids["exhaust"] = await enqueue_job(r, REPO, LOG_EXHAUST, stream_key=STREAM)
        ids["missing"] = await enqueue_job(r, "no_such_repo", LOG_MISSING, stream_key=STREAM)
        worker_task = asyncio.create_task(worker.run(stop))

        async def all_terminal() -> bool:
            for jid in ids.values():
                if await r.hget(job_key(jid), "status") not in ("completed", "failed"):
                    return False
            return True

        check("all three jobs reach a terminal state", await wait_until(all_terminal, 180))

        # ---- happy path ----
        h = await r.hgetall(job_key(ids["happy"]))
        res = json.loads(h["result"]) if "result" in h else {}
        check("happy: job completed on first delivery", h.get("status") == "completed" and h.get("attempt") == "1", str(h.get("status")))
        check("happy: result passed after 2 agent attempts",
              res.get("evaluation_status") == "passed" and res.get("attempts") == 2, str(res.get("evaluation_status")))
        check("happy: stored fix targets a retrieved chunk and has new code",
              bool(res.get("proposed_fix")) and res["proposed_fix"]["new_code"].startswith("def acquire")
              and res["proposed_fix"]["chunk_id"] in [c["chunk_id"] for c in res["retrieved"]])
        check("happy: diff has the real hunk header", "@@ -19,4 +19,4 @@" in res.get("diff_preview", ""))
        check("happy: retrieved list includes ConnectionPool.acquire",
              "ConnectionPool.acquire" in [c["qualified_name"] for c in res.get("retrieved", [])])
        check("happy: handler ran exactly once", handler_calls["# case: happy"] == 1)

        # ---- exhausted ----
        h = await r.hgetall(job_key(ids["exhaust"]))
        res = json.loads(h["result"]) if "result" in h else {}
        check("exhaust: job completed (agent verdict is in result)", h.get("status") == "completed")
        check("exhaust: evaluation failed after 3 attempts, no fix stored",
              res.get("evaluation_status") == "failed" and res.get("attempts") == 3
              and res.get("proposed_fix") is None and res.get("diff_preview") == "")
        check("exhaust: feedback names the problem", "must remain 'acquire'" in (res.get("verification_feedback") or ""))

        # ---- permanent failure ----
        h = await r.hgetall(job_key(ids["missing"]))
        check("missing repo: job failed permanently on first delivery",
              h.get("status") == "failed" and h.get("attempt") == "1" and "no_such_repo" in h.get("error", ""), str(h))
        check("missing repo: no retries, LLM never called",
              handler_calls["# case: missing"] == 1 and "# case: missing" not in llm.analyzed)

        # ---- event loop stays responsive while the agent runs ----
        duration, during = timings.get("# case: happy", (0.0, 0))
        expected = duration / 0.05
        check("agent ran long enough to be a meaningful test (>0.3s)", duration > 0.3, f"{duration:.2f}s")
        check("event loop kept ticking during the agent run (asyncio.to_thread works)",
              during >= 0.5 * expected, f"{during} ticks in {duration:.2f}s (expected ~{expected:.0f})")

        async def none_pending() -> bool:
            return (await r.xpending(STREAM, GROUP))["pending"] == 0

        check("pending list drained", await wait_until(none_pending, 5))
    finally:
        stop.set()
        if worker_task:
            try:
                await asyncio.wait_for(worker_task, timeout=10)
            except asyncio.TimeoutError:
                pass
        tick_task.cancel()
        await r.delete(STREAM, *[job_key(j) for j in ids.values()])
        await r.aclose()


asyncio.run(main())
print("\nMODULE 5C: ALL CHECKS PASSED" if not failures else f"\nMODULE 5C: FAILED -> {failures}")
sys.exit(1 if failures else 0)