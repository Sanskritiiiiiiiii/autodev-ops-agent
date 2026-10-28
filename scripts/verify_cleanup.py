"""Module 8A verification: stream cleanup and job expiry in the worker."""
import asyncio
import sys
import uuid
from pathlib import Path

import redis.asyncio as aioredis

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.config import JOB_TTL_S, REDIS_HOST, REDIS_PORT  # noqa: E402
from app.queue.errors import PermanentJobError  # noqa: E402
from app.queue.events import events_key  # noqa: E402
from app.queue.jobs import enqueue_job, job_key  # noqa: E402
from app.queue.worker import Worker  # noqa: E402

STREAM, GROUP = f"autodev:test:{uuid.uuid4().hex[:8]}:jobs", "test-workers"
failures: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    print(f"[{'PASS' if cond else 'FAIL'}] {name}" + (f"  {detail}" if detail and not cond else ""))
    if not cond:
        failures.append(name)


async def main() -> None:
    r = aioredis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    ids: dict[str, str] = {}

    async def handler(job_id: str, job: dict) -> None:
        if job["log"] == "bad":
            raise PermanentJobError("nope")

    worker = Worker(r, handler, "test-worker", stream_key=STREAM, group=GROUP, block_ms=100,
                    claim_idle_ms=5000, reclaim_interval_s=0.5, job_timeout_s=1.0, max_deliveries=3)
    try:
        await worker.setup()
        ids["ok"] = await enqueue_job(r, "sample_repo", "ok", stream_key=STREAM)
        ids["bad"] = await enqueue_job(r, "sample_repo", "bad", stream_key=STREAM)
        await r.xadd(STREAM, {"job_id": "ghost"})  # message with no job hash
        check("stream holds the 3 queued entries before processing", await r.xlen(STREAM) == 3)

        stop = asyncio.Event()
        task = asyncio.create_task(worker.run(stop))

        async def done() -> bool:
            return (await r.xlen(STREAM)) == 0 and (await r.xpending(STREAM, GROUP))["pending"] == 0

        for _ in range(100):
            if await done():
                break
            await asyncio.sleep(0.1)
        check("every processed entry was deleted from the stream (xlen 0)", await r.xlen(STREAM) == 0)
        check("nothing left pending", (await r.xpending(STREAM, GROUP))["pending"] == 0)

        ok_ttl, bad_ttl = await r.ttl(job_key(ids["ok"])), await r.ttl(job_key(ids["bad"]))
        check("completed job hash expires (TTL set, within JOB_TTL_S)", 0 < ok_ttl <= JOB_TTL_S, str(ok_ttl))
        check("permanently failed job hash expires too", 0 < bad_ttl <= JOB_TTL_S, str(bad_ttl))
        check("statuses are final",
              await r.hget(job_key(ids["ok"]), "status") == "completed"
              and await r.hget(job_key(ids["bad"]), "status") == "failed")
        check("event streams carry an expiry", 0 < await r.ttl(events_key(ids["ok"])) <= 86_400)

        stop.set()
        await asyncio.wait_for(task, timeout=5)
    finally:
        await r.delete(STREAM, *[job_key(j) for j in ids.values()], *[events_key(j) for j in ids.values()])
        await r.aclose()


asyncio.run(main())
print("\nMODULE 8A (cleanup): ALL CHECKS PASSED" if not failures else f"\nMODULE 8A (cleanup): FAILED -> {failures}")
sys.exit(1 if failures else 0)