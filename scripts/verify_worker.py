"""Module 4B verification: worker, ack, retry, timeout, crash recovery."""
import asyncio
import logging
import sys
import uuid
from collections import defaultdict
from pathlib import Path

import redis.asyncio as aioredis

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.config import REDIS_HOST, REDIS_PORT  # noqa: E402
from app.queue.jobs import enqueue_job, job_key  # noqa: E402
from app.queue.worker import Worker  # noqa: E402

logging.basicConfig(level=logging.ERROR)

STREAM = f"autodev:test:{uuid.uuid4().hex[:8]}:jobs"
GROUP = "test-workers"
failures: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    print(f"[{'PASS' if cond else 'FAIL'}] {name}" + (f"  {detail}" if detail and not cond else ""))
    if not cond:
        failures.append(name)


async def wait_until(cond, timeout: float) -> bool:
    end = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < end:
        if await cond():
            return True
        await asyncio.sleep(0.1)
    return False


async def main() -> None:
    r = aioredis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    calls: dict[str, int] = defaultdict(int)
    handled_ids: list[str] = []
    handled_tags: list[str] = []

    async def handler(job_id: str, job: dict) -> None:
        tag = job["log"]
        calls[tag] += 1
        handled_ids.append(job_id)
        handled_tags.append(tag)
        if tag == "retry-once" and calls[tag] == 1:
            raise RuntimeError("transient")
        if tag == "poison":
            raise ValueError("always fails")
        if tag == "slow":
            await asyncio.sleep(5)

    worker = Worker(r, handler, consumer="test-worker", stream_key=STREAM, group=GROUP,
                    block_ms=100, claim_idle_ms=500, reclaim_interval_s=0.2,
                    job_timeout_s=0.3, max_deliveries=3)
    ids: dict[str, str] = {}
    try:
        await worker.setup()
        await worker.setup()  # second call must tolerate the existing group
        check("setup is idempotent (BUSYGROUP tolerated)", True)

        # A "crashed" consumer reads a message and never acks it.
        ids["crashed"] = await enqueue_job(r, "sample_repo", "crashed", stream_key=STREAM)
        got = await r.xreadgroup(GROUP, "dead-worker", {STREAM: ">"}, count=1)
        check("dead consumer holds the crashed job in its pending list",
              got[0][1][0][1]["job_id"] == ids["crashed"])

        for tag in ("happy-1", "happy-2", "happy-3", "retry-once", "poison", "slow"):
            ids[tag] = await enqueue_job(r, "sample_repo", tag, stream_key=STREAM)
        await r.xadd(STREAM, {"job_id": "ghost"})  # no hash behind this message

        stop = asyncio.Event()
        task = asyncio.create_task(worker.run(stop))

        async def all_terminal() -> bool:
            for jid in ids.values():
                if await r.hget(job_key(jid), "status") not in ("completed", "failed"):
                    return False
            return True

        check("all jobs reach a terminal state within 20s", await wait_until(all_terminal, 20))

        async def none_pending() -> bool:
            return (await r.xpending(STREAM, GROUP))["pending"] == 0

        check("pending list drains to 0 (everything acked)", await wait_until(none_pending, 5))

        async def job(tag: str) -> dict:
            return await r.hgetall(job_key(ids[tag]))

        for tag in ("happy-1", "happy-2", "happy-3"):
            h = await job(tag)
            check(f"{tag}: completed on attempt 1",
                  h.get("status") == "completed" and h.get("attempt") == "1"
                  and bool(h.get("started_at")) and bool(h.get("finished_at")), str(h))

        happy_order = [t for t in handled_tags if t.startswith("happy")]
        check("happy jobs handled in submission order",
              happy_order == ["happy-1", "happy-2", "happy-3"], str(happy_order))

        h = await job("retry-once")
        check("retry-once: completed on attempt 2",
              h.get("status") == "completed" and h.get("attempt") == "2" and calls["retry-once"] == 2, str(h))

        h = await job("poison")
        check("poison: failed after exactly 3 handler calls",
              h.get("status") == "failed" and calls["poison"] == 3 and "ValueError" in h.get("error", ""), str(h))

        h = await job("slow")
        check("slow: failed with TimeoutError after 3 calls",
              h.get("status") == "failed" and calls["slow"] == 3 and "TimeoutError" in h.get("error", ""), str(h))

        h = await job("crashed")
        check("crashed: reclaimed from dead consumer, completed on attempt 2",
              h.get("status") == "completed" and h.get("attempt") == "2" and calls["crashed"] == 1, str(h))

        check("ghost message never reached the handler", "ghost" not in handled_ids)

        stop.set()
        try:
            await asyncio.wait_for(task, timeout=5)
            check("worker stops cleanly on stop_event", True)
        except asyncio.TimeoutError:
            check("worker stops cleanly on stop_event", False)
    finally:
        await r.delete(STREAM, *[job_key(j) for j in ids.values()])
        await r.aclose()


asyncio.run(main())
print("\nMODULE 4B: ALL CHECKS PASSED" if not failures else f"\nMODULE 4B: FAILED -> {failures}")
sys.exit(1 if failures else 0)