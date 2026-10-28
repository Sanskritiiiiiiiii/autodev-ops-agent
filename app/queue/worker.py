import asyncio
import json
import logging
import time
from datetime import datetime, timezone

from redis.exceptions import RedisError, ResponseError

from app.config import CONSUMER_GROUP, STREAM_KEY
from app.queue.errors import PermanentJobError
from app.queue.events import publish_event
from app.queue.jobs import job_key

log = logging.getLogger("autodev.worker")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class Worker:
    """Redis Streams consumer: read, process, ack, retry, reclaim."""

    def __init__(self, redis, handler, consumer: str, *,
                 stream_key: str = STREAM_KEY, group: str = CONSUMER_GROUP,
                 block_ms: int = 1000, claim_idle_ms: int = 300_000,
                 reclaim_interval_s: float = 30.0, job_timeout_s: float = 240.0,
                 max_deliveries: int = 3):
        if job_timeout_s * 1000 >= claim_idle_ms:
            raise ValueError("job_timeout_s must be shorter than claim_idle_ms")
        self.redis = redis
        self.handler = handler
        self.consumer = consumer
        self.stream_key = stream_key
        self.group = group
        self.block_ms = block_ms
        self.claim_idle_ms = claim_idle_ms
        self.reclaim_interval_s = reclaim_interval_s
        self.job_timeout_s = job_timeout_s
        self.max_deliveries = max_deliveries

    async def setup(self) -> None:
        """Create the consumer group (and stream) if missing."""
        try:
            await self.redis.xgroup_create(self.stream_key, self.group, id="0", mkstream=True)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def run(self, stop_event: asyncio.Event | None = None) -> None:
        await self.setup()
        log.info("worker %s listening on %s/%s", self.consumer, self.stream_key, self.group)
        last_reclaim = float("-inf")
        while stop_event is None or not stop_event.is_set():
            try:
                if time.monotonic() - last_reclaim >= self.reclaim_interval_s:
                    await self._reclaim_once()
                    last_reclaim = time.monotonic()
                resp = await self.redis.xreadgroup(
                    self.group, self.consumer, {self.stream_key: ">"},
                    count=1, block=self.block_ms,
                )
                for _stream, messages in resp or []:
                    for msg_id, fields in messages:
                        await self._process(msg_id, fields)
            except RedisError as exc:
                log.error("redis error: %r; retrying in 1s", exc)
                await asyncio.sleep(1)

    async def _reclaim_once(self) -> None:
        """Take over messages that sat unacknowledged longer than claim_idle_ms."""
        start = "0-0"
        while True:
            res = await self.redis.xautoclaim(
                self.stream_key, self.group, self.consumer,
                min_idle_time=self.claim_idle_ms, start_id=start, count=10,
            )
            next_start, messages = res[0], res[1]
            for msg_id, fields in messages:
                log.info("reclaimed %s", msg_id)
                await self._process(msg_id, fields)
            if next_start == "0-0":
                break
            start = next_start

    async def _delivery_count(self, msg_id: str) -> int:
        info = await self.redis.xpending_range(
            self.stream_key, self.group, min=msg_id, max=msg_id, count=1
        )
        return int(info[0]["times_delivered"]) if info else 1

    async def _ack(self, msg_id: str) -> None:
        await self.redis.xack(self.stream_key, self.group, msg_id)

    async def _emit(self, job_id: str, event_type: str, data: dict | None = None) -> None:
        await publish_event(self.redis, job_id, event_type, data)

    async def _process(self, msg_id: str, fields: dict) -> None:
        job_id = fields.get("job_id")
        job = await self.redis.hgetall(job_key(job_id)) if job_id else {}
        if not job:
            log.warning("message %s has no job payload; dropping", msg_id)
            await self._ack(msg_id)
            return

        key = job_key(job_id)
        attempt = await self._delivery_count(msg_id)
        if attempt > self.max_deliveries:  # worker died during the last attempt
            error = f"max deliveries ({self.max_deliveries}) exceeded"
            await self.redis.hset(key, mapping={
                "status": "failed", "finished_at": _now(), "error": error,
            })
            await self._emit(job_id, "job_failed", {"error": error})
            await self._ack(msg_id)
            return

        await self.redis.hset(key, mapping={
            "status": "processing", "started_at": _now(), "attempt": str(attempt),
        })
        await self._emit(job_id, "job_started", {"attempt": attempt})
        try:
            await asyncio.wait_for(self.handler(job_id, job), timeout=self.job_timeout_s)
        except Exception as exc:  # handler error or timeout; CancelledError is not caught
            error = repr(exc)
            if isinstance(exc, PermanentJobError) or attempt >= self.max_deliveries:
                log.error("job %s failed permanently: %s", job_id, error)
                await self.redis.hset(key, mapping={
                    "status": "failed", "finished_at": _now(), "error": error,
                })
                await self._emit(job_id, "job_failed", {"error": error})
                await self._ack(msg_id)
            else:
                log.warning("job %s attempt %d failed (%s); will retry", job_id, attempt, error)
                await self.redis.hset(key, mapping={"status": "retrying", "error": error})
                await self._emit(job_id, "job_retrying", {"attempt": attempt, "error": error})
            return

        await self.redis.hset(key, mapping={"status": "completed", "finished_at": _now()})
        raw_result = await self.redis.hget(key, "result")
        await self._emit(job_id, "job_completed",
                         {"result": json.loads(raw_result) if raw_result else None})
        await self._ack(msg_id)
        log.info("job %s completed (attempt %d)", job_id, attempt)