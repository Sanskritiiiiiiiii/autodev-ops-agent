import json
import re
import time

from fastapi import APIRouter, HTTPException, Request
from redis.exceptions import RedisError
from sse_starlette import EventSourceResponse

from app.queue.events import TERMINAL_TYPES, events_key
from app.queue.jobs import job_key

router = APIRouter()

BLOCK_MS = 5000
IDLE_TIMEOUT_S = 900.0
_JOB_ID = re.compile(r"^[0-9a-f]{32}$")
_STREAM_ID = re.compile(r"^\d+-\d+$")


@router.get("/api/v1/triage/stream/{job_id}")
async def stream_triage(job_id: str, request: Request):
    if not _JOB_ID.match(job_id):
        raise HTTPException(status_code=404, detail="job not found")
    last_id = request.headers.get("last-event-id") or "0-0"
    if not _STREAM_ID.match(last_id):
        raise HTTPException(status_code=400, detail="invalid Last-Event-ID")

    redis = request.app.state.redis
    try:
        exists = await redis.exists(job_key(job_id))
    except RedisError as exc:
        raise HTTPException(status_code=503, detail="event store unavailable") from exc
    if not exists:
        raise HTTPException(status_code=404, detail="job not found")

    async def event_generator():
        cursor = last_id
        last_activity = time.monotonic()
        while True:
            try:
                resp = await redis.xread({events_key(job_id): cursor}, count=50, block=BLOCK_MS)
            except RedisError:
                yield {"event": "stream_error", "data": json.dumps({"error": "event store unavailable"})}
                return
            if not resp:
                if time.monotonic() - last_activity > IDLE_TIMEOUT_S:
                    yield {"event": "timeout", "data": json.dumps({"idle_seconds": IDLE_TIMEOUT_S})}
                    return
                continue
            for _key, entries in resp:
                for entry_id, fields in entries:
                    cursor = entry_id
                    last_activity = time.monotonic()
                    yield {"id": entry_id, "event": fields["type"], "data": fields["data"]}
                    if fields["type"] in TERMINAL_TYPES:
                        return

    return EventSourceResponse(event_generator())