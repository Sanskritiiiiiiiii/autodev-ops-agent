import logging
from contextlib import asynccontextmanager

import redis.asyncio as aioredis
from fastapi import Depends, FastAPI, HTTPException, Request, status
from redis.exceptions import RedisError

from app.api.auth import check_api_key_config, require_api_key
from app.api.schemas import TriageSubmitRequest, TriageSubmitResponse
from app.api.stream import router as stream_router
from app.config import REDIS_HOST, REDIS_PORT
from app.queue.jobs import enqueue_job

log = logging.getLogger("autodev.api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    if not check_api_key_config():  # raises if the key is set but too short
        log.warning("AUTODEV_API_KEY is not set: the API is unauthenticated")
    app.state.redis = aioredis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    yield
    await app.state.redis.aclose()


app = FastAPI(title="AutoDev-Ops Agent", lifespan=lifespan)
app.include_router(stream_router)


@app.post(
    "/api/v1/triage/submit",
    response_model=TriageSubmitResponse,
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[Depends(require_api_key)],
)
async def submit_triage(body: TriageSubmitRequest, request: Request):
    try:
        job_id = await enqueue_job(request.app.state.redis, body.repo_name, body.log)
    except RedisError as exc:
        raise HTTPException(status_code=503, detail="Job queue unavailable") from exc
    return TriageSubmitResponse(job_id=job_id, status="queued")