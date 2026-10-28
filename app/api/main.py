from contextlib import asynccontextmanager

import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException, Request, status
from redis.exceptions import RedisError

from app.api.schemas import TriageSubmitRequest, TriageSubmitResponse
from app.api.stream import router as stream_router
from app.config import REDIS_HOST, REDIS_PORT
from app.queue.jobs import enqueue_job


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.redis = aioredis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    yield
    await app.state.redis.aclose()


app = FastAPI(title="AutoDev-Ops Agent", lifespan=lifespan)
app.include_router(stream_router)


@app.post(
    "/api/v1/triage/submit",
    response_model=TriageSubmitResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def submit_triage(body: TriageSubmitRequest, request: Request):
    try:
        job_id = await enqueue_job(request.app.state.redis, body.repo_name, body.log)
    except RedisError as exc:
        raise HTTPException(status_code=503, detail="Job queue unavailable") from exc
    return TriageSubmitResponse(job_id=job_id, status="queued")