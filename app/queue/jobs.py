import uuid
from datetime import datetime, timezone

from app.config import JOB_KEY_PREFIX, STREAM_KEY


def job_key(job_id: str) -> str:
    return f"{JOB_KEY_PREFIX}{job_id}"


async def enqueue_job(redis, repo_name: str, log: str, stream_key: str = STREAM_KEY) -> str:
    """Store the job payload and append its id to the stream, atomically."""
    job_id = uuid.uuid4().hex
    async with redis.pipeline(transaction=True) as pipe:
        pipe.hset(job_key(job_id), mapping={
            "status": "queued",
            "repo_name": repo_name,
            "log": log,
            "created_at": datetime.now(timezone.utc).isoformat(),
        })
        pipe.xadd(stream_key, {"job_id": job_id})
        await pipe.execute()
    return job_id