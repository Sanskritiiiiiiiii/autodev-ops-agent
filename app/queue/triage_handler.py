import asyncio
import json

import redis as redis_sync

from app.agent.runner import RepoNotIndexedError, build_result, run_triage
from app.config import REDIS_HOST, REDIS_PORT
from app.queue.errors import PermanentJobError
from app.queue.events import publish_event_sync, step_payload
from app.queue.jobs import job_key


def make_triage_handler(redis, llm, embedder, reranker):
    """Build the worker handler: run the agent in a thread, stream steps, store the result."""
    sync_client = redis_sync.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    async def handler(job_id: str, job: dict) -> None:
        def on_step(node: str, update: dict) -> None:  # runs in the agent thread
            publish_event_sync(sync_client, job_id, "step", step_payload(node, update))

        try:
            state = await asyncio.to_thread(
                run_triage, job["log"], job["repo_name"], llm, embedder, reranker, on_step
            )
        except RepoNotIndexedError as exc:
            raise PermanentJobError(str(exc)) from exc
        await redis.hset(job_key(job_id), "result", json.dumps(build_result(state)))

    return handler