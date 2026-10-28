import asyncio
import json

import redis as redis_sync

from app.agent.runner import RepoNotIndexedError, build_result, run_triage
from app.config import REDIS_HOST, REDIS_PORT
from app.github.service import maybe_create_pr
from app.patch.service import make_patch_for_state
from app.queue.errors import PermanentJobError
from app.queue.events import publish_event_sync, step_payload
from app.queue.jobs import job_key


def make_triage_handler(redis, llm, embedder, reranker):
    """Build the worker handler: run the agent in a thread, stream steps,
    build the patch, optionally open a draft PR, store the result."""
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

        patch_info = await asyncio.to_thread(make_patch_for_state, state, job["repo_name"], job_id)
        if patch_info["patch_error"] is None:
            pr_info = await asyncio.to_thread(maybe_create_pr, state, job["repo_name"], job_id)
        else:
            pr_info = {"pr_url": None, "pr_number": None, "pr_error": None}
        result = {**build_result(state), **patch_info, **pr_info}
        await redis.hset(job_key(job_id), "result", json.dumps(result))

    return handler