import asyncio
import json

from app.agent.runner import RepoNotIndexedError, build_result, run_triage
from app.queue.errors import PermanentJobError
from app.queue.jobs import job_key


def make_triage_handler(redis, llm, embedder, reranker):
    """Build the worker handler: run the agent in a thread, store the result."""

    async def handler(job_id: str, job: dict) -> None:
        try:
            state = await asyncio.to_thread(
                run_triage, job["log"], job["repo_name"], llm, embedder, reranker
            )
        except RepoNotIndexedError as exc:
            raise PermanentJobError(str(exc)) from exc
        await redis.hset(job_key(job_id), "result", json.dumps(build_result(state)))

    return handler