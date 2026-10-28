import json

from app.config import JOB_KEY_PREFIX

EVENTS_TTL_S = 86_400
TERMINAL_TYPES = ("job_completed", "job_failed")
_CHUNK_KEYS = ("qualified_name", "chunk_type", "file_path", "start_line", "end_line")


def events_key(job_id: str) -> str:
    return f"{JOB_KEY_PREFIX}{job_id}:events"


def _fields(event_type: str, data: dict | None) -> dict:
    return {"type": event_type, "data": json.dumps(data or {})}


async def publish_event(redis, job_id: str, event_type: str, data: dict | None = None) -> None:
    key = events_key(job_id)
    async with redis.pipeline(transaction=True) as pipe:
        pipe.xadd(key, _fields(event_type, data))
        pipe.expire(key, EVENTS_TTL_S)
        await pipe.execute()


def publish_event_sync(redis, job_id: str, event_type: str, data: dict | None = None) -> None:
    """Same as publish_event, for use from the agent's worker thread."""
    key = events_key(job_id)
    with redis.pipeline(transaction=True) as pipe:
        pipe.xadd(key, _fields(event_type, data))
        pipe.expire(key, EVENTS_TTL_S)
        pipe.execute()


def parse_event(fields: dict) -> tuple[str, dict]:
    return fields["type"], json.loads(fields["data"])


def step_payload(node: str, update: dict) -> dict:
    """Turn one LangGraph state update into a compact, human-readable event payload."""
    if node == "analyze_log":
        return {"node": node, "hypothesis": update["hypothesis"],
                "search_query": update["search_query"]}
    if node == "retrieve_code":
        return {"node": node,
                "chunks": [{k: c[k] for k in _CHUNK_KEYS} for c in update["retrieved_chunks"]]}
    if node == "draft_fix":
        fix = update["proposed_fix"]
        return {"node": node, "attempt": update["attempts"], "hypothesis": update["hypothesis"],
                "chunk_id": fix["chunk_id"], "new_code": fix["new_code"]}
    if node == "verify":
        return {"node": node, "evaluation_status": update["evaluation_status"],
                "feedback": update["verification_feedback"]}
    if node == "output_diff":
        return {"node": node, "diff": update["diff_preview"]}
    return {"node": node}