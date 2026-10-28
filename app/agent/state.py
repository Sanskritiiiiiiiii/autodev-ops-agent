from typing import Protocol, TypedDict


class AgentState(TypedDict, total=False):
    log: str
    search_query: str
    retrieved_chunks: list[dict]
    hypothesis: str
    proposed_fix: dict | None          # {"chunk_id": str, "new_code": str}
    verification_feedback: str | None
    attempts: int
    evaluation_status: str             # "pending" | "passed" | "failed"
    diff_preview: str


class LLM(Protocol):
    """What the graph needs from a model. 5B provides the real adapter."""

    def analyze(self, log: str) -> dict:
        """Return {"hypothesis": str, "search_query": str}."""
        ...

    def draft_fix(self, log: str, hypothesis: str, chunks: list[dict],
                  feedback: str | None) -> dict:
        """Return {"hypothesis": str, "chunk_id": str, "new_code": str}."""
        ...


def initial_state(log: str) -> AgentState:
    return {
        "log": log,
        "search_query": "",
        "retrieved_chunks": [],
        "hypothesis": "",
        "proposed_fix": None,
        "verification_feedback": None,
        "attempts": 0,
        "evaluation_status": "pending",
        "diff_preview": "",
    }