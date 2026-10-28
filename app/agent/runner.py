from app.agent.graph import build_graph
from app.agent.state import initial_state
from app.db.chunk_store import count_chunks
from app.db.connection import get_connection
from app.search.hybrid import HybridRetriever


class RepoNotIndexedError(Exception):
    pass


def run_triage(log: str, repo_name: str, llm, embedder, reranker, on_step=None) -> dict:
    """Run the full agent graph synchronously. Call via asyncio.to_thread from async code.
    on_step(node_name, state_update) is called after every graph step."""
    conn = get_connection()
    try:
        if count_chunks(conn, repo_name) == 0:
            raise RepoNotIndexedError(f"repo '{repo_name}' has no indexed chunks")
        graph = build_graph(llm, HybridRetriever(conn, repo_name, embedder, reranker))
        state = initial_state(log)
        for step in graph.stream(state, stream_mode="updates"):
            for node, update in step.items():
                state.update(update)
                if on_step:
                    on_step(node, update)
        return dict(state)
    finally:
        conn.close()


_RETRIEVED_KEYS = ("chunk_id", "qualified_name", "chunk_type", "file_path",
                   "start_line", "end_line", "rerank_score")


def build_result(state: dict) -> dict:
    """JSON-serializable summary stored in the job hash."""
    passed = state["evaluation_status"] == "passed"
    return {
        "evaluation_status": state["evaluation_status"],
        "attempts": state["attempts"],
        "hypothesis": state["hypothesis"],
        "search_query": state["search_query"],
        "verification_feedback": state["verification_feedback"],
        "proposed_fix": state["proposed_fix"] if passed else None,
        "diff_preview": state["diff_preview"],
        "retrieved": [{k: c[k] for k in _RETRIEVED_KEYS} for c in state["retrieved_chunks"]],
    }