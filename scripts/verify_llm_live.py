"""Module 5B live smoke test: real Groq model through the full graph.
Needs GROQ_API_KEY and GROQ_MODEL in the environment."""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from rich.console import Console  # noqa: E402

from app.agent.graph import build_graph  # noqa: E402
from app.agent.llm_groq import make_groq_llm  # noqa: E402
from app.agent.state import initial_state  # noqa: E402
from app.agent.tools import check_fix  # noqa: E402
from app.db.chunk_store import init_schema, init_vector_index, replace_repo_chunks  # noqa: E402
from app.db.connection import get_connection  # noqa: E402
from app.ingestion.ast_chunker import ingest_repo  # noqa: E402
from app.search.dense_search import embed_missing_chunks  # noqa: E402
from app.search.embedder import Embedder  # noqa: E402
from app.search.hybrid import HybridRetriever  # noqa: E402
from app.search.reranker import Reranker  # noqa: E402

REPO_NAME = "sample_repo"
LOG = (
    "Traceback (most recent call last):\n"
    '  File "shop/orders.py", line 5, in get_order\n'
    '  File "shop/db.py", line 33, in run_query\n'
    '  File "shop/db.py", line 23, in acquire\n'
    "shop.db.DatabaseTimeout: ERR_DB_TIMEOUT"
)
console = Console()
failures: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    print(f"[{'PASS' if cond else 'FAIL'}] {name}" + (f"  {detail}" if detail and not cond else ""))
    if not cond:
        failures.append(name)


try:
    llm = make_groq_llm()
except RuntimeError as exc:
    print(f"Cannot run live test: {exc}")
    sys.exit(2)

console.print("Loading models and indexing sample_repo...")
embedder, reranker = Embedder(), Reranker()
conn = get_connection()
try:
    init_schema(conn)
    init_vector_index(conn)
    replace_repo_chunks(conn, REPO_NAME, ingest_repo(ROOT / "sample_repo").chunks)
    embed_missing_chunks(conn, REPO_NAME, embedder)
    graph = build_graph(llm, HybridRetriever(conn, REPO_NAME, embedder, reranker))

    state = initial_state(LOG)
    sequence: list[str] = []
    try:
        for step in graph.stream(state, stream_mode="updates"):
            for node, update in step.items():
                sequence.append(node)
                state.update(update)
    except Exception as exc:  # noqa: BLE001
        check("graph ran without raising", False, f"{type(exc).__name__}: {exc}")
        raise SystemExit(1)

    console.print(f"\nTransitions: {' -> '.join(sequence)}")
    console.print(f"Search query : {state['search_query']}")
    console.print(f"Hypothesis   : {state['hypothesis']}")
    console.print(f"Attempts     : {state['attempts']}   Status: {state['evaluation_status']}")
    console.print(f"Feedback     : {state['verification_feedback']}")
    console.print(f"Diff preview :\n{state['diff_preview'] or '(none)'}\n")

    check("graph ran without raising", True)
    check("starts with analyze_log -> retrieve_code", sequence[:2] == ["analyze_log", "retrieve_code"])
    check("search query is non-empty", bool(state["search_query"].strip()))
    check("retrieval returned chunks", len(state["retrieved_chunks"]) > 0)
    check("attempts within limit", 1 <= state["attempts"] <= 3)
    check("terminal status is passed or failed", state["evaluation_status"] in ("passed", "failed"))
    if state["evaluation_status"] == "passed":
        check("passed fix re-verifies and has a diff",
              check_fix(state["proposed_fix"], state["retrieved_chunks"])["ok"]
              and state["diff_preview"].startswith("--- a/"))
    else:
        check("failed run has no diff", state["diff_preview"] == "")
finally:
    conn.close()

print("\nMODULE 5B (live): ALL CHECKS PASSED" if not failures else f"\nMODULE 5B (live): FAILED -> {failures}")
sys.exit(1 if failures else 0)