"""Module 5A verification: tools, graph transitions, retry loop."""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.agent.graph import build_graph  # noqa: E402
from app.agent.state import initial_state  # noqa: E402
from app.agent.tools import (  # noqa: E402
    build_diff_preview, check_fix, retrieve_code_context, verify_syntax_and_lint,
)

failures: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    print(f"[{'PASS' if cond else 'FAIL'}] {name}" + (f"  {detail}" if detail and not cond else ""))
    if not cond:
        failures.append(name)


# =============== Part 1: pure tool tests (no models, no DB) ===============
GOOD_METHOD = "    def f(self):\n        return 2"
r = verify_syntax_and_lint(GOOD_METHOD, "f")
check("verify: indented method accepted", r["ok"], str(r))
r = verify_syntax_and_lint("def f(self)\n    return 1", "f")
check("verify: syntax error rejected", not r["ok"] and r["errors"][0].startswith("SyntaxError"), str(r))
r = verify_syntax_and_lint("def g(self):\n    return 1", "f")
check("verify: renamed function rejected", not r["ok"] and "must remain 'f'" in r["errors"][0], str(r))
r = verify_syntax_and_lint("def f():\n    pass\n\ndef h():\n    pass", "f")
check("verify: two top-level defs rejected", not r["ok"], str(r))
r = verify_syntax_and_lint("def f():\n    await x", "f")
check("verify: await outside async rejected", not r["ok"], str(r))

chunks = [
    {"chunk_id": "m1", "chunk_type": "method", "name": "f", "content": "    def f(self):\n        return 1",
     "file_path": "pkg/a.py", "start_line": 10, "end_line": 11, "qualified_name": "C.f"},
    {"chunk_id": "c1", "chunk_type": "class", "name": "C", "content": "class C:",
     "file_path": "pkg/a.py", "start_line": 9, "end_line": 11, "qualified_name": "C"},
]
check("check_fix: missing fields rejected", not check_fix({"chunk_id": "m1"}, chunks)["ok"])
check("check_fix: unknown chunk_id rejected", not check_fix({"chunk_id": "zz", "new_code": "def f(): pass"}, chunks)["ok"])
check("check_fix: class chunk rejected",
      not check_fix({"chunk_id": "c1", "new_code": "def C(): pass"}, chunks)["ok"])
check("check_fix: no-op fix rejected",
      not check_fix({"chunk_id": "m1", "new_code": "def f(self):\n    return 1"}, chunks)["ok"])
check("check_fix: valid fix accepted",
      check_fix({"chunk_id": "m1", "new_code": "def f(self):\n    return 2"}, chunks)["ok"])

diff = build_diff_preview(chunks[0], "def f(self):\n    return 2")
check("diff: hunk header shifted to real file lines", "@@ -10,2 +10,2 @@" in diff, diff)
check("diff: shows removed and added lines",
      "-        return 1" in diff and "+        return 2" in diff, diff)

# =============== Part 2: real graph + real hybrid retrieval ===============
from rich.console import Console  # noqa: E402

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
BAD_SYNTAX = "def acquire(self, timeout=5.0)\n    return self\n"
WRONG_NAME = "def other(self):\n    return 1\n"
GOOD = (
    "def acquire(self, timeout=5.0):\n"
    "    deadline = time.monotonic() + timeout\n"
    "    while self.in_use >= self.max_size:\n"
    "        if time.monotonic() > deadline:\n"
    "            raise DatabaseTimeout(ERR_DB_TIMEOUT)\n"
    "        time.sleep(0.01)\n"
    "    self.in_use += 1\n"
    "    return self\n"
)


class ScriptedLLM:
    """Deterministic stand-in: replays a fixed list of drafts for ConnectionPool.acquire."""

    def __init__(self, drafts: list[str]):
        self.drafts = drafts
        self.calls = 0
        self.feedback_seen: list = []

    def analyze(self, log: str) -> dict:
        return {"hypothesis": "Connection pool exhausted; acquire() times out.",
                "search_query": "ERR_DB_TIMEOUT"}

    def draft_fix(self, log, hypothesis, chunks, feedback):
        self.feedback_seen.append(feedback)
        code = self.drafts[min(self.calls, len(self.drafts) - 1)]
        self.calls += 1
        target = next(c for c in chunks if c["qualified_name"] == "ConnectionPool.acquire")
        return {"hypothesis": hypothesis, "chunk_id": target["chunk_id"], "new_code": code}


def run(graph):
    state = initial_state(LOG)
    sequence: list[str] = []
    for step in graph.stream(state, stream_mode="updates"):
        for node, update in step.items():
            sequence.append(node)
            state.update(update)
    return sequence, state


console = Console()
console.print("Loading models (embedder + reranker)...")
embedder, reranker = Embedder(), Reranker()
conn = get_connection()
try:
    init_schema(conn)
    init_vector_index(conn)
    replace_repo_chunks(conn, REPO_NAME, ingest_repo(ROOT / "sample_repo").chunks)
    embed_missing_chunks(conn, REPO_NAME, embedder)
    retriever = HybridRetriever(conn, REPO_NAME, embedder, reranker)

    # ---- retrieval tool ----
    got = retrieve_code_context(retriever, "ERR_DB_TIMEOUT")
    check("retrieve tool returns 3 chunk dicts", len(got) == 3 and all("content" in c for c in got))
    check("retrieve tool finds ConnectionPool.acquire",
          "ConnectionPool.acquire" in [c["qualified_name"] for c in got],
          str([c["qualified_name"] for c in got]))

    # ---- Case A: invalid draft, then a valid one ----
    llm_a = ScriptedLLM([BAD_SYNTAX, GOOD])
    seq, final = run(build_graph(llm_a, retriever))
    console.print(f"Case A transitions: {' -> '.join(seq)}")
    check("A: node sequence includes one retry",
          seq == ["analyze_log", "retrieve_code", "draft_fix", "verify",
                  "draft_fix", "verify", "output_diff"], str(seq))
    check("A: status passed after 2 attempts",
          final["evaluation_status"] == "passed" and final["attempts"] == 2, str(final["attempts"]))
    check("A: second draft received verifier feedback",
          llm_a.feedback_seen[0] is None and str(llm_a.feedback_seen[1]).startswith("SyntaxError"),
          str(llm_a.feedback_seen))
    check("A: feedback cleared on pass", final["verification_feedback"] is None)
    console.print(final["diff_preview"])
    check("A: diff has real hunk header and the change",
          "@@ -19,4 +19,4 @@" in final["diff_preview"]
          and "-    def acquire(self, timeout=1.0):" in final["diff_preview"]
          and "+    def acquire(self, timeout=5.0):" in final["diff_preview"], final["diff_preview"])
    check("A: diff targets shop/db.py", "--- a/shop/db.py" in final["diff_preview"])

    # ---- Case B: every draft invalid -> exhausts attempts ----
    llm_b = ScriptedLLM([WRONG_NAME])
    seq, final = run(build_graph(llm_b, retriever, max_attempts=3))
    console.print(f"Case B transitions: {' -> '.join(seq)}")
    check("B: three draft/verify rounds then stop",
          seq == ["analyze_log", "retrieve_code"] + ["draft_fix", "verify"] * 3, str(seq))
    check("B: status failed, attempts 3, no diff",
          final["evaluation_status"] == "failed" and final["attempts"] == 3
          and final["diff_preview"] == "")
    check("B: final feedback names the problem", "must remain 'acquire'" in (final["verification_feedback"] or ""))
finally:
    conn.close()

print("\nMODULE 5A: ALL CHECKS PASSED" if not failures else f"\nMODULE 5A: FAILED -> {failures}")
sys.exit(1 if failures else 0)