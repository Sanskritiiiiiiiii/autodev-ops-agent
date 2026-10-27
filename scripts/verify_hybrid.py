"""Module 3C verification: RRF + cross-encoder rerank (final Module 3 check)."""
import math
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from rich.console import Console  # noqa: E402
from rich.table import Table  # noqa: E402

from app.db.chunk_store import init_schema, init_vector_index, replace_repo_chunks  # noqa: E402
from app.db.connection import get_connection  # noqa: E402
from app.ingestion.ast_chunker import ingest_repo  # noqa: E402
from app.search.dense_search import embed_missing_chunks  # noqa: E402
from app.search.embedder import Embedder  # noqa: E402
from app.search.hybrid import HybridRetriever, rrf_fuse  # noqa: E402
from app.search.reranker import Reranker  # noqa: E402

REPO_NAME = "sample_repo"
console = Console()
failures: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    print(f"[{'PASS' if cond else 'FAIL'}] {name}" + (f"  {detail}" if detail and not cond else ""))
    if not cond:
        failures.append(name)


def show(query: str, hits) -> None:
    t = Table(title=f"Hybrid top results for: {query}")
    for col in ("Rank", "Qualified name", "BM25 rank", "Dense rank", "RRF", "Rerank"):
        t.add_column(col)
    for rank, h in enumerate(hits, 1):
        t.add_row(str(rank), h.chunk["qualified_name"], str(h.bm25_rank), str(h.dense_rank),
                  f"{h.rrf_score:.5f}",
                  "-" if h.rerank_score is None else f"{h.rerank_score:.4f}")
    console.print(t)


# ---- 1. RRF unit test (pure function) ----
fused = rrf_fuse([["a", "b", "c"], ["b", "c", "a", "d"]])
check("RRF ordering is b, a, c, d", [i for i, _ in fused] == ["b", "a", "c", "d"], str(fused))
check("RRF score math for 'a' = 1/61 + 1/63", abs(dict(fused)["a"] - (1 / 61 + 1 / 63)) < 1e-12)

# ---- 2. Full pipeline ----
console.print("Loading embedding model and reranker (reranker download ~90 MB on first run)...")
embedder = Embedder()
reranker = Reranker()

conn = get_connection()
try:
    init_schema(conn)
    init_vector_index(conn)
    replace_repo_chunks(conn, REPO_NAME, ingest_repo(ROOT / "sample_repo").chunks)
    embed_missing_chunks(conn, REPO_NAME, embedder)

    retriever = HybridRetriever(conn, REPO_NAME, embedder, reranker)

    cases = [
        ("ERR_DB_TIMEOUT", "ConnectionPool.acquire"),
        ("normalize_id", "OrderService.normalize_id"),
        ("database connection pool times out", "ConnectionPool.acquire"),
        ("cancel an existing order", "OrderService.cancel"),
    ]
    for query, expected in cases:
        fused_hits = retriever.fuse(query)
        fused_names = [h.chunk["qualified_name"] for h in fused_hits]
        hits = retriever.search(query, top_k=3)
        show(query, hits)
        final_names = [h.chunk["qualified_name"] for h in hits]

        check(f"[{query}] returns exactly 3 hits", len(hits) == 3, str(final_names))
        check(f"[{query}] RRF top-5 contains {expected}", expected in fused_names[:5], str(fused_names[:5]))
        check(f"[{query}] final top-3 contains {expected}", expected in final_names, str(final_names))
        check(f"[{query}] rerank scores finite and sorted",
              all(math.isfinite(h.rerank_score) for h in hits)
              and [h.rerank_score for h in hits] == sorted((h.rerank_score for h in hits), reverse=True))

    # determinism
    a = [h.chunk["chunk_id"] for h in retriever.search("ERR_DB_TIMEOUT", 3)]
    b = [h.chunk["chunk_id"] for h in retriever.search("ERR_DB_TIMEOUT", 3)]
    check("same query gives same ranking twice", a == b)

    # fallback: BM25 finds nothing, dense still answers
    fb = retriever.search("zzz_nonexistent", top_k=3)
    check("unknown term still returns 3 hits via dense fallback",
          len(fb) == 3 and all(h.bm25_rank is None for h in fb))
finally:
    conn.close()

print("\nMODULE 3C: ALL CHECKS PASSED" if not failures else f"\nMODULE 3C: FAILED -> {failures}")
sys.exit(1 if failures else 0)