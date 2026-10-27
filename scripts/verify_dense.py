"""Module 3B verification: embeddings + pgvector dense search."""
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
from app.search.dense_search import dense_search, embed_missing_chunks  # noqa: E402
from app.search.embedder import Embedder  # noqa: E402

REPO_NAME = "sample_repo"
console = Console()
failures: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    print(f"[{'PASS' if cond else 'FAIL'}] {name}" + (f"  {detail}" if detail and not cond else ""))
    if not cond:
        failures.append(name)


def show(query: str, results) -> None:
    t = Table(title=f"Dense top results for: {query}")
    for col in ("Rank", "Qualified name", "File", "Cosine"):
        t.add_column(col)
    for rank, (row, score) in enumerate(results, 1):
        t.add_row(str(rank), row["qualified_name"], row["file_path"], f"{score:.4f}")
    console.print(t)


console.print("Loading embedding model (first run downloads ~440 MB)...")
embedder = Embedder()
check("model dimension is 768", embedder.model.get_sentence_embedding_dimension() == 768)

vecs = embedder.embed_documents(["def f(): pass"])
norm = math.sqrt(sum(x * x for x in vecs[0]))
check("document vectors are L2-normalized", abs(norm - 1.0) < 1e-3, f"norm={norm}")

conn = get_connection()
try:
    init_schema(conn)
    init_vector_index(conn)
    replace_repo_chunks(conn, REPO_NAME, ingest_repo(ROOT / "sample_repo").chunks)

    n1 = embed_missing_chunks(conn, REPO_NAME, embedder)
    n2 = embed_missing_chunks(conn, REPO_NAME, embedder)
    check("embedded all 12 chunks", n1 == 12, f"got {n1}")
    check("second embed pass is a no-op", n2 == 0, f"got {n2}")

    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM code_chunks WHERE repo_name=%s AND vector_dims(embedding)=768",
                    (REPO_NAME,))
        dims_ok = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM pg_indexes WHERE indexname='idx_code_chunks_embedding_hnsw'")
        idx = cur.fetchone()[0]
    check("12 rows stored with 768-dim vectors", dims_ok == 12, str(dims_ok))
    check("HNSW index exists", idx == 1)

    # Semantic queries: expected chunk must appear in the top 3 of 12.
    semantic = [
        ("raise an error when the database connection pool times out", "ConnectionPool.acquire"),
        ("convert a raw order id value into an integer", "OrderService.normalize_id"),
        ("cancel an existing order in the database", "OrderService.cancel"),
    ]
    for query, expected in semantic:
        res = dense_search(conn, REPO_NAME, embedder, query, top_k=3)
        show(query, res)
        names = [r["qualified_name"] for r, _ in res]
        check(f"'{expected}' in top 3 for: {query}", expected in names, f"got {names}")
        scores = [s for _, s in res]
        check("  scores sorted descending", scores == sorted(scores, reverse=True))

    # Informational only (no assertion): how dense search handles an exact identifier.
    info = dense_search(conn, REPO_NAME, embedder, "ERR_DB_TIMEOUT", top_k=3)
    show("ERR_DB_TIMEOUT  (informational, not asserted)", info)
finally:
    conn.close()

print("\nMODULE 3B: ALL CHECKS PASSED" if not failures else f"\nMODULE 3B: FAILED -> {failures}")
sys.exit(1 if failures else 0)