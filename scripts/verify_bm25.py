"""Module 3A verification: schema + chunk store + BM25 sparse index."""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from rich.console import Console  # noqa: E402
from rich.table import Table  # noqa: E402

from app.db.chunk_store import fetch_chunks, init_schema, replace_repo_chunks  # noqa: E402
from app.db.connection import get_connection  # noqa: E402
from app.ingestion.ast_chunker import ingest_repo  # noqa: E402
from app.search.bm25_index import BM25Index  # noqa: E402
from app.search.tokenizer import tokenize  # noqa: E402

REPO_NAME = "sample_repo"
console = Console()
failures: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    print(f"[{'PASS' if cond else 'FAIL'}] {name}" + (f"  {detail}" if detail and not cond else ""))
    if not cond:
        failures.append(name)


# ---- tokenizer ----
check("tokenize ERR_DB_TIMEOUT",
      tokenize("ERR_DB_TIMEOUT") == ["err_db_timeout", "err", "db", "timeout"],
      str(tokenize("ERR_DB_TIMEOUT")))
check("tokenize DatabaseTimeout",
      tokenize("DatabaseTimeout") == ["databasetimeout", "database", "timeout"],
      str(tokenize("DatabaseTimeout")))
check("tokenize plain word", tokenize("self") == ["self"])

# ---- database ----
conn = get_connection()
try:
    init_schema(conn)
    result = ingest_repo(ROOT / "sample_repo")
    n1 = replace_repo_chunks(conn, REPO_NAME, result.chunks)
    n2 = replace_repo_chunks(conn, REPO_NAME, result.chunks)  # idempotency
    rows = fetch_chunks(conn, REPO_NAME)

    with conn.cursor() as cur:
        cur.execute("SELECT format_type(atttypid, atttypmod) FROM pg_attribute "
                    "WHERE attrelid = 'code_chunks'::regclass AND attname = 'embedding'")
        emb_type = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM code_chunks WHERE repo_name = %s", (REPO_NAME,))
        db_count = cur.fetchone()[0]
finally:
    conn.close()

check("embedding column is vector(768)", emb_type == "vector(768)", emb_type)
check("inserted 12 chunks", n1 == 12 and n2 == 12, f"{n1}, {n2}")
check("re-ingest is idempotent (12 rows in DB)", db_count == 12 and len(rows) == 12, str(db_count))

# ---- BM25 ----
index = BM25Index(rows)
queries = ["ERR_DB_TIMEOUT", "normalize_id", "zzz_nonexistent"]
results = {q: index.search(q, top_k=3) for q in queries}

for q in queries:
    t = Table(title=f"BM25 top results for: {q}")
    for col in ("Rank", "Qualified name", "File", "Score"):
        t.add_column(col)
    for rank, (row, score) in enumerate(results[q], 1):
        t.add_row(str(rank), row["qualified_name"], row["file_path"], f"{score:.3f}")
    console.print(t)

check("ERR_DB_TIMEOUT -> ConnectionPool.acquire is top-1",
      bool(results["ERR_DB_TIMEOUT"]) and results["ERR_DB_TIMEOUT"][0][0]["qualified_name"] == "ConnectionPool.acquire")
check("normalize_id -> OrderService.normalize_id is top-1",
      bool(results["normalize_id"]) and results["normalize_id"][0][0]["qualified_name"] == "OrderService.normalize_id")
check("unknown term returns no results", results["zzz_nonexistent"] == [])
check("BM25 deterministic across runs",
      [r[0]["chunk_id"] for r in BM25Index(rows).search("ERR_DB_TIMEOUT", 3)]
      == [r[0]["chunk_id"] for r in results["ERR_DB_TIMEOUT"]])

print("\nMODULE 3A: ALL CHECKS PASSED" if not failures else f"\nMODULE 3A: FAILED -> {failures}")
sys.exit(1 if failures else 0)