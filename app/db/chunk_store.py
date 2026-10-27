from psycopg2.extras import RealDictCursor, execute_values

from app.ingestion.models import CodeChunk

EMBEDDING_DIM = 768

SCHEMA_SQL = f"""
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS code_chunks (
    repo_name       TEXT        NOT NULL,
    chunk_id        TEXT        NOT NULL,
    file_path       TEXT        NOT NULL,
    chunk_type      TEXT        NOT NULL,
    name            TEXT        NOT NULL,
    qualified_name  TEXT        NOT NULL,
    class_name      TEXT,
    start_line      INTEGER     NOT NULL,
    end_line        INTEGER     NOT NULL,
    content         TEXT        NOT NULL,
    embedding       vector({EMBEDDING_DIM}),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (repo_name, chunk_id)
);
"""


def init_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
    conn.commit()


def replace_repo_chunks(conn, repo_name: str, chunks: list[CodeChunk]) -> int:
    """Delete all rows for repo_name, then insert chunks. Idempotent."""
    rows = [
        (repo_name, c.chunk_id, c.file_path, c.chunk_type, c.name,
         c.qualified_name, c.class_name, c.start_line, c.end_line, c.content)
        for c in chunks
    ]
    with conn.cursor() as cur:
        cur.execute("DELETE FROM code_chunks WHERE repo_name = %s", (repo_name,))
        if rows:
            execute_values(
                cur,
                "INSERT INTO code_chunks (repo_name, chunk_id, file_path, chunk_type, "
                "name, qualified_name, class_name, start_line, end_line, content) VALUES %s",
                rows,
            )
    conn.commit()
    return len(rows)


def fetch_chunks(conn, repo_name: str) -> list[dict]:
    """Return a repo's chunks (without embeddings) in deterministic order."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT chunk_id, file_path, chunk_type, name, qualified_name, class_name, "
            "start_line, end_line, content FROM code_chunks "
            "WHERE repo_name = %s ORDER BY file_path, start_line, chunk_id",
            (repo_name,),
        )
        return [dict(r) for r in cur.fetchall()]

def init_vector_index(conn) -> None:
    """HNSW index for cosine similarity on the embedding column."""
    with conn.cursor() as cur:
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_code_chunks_embedding_hnsw "
            "ON code_chunks USING hnsw (embedding vector_cosine_ops)"
        )
    conn.commit()


def to_pgvector(vec) -> str:
    """Format a sequence of floats as a pgvector text literal."""
    return "[" + ",".join(repr(float(x)) for x in vec) + "]"


def fetch_unembedded_chunks(conn, repo_name: str) -> list[dict]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT chunk_id, file_path, chunk_type, qualified_name, content "
            "FROM code_chunks WHERE repo_name = %s AND embedding IS NULL "
            "ORDER BY file_path, start_line, chunk_id",
            (repo_name,),
        )
        return [dict(r) for r in cur.fetchall()]


def save_embeddings(conn, repo_name: str, items: list[tuple[str, list[float]]]) -> int:
    """items: list of (chunk_id, embedding_vector)."""
    params = [(to_pgvector(vec), repo_name, cid) for cid, vec in items]
    with conn.cursor() as cur:
        cur.executemany(
            "UPDATE code_chunks SET embedding = %s::vector "
            "WHERE repo_name = %s AND chunk_id = %s",
            params,
        )
    conn.commit()
    return len(params)


def search_by_vector(conn, repo_name: str, query_vec, top_k: int = 5) -> list[tuple[dict, float]]:
    """Cosine-similarity search. Returns (row, similarity) pairs, best first."""
    qv = to_pgvector(query_vec)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT chunk_id, file_path, chunk_type, name, qualified_name, class_name, "
            "start_line, end_line, content, 1 - (embedding <=> %s::vector) AS score "
            "FROM code_chunks WHERE repo_name = %s AND embedding IS NOT NULL "
            "ORDER BY embedding <=> %s::vector LIMIT %s",
            (qv, repo_name, qv, top_k),
        )
        out = []
        for r in cur.fetchall():
            d = dict(r)
            score = float(d.pop("score"))
            out.append((d, score))
        return out