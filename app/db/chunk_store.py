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