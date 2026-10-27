from app.db.chunk_store import fetch_unembedded_chunks, save_embeddings, search_by_vector
from app.search.embedder import Embedder, build_embedding_text


def embed_missing_chunks(conn, repo_name: str, embedder: Embedder, batch_size: int = 16) -> int:
    """Embed every chunk in repo_name that has no embedding yet. Returns count embedded."""
    rows = fetch_unembedded_chunks(conn, repo_name)
    total = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        vecs = embedder.embed_documents([build_embedding_text(r) for r in batch])
        total += save_embeddings(conn, repo_name, [(r["chunk_id"], v) for r, v in zip(batch, vecs)])
    return total


def dense_search(conn, repo_name: str, embedder: Embedder, query: str, top_k: int = 5):
    return search_by_vector(conn, repo_name, embedder.embed_query(query), top_k)