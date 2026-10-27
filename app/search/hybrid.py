from dataclasses import dataclass

from app.db.chunk_store import fetch_chunks
from app.search.bm25_index import BM25Index
from app.search.dense_search import dense_search
from app.search.embedder import Embedder, build_embedding_text
from app.search.reranker import Reranker

RRF_K = 60


@dataclass
class SearchHit:
    chunk: dict
    rrf_score: float
    bm25_rank: int | None       # 1-based rank in BM25 results, None if absent
    dense_rank: int | None      # 1-based rank in dense results, None if absent
    rerank_score: float | None = None


def rrf_fuse(rankings: list[list[str]], k: int = RRF_K) -> list[tuple[str, float]]:
    """Reciprocal Rank Fusion over ranked id lists. Ties broken by id."""
    scores: dict[str, float] = {}
    for ranking in rankings:
        for rank, item_id in enumerate(ranking, 1):
            scores[item_id] = scores.get(item_id, 0.0) + 1.0 / (k + rank)
    return sorted(scores.items(), key=lambda kv: (-kv[1], kv[0]))


class HybridRetriever:
    """BM25 + dense retrieval, RRF fusion, then cross-encoder rerank."""

    def __init__(self, conn, repo_name: str, embedder: Embedder, reranker: Reranker,
                 candidate_k: int = 20, rerank_k: int = 10):
        self.conn = conn
        self.repo_name = repo_name
        self.embedder = embedder
        self.reranker = reranker
        self.candidate_k = candidate_k
        self.rerank_k = rerank_k
        self.bm25 = BM25Index(fetch_chunks(conn, repo_name))

    def fuse(self, query: str) -> list[SearchHit]:
        """Stage 1: BM25 + dense candidates fused by RRF (no reranking)."""
        bm25_hits = self.bm25.search(query, top_k=self.candidate_k)
        dense_hits = dense_search(self.conn, self.repo_name, self.embedder,
                                  query, top_k=self.candidate_k)

        rows: dict[str, dict] = {}
        bm25_rank: dict[str, int] = {}
        dense_rank: dict[str, int] = {}
        for rank, (row, _) in enumerate(bm25_hits, 1):
            rows[row["chunk_id"]] = row
            bm25_rank[row["chunk_id"]] = rank
        for rank, (row, _) in enumerate(dense_hits, 1):
            rows[row["chunk_id"]] = row
            dense_rank[row["chunk_id"]] = rank

        fused = rrf_fuse([
            [r["chunk_id"] for r, _ in bm25_hits],
            [r["chunk_id"] for r, _ in dense_hits],
        ])
        return [
            SearchHit(chunk=rows[cid], rrf_score=score,
                      bm25_rank=bm25_rank.get(cid), dense_rank=dense_rank.get(cid))
            for cid, score in fused
        ]

    def search(self, query: str, top_k: int = 3) -> list[SearchHit]:
        """Stage 1 + stage 2: rerank the RRF shortlist with the cross-encoder."""
        shortlist = self.fuse(query)[: self.rerank_k]
        scores = self.reranker.score(
            [(query, build_embedding_text(h.chunk)) for h in shortlist]
        )
        for hit, score in zip(shortlist, scores):
            hit.rerank_score = score
        shortlist.sort(key=lambda h: (-h.rerank_score, h.chunk["chunk_id"]))
        return shortlist[:top_k]