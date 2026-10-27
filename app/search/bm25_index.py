from rank_bm25 import BM25Okapi

from app.search.tokenizer import tokenize


class BM25Index:
    """In-memory BM25 index built from chunk rows (dicts from fetch_chunks)."""

    def __init__(self, rows: list[dict]):
        if not rows:
            raise ValueError("Cannot build a BM25 index over zero chunks")
        self.rows = rows
        self._bm25 = BM25Okapi([tokenize(r["content"]) for r in rows])

    def search(self, query: str, top_k: int = 5) -> list[tuple[dict, float]]:
        scores = self._bm25.get_scores(tokenize(query))
        ranked = sorted(range(len(self.rows)), key=lambda i: (-scores[i], i))
        return [(self.rows[i], float(scores[i])) for i in ranked[:top_k] if scores[i] > 0]