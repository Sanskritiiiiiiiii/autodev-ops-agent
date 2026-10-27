from sentence_transformers import CrossEncoder

RERANK_MODEL = "cross-encoder/ms-marco-MiniLM-L-6-v2"


class Reranker:
    def __init__(self, model_name: str = RERANK_MODEL):
        self.model = CrossEncoder(model_name)

    def score(self, pairs: list[tuple[str, str]]) -> list[float]:
        """Relevance score for each (query, passage) pair; higher is better."""
        if not pairs:
            return []
        scores = self.model.predict([list(p) for p in pairs], show_progress_bar=False)
        return [float(s) for s in scores]