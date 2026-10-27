from sentence_transformers import SentenceTransformer

from app.db.chunk_store import EMBEDDING_DIM

MODEL_NAME = "BAAI/bge-base-en-v1.5"
QUERY_INSTRUCTION = "Represent this sentence for searching relevant passages: "


def build_embedding_text(row: dict) -> str:
    """Text that represents a chunk to the embedding model."""
    return f"{row['file_path']}\n{row['chunk_type']} {row['qualified_name']}\n{row['content']}"


class Embedder:
    def __init__(self, model_name: str = MODEL_NAME):
        self.model = SentenceTransformer(model_name)
        dim = self.model.get_sentence_embedding_dimension()
        if dim != EMBEDDING_DIM:
            raise ValueError(f"Model dimension {dim} != schema dimension {EMBEDDING_DIM}")

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        vecs = self.model.encode(
            texts, batch_size=16, normalize_embeddings=True, show_progress_bar=False
        )
        return vecs.tolist()

    def embed_query(self, query: str) -> list[float]:
        vec = self.model.encode(
            [QUERY_INSTRUCTION + query], normalize_embeddings=True, show_progress_bar=False
        )[0]
        return vec.tolist()