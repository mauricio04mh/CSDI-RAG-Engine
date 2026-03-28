from __future__ import annotations

import logging
import threading
from dataclasses import dataclass

from src.vector_indexing.encoder.embedding_model import EmbeddingModel
from src.vector_indexing.index.faiss_index import FaissIndex
from src.vector_indexing.index.vector_store import VectorStore

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class VectorResult:
    doc_id: str
    score: float


class VectorRetriever:
    """Runs dense ANN queries against the shared in-memory FAISS index.

    Receives the EmbeddingModel, FaissIndex, VectorStore, and their shared
    lock from VectorIndexBuilder — no separate loading needed.
    """

    def __init__(
        self,
        embedding_model: EmbeddingModel,
        faiss_index: FaissIndex,
        vector_store: VectorStore,
        lock: threading.RLock,
    ) -> None:
        self._embedding_model = embedding_model
        self._faiss_index = faiss_index
        self._vector_store = vector_store
        self._lock = lock

    def search(self, query: str, top_k: int) -> list[VectorResult]:
        if not query.strip():
            raise ValueError("Query must not be empty.")

        query_vector = self._embedding_model.encode_one(query)

        with self._lock:
            scores, vector_ids = self._faiss_index.search(query_vector, top_k)
            results: list[VectorResult] = []
            for score, vid in zip(scores[0], vector_ids[0]):
                if vid < 0:
                    continue  # FAISS pads unfilled slots with -1
                doc_id = self._vector_store.get_doc_id(int(vid))
                if doc_id is not None:
                    results.append(VectorResult(doc_id=doc_id, score=float(score)))

        logger.info("vector_search_completed query_len=%s results=%s", len(query), len(results))
        return results
