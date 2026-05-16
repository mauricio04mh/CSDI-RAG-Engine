from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

from src.bm25.pipeline.bm25_retriever import BM25Retriever
from src.hybrid.fusion.rrf import reciprocal_rank_fusion
from src.vector_retrieval.pipeline.vector_retriever import VectorRetriever

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class HybridResult:
    doc_id: str
    score: float
    found_by: frozenset[str] = field(default_factory=frozenset)


class HybridRetriever:
    """Combines BM25 lexical search and dense vector search via Reciprocal Rank Fusion.

    Both retrievers run concurrently. Each produces a ranked list of doc_ids.
    RRF merges the two rankings into one without needing score normalization,
    since it only uses rank position.
    """

    def __init__(
        self,
        bm25_retriever: BM25Retriever,
        vector_retriever: VectorRetriever,
        fetch_k: int = 50,
        bm25_weight: float = 0.3,
        vector_weight: float = 0.7,
    ) -> None:
        self._bm25 = bm25_retriever
        self._vector = vector_retriever
        self.fetch_k = fetch_k
        self._bm25_weight = bm25_weight
        self._vector_weight = vector_weight

    def search(self, query: str, top_k: int, vector_query: str | None = None) -> list[HybridResult]:
        if not query.strip():
            raise ValueError("Query must not be empty.")

        vector_q = vector_query if vector_query is not None else query
        with ThreadPoolExecutor(max_workers=2) as executor:
            bm25_future = executor.submit(self._bm25.search, query, self.fetch_k)
            vector_future = executor.submit(self._vector.search, vector_q, self.fetch_k)
            bm25_results = bm25_future.result()
            vector_results = vector_future.result()

        bm25_ids = [r.doc_id for r in bm25_results]
        vector_ids = [r.doc_id for r in vector_results]
        bm25_set = set(bm25_ids)
        vector_set = set(vector_ids)

        fused = reciprocal_rank_fusion(
            [bm25_ids, vector_ids],
            weights=[self._bm25_weight, self._vector_weight],
        )

        logger.info(
            "hybrid_search_completed bm25_hits=%s vector_hits=%s fused=%s bm25_w=%.2f vector_w=%.2f",
            len(bm25_ids),
            len(vector_ids),
            len(fused),
            self._bm25_weight,
            self._vector_weight,
        )

        results = []
        for doc_id, score in fused[:top_k]:
            found_by: set[str] = set()
            if doc_id in bm25_set:
                found_by.add("bm25")
            if doc_id in vector_set:
                found_by.add("vector")
            results.append(HybridResult(doc_id=doc_id, score=score, found_by=frozenset(found_by)))
        return results

    def update_weights(self, bm25_weight: float, vector_weight: float) -> None:
        """Hot-update the RRF fusion weights without restarting."""
        self._bm25_weight = bm25_weight
        self._vector_weight = vector_weight
        logger.info(
            "hybrid_weights_updated bm25_weight=%.2f vector_weight=%.2f",
            bm25_weight,
            vector_weight,
        )
