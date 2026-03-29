from __future__ import annotations

import logging
from dataclasses import dataclass

from sentence_transformers import CrossEncoder

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class RerankResult:
    doc_id: str
    score: float


class CrossEncoderReranker:
    """Second-stage re-ranker using a cross-encoder model.

    Takes (query, passage) pairs and scores them jointly, producing much
    more accurate relevance scores than bi-encoder (vector) similarity.

    Typical usage:
        1. First-stage retrieval returns top-N candidates (e.g. 30).
        2. Re-ranker scores all N pairs and returns the top-k.
    """

    def __init__(self, model_name: str) -> None:
        logger.info("reranker_loading model=%s", model_name)
        self._model = CrossEncoder(model_name)
        logger.info("reranker_ready model=%s", model_name)

    def rerank(
        self,
        query: str,
        candidates: list[tuple[str, str]],
        top_k: int,
    ) -> list[RerankResult]:
        """Re-rank candidates and return the top_k.

        Args:
            query: The user query.
            candidates: List of (doc_id, text) pairs.
            top_k: How many results to return.

        Returns:
            List of RerankResult sorted by score descending.
        """
        if not candidates:
            return []

        pairs = [(query, text) for _, text in candidates]
        scores = self._model.predict(pairs)

        ranked = sorted(
            zip([doc_id for doc_id, _ in candidates], scores),
            key=lambda x: x[1],
            reverse=True,
        )

        logger.debug(
            "rerank_done candidates=%s top_k=%s top_score=%.4f",
            len(candidates),
            top_k,
            ranked[0][1] if ranked else 0.0,
        )

        return [RerankResult(doc_id=doc_id, score=float(score)) for doc_id, score in ranked[:top_k]]
