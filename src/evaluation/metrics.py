"""Core Information Retrieval evaluation metrics.

The functions in this module are intentionally pure and framework-independent.
They will be used to evaluate ranked retrieval results produced by BM25, vector
search, and hybrid search.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from math import log2

RelevanceJudgments = Mapping[str, int | float]
RankedDocumentIds = Sequence[str]


DEFAULT_RELEVANCE_THRESHOLD = 2.0


def precision_at_k(
    retrieved_ids: RankedDocumentIds,
    relevance_judgments: RelevanceJudgments,
    k: int,
    relevance_threshold: int | float = DEFAULT_RELEVANCE_THRESHOLD,
) -> float:
    """Return the fraction of top-k retrieved documents that are relevant."""
    if k <= 0:
        return 0.0

    relevant_retrieved = _count_relevant_at_k(
        retrieved_ids,
        relevance_judgments,
        k,
        relevance_threshold,
    )
    return relevant_retrieved / k


def recall_at_k(
    retrieved_ids: RankedDocumentIds,
    relevance_judgments: RelevanceJudgments,
    k: int,
    relevance_threshold: int | float = DEFAULT_RELEVANCE_THRESHOLD,
) -> float:
    """Return the fraction of all relevant documents found in the top-k results."""
    if k <= 0:
        return 0.0

    total_relevant = _count_relevant_documents(
        relevance_judgments,
        relevance_threshold,
    )
    if total_relevant == 0:
        return 0.0

    relevant_retrieved = _count_relevant_at_k(
        retrieved_ids,
        relevance_judgments,
        k,
        relevance_threshold,
    )
    return relevant_retrieved / total_relevant


def f1_at_k(
    retrieved_ids: RankedDocumentIds,
    relevance_judgments: RelevanceJudgments,
    k: int,
    relevance_threshold: int | float = DEFAULT_RELEVANCE_THRESHOLD,
) -> float:
    """Return the harmonic mean of precision@k and recall@k."""
    precision = precision_at_k(
        retrieved_ids,
        relevance_judgments,
        k,
        relevance_threshold,
    )
    recall = recall_at_k(
        retrieved_ids,
        relevance_judgments,
        k,
        relevance_threshold,
    )
    if precision + recall == 0:
        return 0.0

    return 2 * precision * recall / (precision + recall)


def reciprocal_rank(
    retrieved_ids: RankedDocumentIds,
    relevance_judgments: RelevanceJudgments,
    relevance_threshold: int | float = DEFAULT_RELEVANCE_THRESHOLD,
) -> float:
    """Return the inverse rank of the first relevant retrieved document."""
    seen: set[str] = set()
    rank = 0

    for document_id in retrieved_ids:
        if document_id in seen:
            continue

        seen.add(document_id)
        rank += 1

        if _is_relevant(document_id, relevance_judgments, relevance_threshold):
            return 1 / rank

    return 0.0


def dcg_at_k(
    retrieved_ids: RankedDocumentIds,
    relevance_judgments: RelevanceJudgments,
    k: int,
) -> float:
    """Return discounted cumulative gain at k using graded relevance values."""
    if k <= 0:
        return 0.0

    relevances = [
        float(relevance_judgments.get(document_id, 0.0))
        for document_id in _top_k_unique(retrieved_ids, k)
    ]
    return _dcg_from_relevances(relevances)


def ndcg_at_k(
    retrieved_ids: RankedDocumentIds,
    relevance_judgments: RelevanceJudgments,
    k: int,
) -> float:
    """Return normalized discounted cumulative gain at k."""
    if k <= 0:
        return 0.0

    dcg = dcg_at_k(retrieved_ids, relevance_judgments, k)
    ideal_relevances = sorted(
        (float(relevance) for relevance in relevance_judgments.values()),
        reverse=True,
    )[:k]
    ideal_dcg = _dcg_from_relevances(ideal_relevances)
    if ideal_dcg == 0:
        return 0.0

    return dcg / ideal_dcg


def _count_relevant_at_k(
    retrieved_ids: RankedDocumentIds,
    relevance_judgments: RelevanceJudgments,
    k: int,
    relevance_threshold: int | float,
) -> int:
    return sum(
        1
        for document_id in _top_k_unique(retrieved_ids, k)
        if _is_relevant(document_id, relevance_judgments, relevance_threshold)
    )


def _count_relevant_documents(
    relevance_judgments: RelevanceJudgments,
    relevance_threshold: int | float,
) -> int:
    return sum(
        1
        for relevance in relevance_judgments.values()
        if relevance >= relevance_threshold
    )


def _is_relevant(
    document_id: str,
    relevance_judgments: RelevanceJudgments,
    relevance_threshold: int | float,
) -> bool:
    return relevance_judgments.get(document_id, 0.0) >= relevance_threshold


def _top_k_unique(retrieved_ids: RankedDocumentIds, k: int) -> list[str]:
    top_ids: list[str] = []
    seen: set[str] = set()

    for document_id in retrieved_ids:
        if document_id in seen:
            continue

        seen.add(document_id)
        top_ids.append(document_id)

        if len(top_ids) == k:
            break

    return top_ids


def _dcg_from_relevances(relevances: Sequence[float]) -> float:
    """Calculate DCG from an ordered sequence of graded relevance scores."""
    return sum(
        _graded_gain(relevance) / log2(rank + 1)
        for rank, relevance in enumerate(relevances, start=1)
    )


def _graded_gain(relevance: float) -> float:
    return (2**relevance) - 1


__all__ = [
    "precision_at_k",
    "recall_at_k",
    "f1_at_k",
    "reciprocal_rank",
    "dcg_at_k",
    "ndcg_at_k",
]
