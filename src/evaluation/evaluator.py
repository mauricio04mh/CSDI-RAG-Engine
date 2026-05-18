"""Evaluation runner scaffold.

This module will load evaluation queries and relevance judgments, execute retrieval
strategies, calculate IR metrics, and produce comparison reports.
"""

from __future__ import annotations

from src.evaluation.metrics import (
    f1_at_k,
    ndcg_at_k,
    precision_at_k,
    recall_at_k,
    reciprocal_rank,
)

MetricResults = dict[str, float]
QueryRankings = dict[str, list[str]]
QueryRelevanceJudgments = dict[str, dict[str, int | float]]

METRIC_NAMES = (
    "precision_at_k",
    "recall_at_k",
    "f1_at_k",
    "reciprocal_rank",
    "ndcg_at_k",
)


def evaluate_query(
    retrieved_ids: list[str],
    relevance_judgments: dict[str, int | float],
    k: int = 5,
) -> MetricResults:
    """Evaluate one ranked result list against relevance judgments."""
    return {
        "precision_at_k": precision_at_k(retrieved_ids, relevance_judgments, k),
        "recall_at_k": recall_at_k(retrieved_ids, relevance_judgments, k),
        "f1_at_k": f1_at_k(retrieved_ids, relevance_judgments, k),
        "reciprocal_rank": reciprocal_rank(retrieved_ids, relevance_judgments),
        "ndcg_at_k": ndcg_at_k(retrieved_ids, relevance_judgments, k),
    }


def evaluate_strategy(
    rankings_by_query: QueryRankings,
    qrels: QueryRelevanceJudgments,
    k: int = 5,
) -> dict[str, int | MetricResults | dict[str, MetricResults]]:
    """Evaluate a strategy's ranked results and average metrics by query."""
    per_query: dict[str, MetricResults] = {}

    for query_id, retrieved_ids in rankings_by_query.items():
        relevance_judgments = qrels.get(query_id)
        if relevance_judgments is None:
            continue

        per_query[query_id] = evaluate_query(retrieved_ids, relevance_judgments, k)

    return {
        "k": k,
        "evaluated_queries": len(per_query),
        "per_query": per_query,
        "averages": _average_metrics(per_query),
    }


def _average_metrics(per_query: dict[str, MetricResults]) -> MetricResults:
    if not per_query:
        return {metric_name: 0.0 for metric_name in METRIC_NAMES}

    evaluated_queries = len(per_query)
    return {
        metric_name: sum(metrics[metric_name] for metrics in per_query.values())
        / evaluated_queries
        for metric_name in METRIC_NAMES
    }
