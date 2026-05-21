"""Data structures for the evaluation module."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class EvaluationQuery:
    """One query used by the evaluation dataset."""

    id: str
    query: str
    source_ids: list[str] | None = None


@dataclass(slots=True)
class EvaluationResult:
    """Aggregated metric result for one retrieval strategy."""

    strategy: str
    precision_at_k: float
    recall_at_k: float
    f1_at_k: float
    mrr: float
    ndcg_at_k: float
