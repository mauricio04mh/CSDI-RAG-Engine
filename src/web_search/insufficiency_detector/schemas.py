from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from src.web_search.insufficiency_detector.reasons import InsufficiencyReason


@dataclass(slots=True)
class RetrievedChunk:
    """One retrieved evidence item already enriched with metadata + text.

    This type is intentionally retrieval-agnostic: it does not assume BM25,
    dense vectors, FAISS, or any particular fusion strategy.
    """

    chunk_id: str
    text: str
    score: float | None = None
    source_id: str | None = None
    url: str | None = None
    title: str | None = None
    breadcrumb: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class InsufficiencyMetrics:
    num_results: int
    unique_urls: int
    top_score: float
    top_score_norm: float
    quantity_score: float
    coverage_score: float
    diversity_score: float
    answerability_score: float
    local_confidence: float
    relevant_results: int


@dataclass(slots=True)
class InsufficiencyDecision:
    needs_web_search: bool
    sufficiency_confidence: float
    reasons: list[InsufficiencyReason]
    metrics: InsufficiencyMetrics
