"""Schemas for the Query Feedback module."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(slots=True)
class ExpansionResult:
    original_query: str
    expanded_query: str
    expansion_terms: list[str]
    method: str
    feedback_documents_used: int


@dataclass(slots=True)
class SearchResultItem:
    chunk_id: str
    score: float
    source_id: str
    url: str
    title: str
    breadcrumb: str
    text: str


@dataclass(slots=True)
class ExpandedSearchResult:
    original_query: str
    expanded_query: str
    expansion_terms: list[str]
    method: str
    strategy: str
    expansion_enabled: bool
    feedback_documents_used: int
    results: list[SearchResultItem]


@dataclass(slots=True)
class FeedbackRecord:
    id: int
    query: str
    normalized_query: str
    chunk_id: str
    source_id: str | None
    relevance: int
    notes: str | None
    session_id: str | None
    created_at: datetime
    updated_at: datetime | None


@dataclass(slots=True)
class FeedbackMatch:
    chunk_id: str
    relevance: int
    source_query: str
    normalized_source_query: str
    query_similarity: float
    match_type: str


@dataclass(slots=True)
class FeedbackAdjustedSearchResultItem:
    chunk_id: str
    original_score: float
    adjusted_score: float
    feedback_boost: float
    feedback_applied: bool
    feedback_relevance: int | None
    feedback_source_query: str | None
    feedback_query_similarity: float | None
    feedback_match_type: str | None
    source_id: str
    url: str
    title: str
    breadcrumb: str
    text: str


@dataclass(slots=True)
class FeedbackRerankResult:
    original_query: str
    expanded_query: str
    expansion_terms: list[str]
    method: str
    strategy: str
    expansion_enabled: bool
    feedback_enabled: bool
    semantic_feedback_enabled: bool
    semantic_similarity_threshold: float
    feedback_applied: bool
    feedback_items_used: int
    matched_feedback_queries: list[dict[str, str | float]]
    feedback_documents_used: int
    results: list[FeedbackAdjustedSearchResultItem]
