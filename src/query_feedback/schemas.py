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
