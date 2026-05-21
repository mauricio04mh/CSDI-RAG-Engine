"""Schemas for the Query Feedback module."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class ExpansionResult:
    original_query: str
    expanded_query: str
    expansion_terms: list[str]
    method: str
    feedback_documents_used: int
