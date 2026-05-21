from __future__ import annotations

import sys
from dataclasses import dataclass
from types import SimpleNamespace

import pytest

if "snowballstemmer" not in sys.modules:
    def _stem_word(token: str) -> str:
        for suffix in ("ators", "ator", "ated", "ers", "er", "ing", "ies", "ied", "ed", "es", "s"):
            if token.endswith(suffix) and len(token) > len(suffix) + 1:
                if suffix == "ies":
                    return f"{token[:-3]}y"
                return token[: -len(suffix)]
        return token

    sys.modules["snowballstemmer"] = SimpleNamespace(
        stemmer=lambda _language: SimpleNamespace(stemWord=_stem_word)
    )

from src.query_feedback.expansion import QueryExpansionService


@dataclass(slots=True)
class FakeChunk:
    chunk_id: str
    source_id: str
    title: str
    breadcrumb: str
    text: str


def test_expand_from_chunks_uses_terms_extracted_from_chunks():
    service = QueryExpansionService()
    chunks = [
        FakeChunk(
            chunk_id="c1",
            source_id="python_docs",
            title="Decorator closures and wrappers",
            breadcrumb="Python Functions Decorators",
            text="Closures help build wrappers around decorated functions.",
        ),
    ]

    result = service.expand_from_chunks("python decorator", chunks, max_expansion_terms=4)

    assert result.original_query == "python decorator"
    assert result.expanded_query.startswith("python decorator ")
    assert result.expansion_terms[:3] == ["closures", "wrappers", "functions"]
    assert "decorator" not in result.expansion_terms
    assert result.method == "pseudo_relevance_feedback"


def test_expand_from_chunks_avoids_terms_already_in_query():
    service = QueryExpansionService()
    chunks = [
        FakeChunk(
            chunk_id="c1",
            source_id="python_docs",
            title="Decorators in Python",
            breadcrumb="Python Decorators",
            text="Decorator syntax uses wrappers and closures.",
        ),
    ]

    result = service.expand_from_chunks("python decorators", chunks, max_expansion_terms=5)

    assert "python" not in result.expansion_terms
    assert "decorators" not in result.expansion_terms
    assert "wrappers" in result.expansion_terms


def test_expand_from_chunks_respects_max_expansion_terms():
    service = QueryExpansionService()
    chunks = [
        FakeChunk(
            chunk_id="c1",
            source_id="python_docs",
            title="Closures wrappers descriptors generators",
            breadcrumb="Functions modules classes methods",
            text="Decorated functions often rely on wrappers and closures.",
        ),
    ]

    result = service.expand_from_chunks("decorator", chunks, max_expansion_terms=3)

    assert len(result.expansion_terms) == 3


def test_expand_from_chunks_returns_original_query_when_no_usable_terms_exist():
    service = QueryExpansionService()
    chunks = [
        FakeChunk(
            chunk_id="c1",
            source_id="python_docs",
            title="Decorator decorators decorator",
            breadcrumb="Decorator",
            text="decorators decorator decorated",
        ),
    ]

    result = service.expand_from_chunks("decorator", chunks, max_expansion_terms=5)

    assert result.expansion_terms == []
    assert result.expanded_query == "decorator"


def test_expand_from_chunks_rejects_empty_query():
    service = QueryExpansionService()

    with pytest.raises(ValueError, match="query must not be empty"):
        service.expand_from_chunks("   ", [])


def test_expand_from_chunks_orders_ties_alphabetically():
    service = QueryExpansionService()
    chunks = [
        FakeChunk(
            chunk_id="c1",
            source_id="python_docs",
            title="banana apple cherry",
            breadcrumb="",
            text="",
        ),
    ]

    result = service.expand_from_chunks("decorator", chunks, max_expansion_terms=3)

    assert result.expansion_terms == ["apple", "banana", "cherry"]


def test_expand_from_chunks_reports_feedback_documents_used():
    service = QueryExpansionService()
    chunks = [
        FakeChunk("c1", "python_docs", "closures", "", "wrappers"),
        FakeChunk("c2", "python_docs", "descriptors", "", "generators"),
    ]

    result = service.expand_from_chunks("decorator", chunks, max_expansion_terms=2)

    assert result.feedback_documents_used == 2


def test_expand_from_chunks_avoids_duplicate_normalized_terms():
    service = QueryExpansionService()
    chunks = [
        FakeChunk(
            chunk_id="c1",
            source_id="python_docs",
            title="wrapper wrappers closures",
            breadcrumb="",
            text="",
        ),
    ]

    result = service.expand_from_chunks("decorator", chunks, max_expansion_terms=3)

    assert {"wrapper", "wrappers"} & set(result.expansion_terms)
    assert not {"wrapper", "wrappers"} <= set(result.expansion_terms)
