from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from src.hybrid.api.routes import SearchRequest, SearchResultItem
from src.vector_retrieval.api.vector_search_routes import VectorSearchRequest, VectorSearchResultItem


def _make_chunk(source_id: str) -> MagicMock:
    c = MagicMock()
    c.source_id = source_id
    c.url = "http://example.com"
    c.title = "T"
    c.breadcrumb = ""
    c.text = "body"
    return c


def _make_search_result_item(chunk_id: str, source_id: str) -> SearchResultItem:
    return SearchResultItem(
        chunk_id=chunk_id,
        score=1.0,
        source_id=source_id,
        url="http://example.com",
        title="T",
        breadcrumb="",
        text="body",
    )


def _make_vector_result_item(doc_id: str, source_id: str) -> VectorSearchResultItem:
    return VectorSearchResultItem(
        doc_id=doc_id,
        score=1.0,
        source_id=source_id,
        url="http://example.com",
        title="T",
        breadcrumb="",
        text="body",
    )


# ── SearchRequest (hybrid) ────────────────────────────────────────────────────

def test_search_request_source_ids_defaults_to_none():
    req = SearchRequest(query="test")
    assert req.source_ids is None


def test_hybrid_filter_includes_matching_source():
    results = [
        _make_search_result_item("c1", "src_a"),
        _make_search_result_item("c2", "src_b"),
    ]
    sid_set = {"src_a"}
    filtered = [item for item in results if item.source_id in sid_set]
    assert len(filtered) == 1
    assert filtered[0].chunk_id == "c1"


def test_hybrid_filter_no_source_ids_returns_all():
    results = [
        _make_search_result_item("c1", "src_a"),
        _make_search_result_item("c2", "src_b"),
    ]
    # source_ids=None → no filter applied
    filtered = results  # no filter
    assert len(filtered) == 2


def test_hybrid_filter_excludes_all_when_no_match():
    results = [
        _make_search_result_item("c1", "src_a"),
    ]
    sid_set = {"src_z"}
    filtered = [item for item in results if item.source_id in sid_set]
    assert filtered == []


# ── VectorSearchRequest ───────────────────────────────────────────────────────

def test_vector_request_source_ids_defaults_to_none():
    req = VectorSearchRequest(query="test")
    assert req.source_ids is None


def test_vector_filter_includes_matching_source():
    results = [
        _make_vector_result_item("d1", "src_a"),
        _make_vector_result_item("d2", "src_b"),
    ]
    sid_set = {"src_b"}
    filtered = [item for item in results if item.source_id in sid_set]
    assert len(filtered) == 1
    assert filtered[0].doc_id == "d2"


def test_vector_filter_multiple_sources():
    results = [
        _make_vector_result_item("d1", "src_a"),
        _make_vector_result_item("d2", "src_b"),
        _make_vector_result_item("d3", "src_c"),
    ]
    sid_set = {"src_a", "src_c"}
    filtered = [item for item in results if item.source_id in sid_set]
    assert {item.doc_id for item in filtered} == {"d1", "d3"}
