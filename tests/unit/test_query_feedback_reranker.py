from __future__ import annotations

from src.query_feedback.reranker import FeedbackReranker
from src.query_feedback.schemas import FeedbackMatch, SearchResultItem


def _result(chunk_id: str, score: float) -> SearchResultItem:
    return SearchResultItem(
        chunk_id=chunk_id,
        score=score,
        source_id="python_docs",
        url=f"https://example.test/{chunk_id}",
        title=f"Title {chunk_id}",
        breadcrumb="Docs",
        text=f"Text {chunk_id}",
    )


def test_reranker_increases_score_for_high_relevance_feedback():
    reranker = FeedbackReranker()
    results = [_result("doc-1", 1.0)]
    feedback = {
        "doc-1": FeedbackMatch(
            chunk_id="doc-1",
            relevance=3,
            source_query="How do decorators work?",
            normalized_source_query="how do decorators work?",
            query_similarity=1.0,
            match_type="exact",
        )
    }

    reranked = reranker.rerank(results, feedback, top_k=1)

    assert reranked[0].adjusted_score > reranked[0].original_score


def test_reranker_decreases_score_for_negative_feedback():
    reranker = FeedbackReranker()
    results = [_result("doc-1", 1.0)]
    feedback = {
        "doc-1": FeedbackMatch(
            chunk_id="doc-1",
            relevance=0,
            source_query="How do decorators work?",
            normalized_source_query="how do decorators work?",
            query_similarity=1.0,
            match_type="exact",
        )
    }

    reranked = reranker.rerank(results, feedback, top_k=1)

    assert reranked[0].adjusted_score < reranked[0].original_score


def test_reranker_keeps_original_score_without_feedback():
    reranker = FeedbackReranker()
    results = [_result("doc-1", 1.0)]

    reranked = reranker.rerank(results, {}, top_k=1)

    assert reranked[0].adjusted_score == reranked[0].original_score
    assert reranked[0].feedback_applied is False


def test_reranker_orders_results_by_adjusted_score():
    reranker = FeedbackReranker()
    results = [_result("doc-1", 0.8), _result("doc-2", 0.7)]
    feedback = {
        "doc-2": FeedbackMatch(
            chunk_id="doc-2",
            relevance=3,
            source_query="How do decorators work?",
            normalized_source_query="how do decorators work?",
            query_similarity=1.0,
            match_type="exact",
        )
    }

    reranked = reranker.rerank(results, feedback, top_k=2)

    assert [item.chunk_id for item in reranked] == ["doc-2", "doc-1"]


def test_reranker_respects_top_k():
    reranker = FeedbackReranker()
    results = [_result("doc-1", 0.8), _result("doc-2", 0.7), _result("doc-3", 0.6)]

    reranked = reranker.rerank(results, {}, top_k=2)

    assert len(reranked) == 2


def test_reranker_preserves_chunk_metadata():
    reranker = FeedbackReranker()
    results = [_result("doc-1", 1.0)]

    reranked = reranker.rerank(results, {}, top_k=1)

    assert reranked[0].source_id == "python_docs"
    assert reranked[0].url == "https://example.test/doc-1"
    assert reranked[0].title == "Title doc-1"
    assert reranked[0].breadcrumb == "Docs"
    assert reranked[0].text == "Text doc-1"


def test_reranker_populates_feedback_fields_when_feedback_applies():
    reranker = FeedbackReranker()
    results = [_result("doc-1", 1.0)]
    feedback = {
        "doc-1": FeedbackMatch(
            chunk_id="doc-1",
            relevance=2,
            source_query="Explain Python decorators",
            normalized_source_query="explain python decorators",
            query_similarity=0.95,
            match_type="semantic",
        )
    }

    reranked = reranker.rerank(results, feedback, top_k=1)

    assert reranked[0].feedback_applied is True
    assert reranked[0].feedback_relevance == 2
    assert reranked[0].feedback_source_query == "Explain Python decorators"
    assert reranked[0].feedback_query_similarity == 0.95
    assert reranked[0].feedback_match_type == "semantic"
