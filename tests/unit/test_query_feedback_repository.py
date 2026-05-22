from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from src.query_feedback.models import Base
from src.query_feedback.repositories.feedback_repository import FeedbackRepository, normalize_query


def _build_repository() -> FeedbackRepository:
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    return FeedbackRepository(engine)


def test_normalize_query_collapses_whitespace_and_lowercases_text():
    assert normalize_query("  How   do Decorators work? ") == "how do decorators work?"


def test_add_or_update_feedback_inserts_a_new_row():
    repository = _build_repository()

    record = repository.add_or_update_feedback(
        query="How do decorators work?",
        chunk_id="chunk-1",
        relevance=3,
        source_id="python_docs",
    )

    assert record.id == 1
    assert record.normalized_query == "how do decorators work?"
    assert record.chunk_id == "chunk-1"
    assert record.relevance == 3


def test_add_or_update_feedback_updates_existing_feedback_for_same_query_chunk_and_session():
    repository = _build_repository()
    first = repository.add_or_update_feedback(
        query="How do decorators work?",
        chunk_id="chunk-1",
        relevance=1,
        session_id="session-a",
    )

    second = repository.add_or_update_feedback(
        query="  how do   decorators work? ",
        chunk_id="chunk-1",
        relevance=3,
        notes="Updated",
        session_id="session-a",
    )

    assert second.id == first.id
    assert second.relevance == 3
    assert second.notes == "Updated"


def test_add_or_update_feedback_treats_session_none_as_same_nullable_session():
    repository = _build_repository()
    first = repository.add_or_update_feedback(
        query="How do decorators work?",
        chunk_id="chunk-1",
        relevance=1,
    )

    second = repository.add_or_update_feedback(
        query="how do decorators work?",
        chunk_id="chunk-1",
        relevance=2,
    )

    assert second.id == first.id
    assert second.relevance == 2


def test_add_or_update_feedback_creates_separate_rows_for_different_sessions():
    repository = _build_repository()
    first = repository.add_or_update_feedback(
        query="How do decorators work?",
        chunk_id="chunk-1",
        relevance=1,
        session_id="session-a",
    )
    second = repository.add_or_update_feedback(
        query="How do decorators work?",
        chunk_id="chunk-1",
        relevance=2,
        session_id="session-b",
    )

    assert first.id != second.id


def test_add_or_update_feedback_rejects_invalid_relevance():
    repository = _build_repository()

    try:
        repository.add_or_update_feedback(query="q", chunk_id="chunk-1", relevance=-1)
        assert False, "expected ValueError"
    except ValueError as exc:
        assert str(exc) == "relevance must be between 0 and 3"

    try:
        repository.add_or_update_feedback(query="q", chunk_id="chunk-1", relevance=4)
        assert False, "expected ValueError"
    except ValueError as exc:
        assert str(exc) == "relevance must be between 0 and 3"


def test_add_or_update_feedback_rejects_empty_query():
    repository = _build_repository()

    try:
        repository.add_or_update_feedback(query="   ", chunk_id="chunk-1", relevance=1)
        assert False, "expected ValueError"
    except ValueError as exc:
        assert str(exc) == "query must not be empty"


def test_add_or_update_feedback_rejects_empty_chunk_id():
    repository = _build_repository()

    try:
        repository.add_or_update_feedback(query="query", chunk_id="   ", relevance=1)
        assert False, "expected ValueError"
    except ValueError as exc:
        assert str(exc) == "chunk_id must not be empty"


def test_get_feedback_for_query_returns_feedback_for_normalized_query():
    repository = _build_repository()
    repository.add_or_update_feedback(query="How do decorators work?", chunk_id="chunk-1", relevance=3)
    repository.add_or_update_feedback(query="  how do   decorators work? ", chunk_id="chunk-2", relevance=2)
    repository.add_or_update_feedback(query="Other query", chunk_id="chunk-3", relevance=1)

    records = repository.get_feedback_for_query("how do decorators work?")

    assert [record.chunk_id for record in records] == ["chunk-2", "chunk-1"]


def test_get_feedback_for_chunk_returns_feedback_for_chunk():
    repository = _build_repository()
    repository.add_or_update_feedback(query="How do decorators work?", chunk_id="chunk-1", relevance=3)
    repository.add_or_update_feedback(query="How do generators work?", chunk_id="chunk-1", relevance=2, session_id="s1")
    repository.add_or_update_feedback(query="Other query", chunk_id="chunk-2", relevance=1)

    records = repository.get_feedback_for_chunk("chunk-1")

    assert len(records) == 2
    assert {record.normalized_query for record in records} == {
        "how do decorators work?",
        "how do generators work?",
    }


def test_get_summary_returns_zeros_for_empty_repository():
    repository = _build_repository()

    summary = repository.get_summary()

    assert summary.total_feedback_items == 0
    assert summary.queries_with_feedback == 0
    assert summary.positive_feedback == 0
    assert summary.negative_feedback == 0
    assert summary.marginal_feedback == 0
    assert summary.average_relevance == 0.0


def test_get_summary_counts_feedback_records_and_average():
    repository = _build_repository()
    repository.add_or_update_feedback(query="How do decorators work?", chunk_id="chunk-1", relevance=3)
    repository.add_or_update_feedback(query="How do decorators work?", chunk_id="chunk-2", relevance=0)
    repository.add_or_update_feedback(query="Explain generators", chunk_id="chunk-3", relevance=1)

    summary = repository.get_summary()

    assert summary.total_feedback_items == 3
    assert summary.queries_with_feedback == 2
    assert summary.positive_feedback == 1
    assert summary.negative_feedback == 1
    assert summary.marginal_feedback == 1
    assert summary.average_relevance == 4 / 3


def test_get_feedback_for_normalized_query_returns_matching_records():
    repository = _build_repository()
    repository.add_or_update_feedback(query="How do decorators work?", chunk_id="chunk-1", relevance=3)
    repository.add_or_update_feedback(query="  how do   decorators work? ", chunk_id="chunk-2", relevance=2)
    repository.add_or_update_feedback(query="Other query", chunk_id="chunk-3", relevance=1)

    records = repository.get_feedback_for_normalized_query("how do decorators work?")

    assert [record.chunk_id for record in records] == ["chunk-2", "chunk-1"]


def test_get_feedback_for_normalized_query_applies_session_filter():
    repository = _build_repository()
    repository.add_or_update_feedback(query="How do decorators work?", chunk_id="chunk-1", relevance=3, session_id="session-a")
    repository.add_or_update_feedback(query="How do decorators work?", chunk_id="chunk-2", relevance=2, session_id="session-b")

    records = repository.get_feedback_for_normalized_query(
        "how do decorators work?",
        session_id="session-a",
    )

    assert [record.chunk_id for record in records] == ["chunk-1"]


def test_get_feedback_for_normalized_query_returns_all_sessions_when_session_is_none():
    repository = _build_repository()
    repository.add_or_update_feedback(query="How do decorators work?", chunk_id="chunk-1", relevance=3, session_id="session-a")
    repository.add_or_update_feedback(query="How do decorators work?", chunk_id="chunk-2", relevance=2, session_id="session-b")

    records = repository.get_feedback_for_normalized_query("how do decorators work?")

    assert [record.chunk_id for record in records] == ["chunk-2", "chunk-1"]


def test_get_feedback_for_normalized_query_rejects_empty_input():
    repository = _build_repository()

    try:
        repository.get_feedback_for_normalized_query("   ")
        assert False, "expected ValueError"
    except ValueError as exc:
        assert str(exc) == "normalized_query must not be empty"
