from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import datetime
from types import SimpleNamespace

import pytest

if "numpy" not in sys.modules:
    class _FakeNdArray:
        def __init__(self, values):
            self._values = values

        @property
        def ndim(self) -> int:
            if isinstance(self._values, list) and self._values and isinstance(self._values[0], list):
                return 2
            if isinstance(self._values, list):
                return 1
            return 0

        @property
        def shape(self) -> tuple[int, ...]:
            if self.ndim == 2:
                return (len(self._values), len(self._values[0]) if self._values else 0)
            if self.ndim == 1:
                return (len(self._values),)
            return ()

        def reshape(self, _size: int):
            return _FakeNdArray(_flatten(self._values))

        def __iter__(self):
            return iter(self._values)

    def _flatten(value):
        if isinstance(value, list):
            flattened = []
            for item in value:
                flattened.extend(_flatten(item))
            return flattened
        return [float(value)]

    def _asarray(value, dtype=float):
        if isinstance(value, _FakeNdArray):
            return value
        if isinstance(value, tuple):
            value = list(value)
        if isinstance(value, list):
            if value and isinstance(value[0], tuple):
                value = [list(item) for item in value]
            return _FakeNdArray(value)
        return _FakeNdArray(dtype(value))

    def _dot(left, right):
        left_values = _flatten(left._values if isinstance(left, _FakeNdArray) else left)
        right_values = _flatten(right._values if isinstance(right, _FakeNdArray) else right)
        return sum(a * b for a, b in zip(left_values, right_values, strict=True))

    sys.modules["numpy"] = SimpleNamespace(
        ndarray=_FakeNdArray,
        array=lambda value, dtype=float: _asarray(value, dtype=dtype),
        asarray=_asarray,
        dot=_dot,
        isscalar=lambda x: isinstance(x, (bool, int, float, complex)),
        bool_=type("bool_", (), {}),
    )

import numpy as np

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

from src.query_feedback.schemas import FeedbackRecord
from src.query_feedback.service import QueryFeedbackService


@dataclass(slots=True)
class FakeSearchResult:
    doc_id: str
    score: float


@dataclass(slots=True)
class FakeChunk:
    chunk_id: str
    source_id: str
    url: str
    title: str
    breadcrumb: str
    text: str


class FakeHybridRetriever:
    def search(
        self,
        query: str,
        top_k: int,
        vector_query: str | None = None,
    ) -> list[FakeSearchResult]:
        return [
            FakeSearchResult("doc-1", 0.9),
            FakeSearchResult("doc-2", 0.8),
        ][:top_k]


class FakeChunkRepository:
    def get_chunks(self, chunk_ids: list[str]) -> dict[str, FakeChunk]:
        chunks = {
            "doc-1": FakeChunk(
                chunk_id="doc-1",
                source_id="python_docs",
                url="https://docs.python.org/doc-1",
                title="Closures wrappers",
                breadcrumb="Python Functions",
                text="Decorators often use closures and wrappers.",
            ),
            "doc-2": FakeChunk(
                chunk_id="doc-2",
                source_id="python_docs",
                url="https://docs.python.org/doc-2",
                title="Descriptors generators",
                breadcrumb="Python Classes",
                text="Descriptors and generators complement decorators.",
            ),
        }
        return {chunk_id: chunks[chunk_id] for chunk_id in chunk_ids if chunk_id in chunks}


class FakeFeedbackRepository:
    def __init__(self) -> None:
        self._records = [
            FeedbackRecord(
                id=1,
                query="How do decorators work?",
                normalized_query="how do decorators work?",
                chunk_id="doc-1",
                source_id="python_docs",
                relevance=3,
                notes=None,
                session_id=None,
                created_at=datetime(2026, 5, 21, 0, 0, 0),
                updated_at=datetime(2026, 5, 21, 0, 0, 0),
            )
        ]

    def get_feedback_for_query(self, query: str, session_id: str | None = None) -> list[FeedbackRecord]:
        normalized = " ".join(query.strip().lower().split())
        return [record for record in self._records if record.normalized_query == normalized]

    def list_all_feedback(self) -> list[FeedbackRecord]:
        return list(self._records)


class FakeEmbeddingModel1D:
    def encode_query(self, text: str, prefix: str = ""):
        normalized = " ".join(text.strip().lower().split())
        if normalized == "how do decorators work?":
            return np.array([1.0, 0.0])
        if normalized == "explain python decorators":
            return np.array([0.96, 0.28])
        return np.array([0.0, 1.0])


class FakeEmbeddingModel2D:
    def encode_query(self, text: str, prefix: str = ""):
        normalized = " ".join(text.strip().lower().split())
        if normalized == "how do decorators work?":
            return np.array([[1.0, 0.0]])
        if normalized == "explain python decorators":
            return np.array([[0.96, 0.28]])
        return np.array([[0.0, 1.0]])


class FakeEmbeddingModelMismatch:
    def encode_query(self, text: str, prefix: str = ""):
        normalized = " ".join(text.strip().lower().split())
        if normalized == "how do decorators work?":
            return np.array([1.0, 0.0])
        return np.array([1.0, 0.0, 0.0])


def _build_service(embedding_model) -> QueryFeedbackService:
    return QueryFeedbackService(
        hybrid_retriever=FakeHybridRetriever(),
        chunk_repo=FakeChunkRepository(),
        feedback_repository=FakeFeedbackRepository(),
        embedding_model=embedding_model,
        query_prefix="query: ",
    )


def test_search_with_feedback_supports_numpy_1d_embeddings():
    service = _build_service(FakeEmbeddingModel1D())

    result = service.search_with_feedback(
        query="Explain Python decorators",
        top_k=2,
        semantic_feedback_enabled=True,
        semantic_similarity_threshold=0.92,
    )

    doc_1 = next(item for item in result.results if item.chunk_id == "doc-1")
    assert doc_1.feedback_match_type == "semantic"
    assert doc_1.feedback_applied is True


def test_search_with_feedback_supports_numpy_2d_embeddings():
    service = _build_service(FakeEmbeddingModel2D())

    result = service.search_with_feedback(
        query="Explain Python decorators",
        top_k=2,
        semantic_feedback_enabled=True,
        semantic_similarity_threshold=0.92,
    )

    doc_1 = next(item for item in result.results if item.chunk_id == "doc-1")
    assert doc_1.feedback_match_type == "semantic"
    assert doc_1.feedback_applied is True


def test_search_with_feedback_raises_for_mismatched_embedding_dimensions():
    service = _build_service(FakeEmbeddingModelMismatch())

    with pytest.raises(ValueError, match="embedding dimensions do not match"):
        service.search_with_feedback(
            query="Explain Python decorators",
            top_k=2,
            semantic_feedback_enabled=True,
            semantic_similarity_threshold=0.92,
        )
