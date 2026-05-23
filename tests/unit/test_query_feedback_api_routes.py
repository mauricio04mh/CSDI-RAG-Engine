from __future__ import annotations

import sys
from dataclasses import dataclass
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
import httpx
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

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

from src.query_feedback.models import Base as QueryFeedbackBase
from src.query_feedback.api.routes import router


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
    def __init__(self, results: list[FakeSearchResult]) -> None:
        self._results = results
        self.calls: list[dict[str, object]] = []

    def search(
        self,
        query: str,
        top_k: int,
        vector_query: str | None = None,
    ) -> list[FakeSearchResult]:
        self.calls.append({
            "query": query,
            "top_k": top_k,
            "vector_query": vector_query,
        })
        return self._results[:top_k]


class FakeChunkRepository:
    def __init__(self, chunks: dict[str, FakeChunk]) -> None:
        self._chunks = chunks

    def get_chunks(self, chunk_ids: list[str]) -> dict[str, FakeChunk]:
        return {
            chunk_id: self._chunks[chunk_id]
            for chunk_id in chunk_ids
            if chunk_id in self._chunks
        }


class FakeEmbeddingModel:
    def encode_query(self, text: str, prefix: str = ""):
        normalized = " ".join(text.strip().lower().split())
        if normalized == "how do decorators work?":
            import numpy as np
            return np.array([1.0, 0.0])
        if normalized == "explain python decorators":
            import numpy as np
            return np.array([0.96, 0.28])
        if normalized == "why use python decorators":
            import numpy as np
            return np.array([0.93, 0.37])
        import numpy as np
        return np.array([0.0, 1.0])


class FakeEmbeddingModel2D:
    def encode_query(self, text: str, prefix: str = ""):
        normalized = " ".join(text.strip().lower().split())
        if normalized == "how do decorators work?":
            import numpy as np
            return np.array([[1.0, 0.0]])
        if normalized == "explain python decorators":
            import numpy as np
            return np.array([[0.96, 0.28]])
        import numpy as np
        return np.array([[0.0, 1.0]])


class FakeVectorRetriever:
    def __init__(self, embedding_model=None) -> None:
        self._embedding_model = embedding_model or FakeEmbeddingModel()
        self._query_prefix = "query: "


def _build_app(*, embedding_model=None) -> FastAPI:
    app = FastAPI()
    app.include_router(router)
    app.state.hybrid_retriever = FakeHybridRetriever([
        FakeSearchResult("doc-1", 0.9),
        FakeSearchResult("doc-2", 0.8),
        FakeSearchResult("doc-3", 0.7),
    ])
    app.state.chunk_repo = FakeChunkRepository({
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
            source_id="mdn_js",
            url="https://developer.mozilla.org/doc-2",
            title="Promises callbacks",
            breadcrumb="JavaScript Async",
            text="Callbacks and promises support async flows.",
        ),
        "doc-3": FakeChunk(
            chunk_id="doc-3",
            source_id="python_docs",
            url="https://docs.python.org/doc-3",
            title="Descriptors generators",
            breadcrumb="Python Classes",
            text="Descriptors and generators complement decorators.",
        ),
    })
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    QueryFeedbackBase.metadata.create_all(engine)
    app.state.db_engine = engine
    app.state.vector_retriever = FakeVectorRetriever(embedding_model=embedding_model)
    return app


@pytest.mark.anyio
async def test_query_feedback_health_endpoint_returns_expected_payload():
    app = _build_app()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://testserver",
    ) as client:
        response = await client.get("/api/v1/query-feedback/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "module": "query-feedback"}


@pytest.mark.anyio
async def test_query_feedback_expand_endpoint_returns_expansion_result():
    app = _build_app()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://testserver",
    ) as client:
        response = await client.post(
            "/api/v1/query-feedback/expand",
            json={
                "query": "python decorator",
                "top_k_feedback": 2,
                "max_expansion_terms": 4,
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["original_query"] == "python decorator"
    assert payload["expanded_query"].startswith("python decorator")
    assert payload["expansion_terms"]
    assert payload["method"] == "pseudo_relevance_feedback"
    assert payload["feedback_documents_used"] == 2


@pytest.mark.anyio
async def test_query_feedback_expand_endpoint_applies_source_filter():
    app = _build_app()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://testserver",
    ) as client:
        response = await client.post(
            "/api/v1/query-feedback/expand",
            json={
                "query": "python decorator",
                "top_k_feedback": 2,
                "max_expansion_terms": 4,
                "source_ids": ["python_docs"],
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["feedback_documents_used"] == 2
    assert "promises" not in payload["expansion_terms"]
    assert "callbacks" not in payload["expansion_terms"]


@pytest.mark.anyio
async def test_query_feedback_expand_endpoint_rejects_empty_query():
    app = _build_app()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://testserver",
    ) as client:
        response = await client.post(
            "/api/v1/query-feedback/expand",
            json={
                "query": "",
            },
        )

    assert response.status_code == 422


@pytest.mark.anyio
async def test_query_feedback_search_endpoint_uses_expanded_vector_query():
    app = _build_app()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://testserver",
    ) as client:
        response = await client.post(
            "/api/v1/query-feedback/search",
            json={
                "query": "python decorator",
                "top_k": 2,
                "expansion_enabled": True,
                "top_k_feedback": 2,
                "max_expansion_terms": 4,
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["original_query"] == "python decorator"
    assert payload["expanded_query"] != "python decorator"
    assert payload["expansion_terms"]
    assert payload["strategy"] == "hybrid_expanded_vector"
    assert payload["expansion_enabled"] is True
    assert len(payload["results"]) == 2
    assert payload["results"][0]["chunk_id"] == "doc-1"
    assert payload["results"][0]["source_id"] == "python_docs"
    assert payload["results"][0]["url"] == "https://docs.python.org/doc-1"
    assert payload["results"][0]["title"] == "Closures wrappers"
    assert payload["results"][0]["breadcrumb"] == "Python Functions"
    assert "Decorators often use closures" in payload["results"][0]["text"]

    final_call = app.state.hybrid_retriever.calls[-1]
    assert final_call["query"] == "python decorator"
    assert final_call["vector_query"] == payload["expanded_query"]
    assert final_call["top_k"] == 2


@pytest.mark.anyio
async def test_query_feedback_search_endpoint_without_expansion_uses_plain_hybrid():
    app = _build_app()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://testserver",
    ) as client:
        response = await client.post(
            "/api/v1/query-feedback/search",
            json={
                "query": "python decorator",
                "top_k": 2,
                "expansion_enabled": False,
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["expanded_query"] == "python decorator"
    assert payload["expansion_terms"] == []
    assert payload["method"] == "none"
    assert payload["strategy"] == "hybrid"
    assert payload["expansion_enabled"] is False
    final_call = app.state.hybrid_retriever.calls[-1]
    assert final_call["query"] == "python decorator"
    assert final_call["vector_query"] is None
    assert final_call["top_k"] == 2


@pytest.mark.anyio
async def test_query_feedback_search_endpoint_applies_source_filter():
    app = _build_app()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://testserver",
    ) as client:
        response = await client.post(
            "/api/v1/query-feedback/search",
            json={
                "query": "python decorator",
                "top_k": 2,
                "source_ids": ["python_docs"],
                "expansion_enabled": True,
                "top_k_feedback": 2,
                "max_expansion_terms": 4,
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert [item["source_id"] for item in payload["results"]] == ["python_docs", "python_docs"]
    assert [item["chunk_id"] for item in payload["results"]] == ["doc-1", "doc-3"]


@pytest.mark.anyio
async def test_query_feedback_search_endpoint_rejects_empty_query():
    app = _build_app()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://testserver",
    ) as client:
        response = await client.post(
            "/api/v1/query-feedback/search",
            json={
                "query": "",
            },
        )

    assert response.status_code == 422


@pytest.mark.anyio
async def test_query_feedback_feedback_endpoint_returns_stored_record():
    app = _build_app()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://testserver",
    ) as client:
        response = await client.post(
            "/api/v1/query-feedback/feedback",
            json={
                "query": "How do decorators work?",
                "chunk_id": "doc-1",
                "relevance": 3,
                "source_id": "python_docs",
                "notes": "Very useful",
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["id"] == 1
    assert payload["query"] == "How do decorators work?"
    assert payload["normalized_query"] == "how do decorators work?"
    assert payload["chunk_id"] == "doc-1"
    assert payload["relevance"] == 3
    assert payload["stored"] is True


@pytest.mark.anyio
async def test_query_feedback_feedback_endpoint_updates_existing_record():
    app = _build_app()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://testserver",
    ) as client:
        first = await client.post(
            "/api/v1/query-feedback/feedback",
            json={
                "query": "How do decorators work?",
                "chunk_id": "doc-1",
                "relevance": 1,
                "session_id": "session-a",
            },
        )
        second = await client.post(
            "/api/v1/query-feedback/feedback",
            json={
                "query": "  how do   decorators work? ",
                "chunk_id": "doc-1",
                "relevance": 3,
                "notes": "Updated",
                "session_id": "session-a",
            },
        )

    assert first.status_code == 200
    assert second.status_code == 200
    assert second.json()["id"] == first.json()["id"]
    assert second.json()["relevance"] == 3
    assert second.json()["notes"] == "Updated"


@pytest.mark.anyio
async def test_query_feedback_feedback_endpoint_rejects_invalid_relevance():
    app = _build_app()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://testserver",
    ) as client:
        response = await client.post(
            "/api/v1/query-feedback/feedback",
            json={
                "query": "How do decorators work?",
                "chunk_id": "doc-1",
                "relevance": 4,
            },
        )

    assert response.status_code == 422


@pytest.mark.anyio
async def test_query_feedback_feedback_endpoint_rejects_empty_query():
    app = _build_app()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://testserver",
    ) as client:
        response = await client.post(
            "/api/v1/query-feedback/feedback",
            json={
                "query": "",
                "chunk_id": "doc-1",
                "relevance": 2,
            },
        )

    assert response.status_code == 422


@pytest.mark.anyio
async def test_query_feedback_feedback_endpoint_rejects_empty_chunk_id():
    app = _build_app()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://testserver",
    ) as client:
        response = await client.post(
            "/api/v1/query-feedback/feedback",
            json={
                "query": "How do decorators work?",
                "chunk_id": "",
                "relevance": 2,
            },
        )

    assert response.status_code == 422


@pytest.mark.anyio
async def test_query_feedback_summary_endpoint_returns_zeros_without_feedback():
    app = _build_app()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        response = await client.get("/api/v1/query-feedback/feedback/summary")

    assert response.status_code == 200
    assert response.json() == {
        "total_feedback_items": 0,
        "queries_with_feedback": 0,
        "positive_feedback": 0,
        "negative_feedback": 0,
        "marginal_feedback": 0,
        "average_relevance": 0.0,
    }


@pytest.mark.anyio
async def test_query_feedback_summary_endpoint_returns_aggregate_counts():
    app = _build_app()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        await client.post(
            "/api/v1/query-feedback/feedback",
            json={"query": "How do decorators work?", "chunk_id": "doc-1", "relevance": 3},
        )
        await client.post(
            "/api/v1/query-feedback/feedback",
            json={"query": "How do decorators work?", "chunk_id": "doc-2", "relevance": 0},
        )
        await client.post(
            "/api/v1/query-feedback/feedback",
            json={"query": "Explain generators", "chunk_id": "doc-3", "relevance": 1},
        )
        response = await client.get("/api/v1/query-feedback/feedback/summary")

    assert response.status_code == 200
    payload = response.json()
    assert payload["total_feedback_items"] == 3
    assert payload["queries_with_feedback"] == 2
    assert payload["positive_feedback"] == 1
    assert payload["negative_feedback"] == 1
    assert payload["marginal_feedback"] == 1
    assert payload["average_relevance"] == 4 / 3


@pytest.mark.anyio
async def test_query_feedback_get_feedback_by_query_returns_matching_items():
    app = _build_app()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        await client.post(
            "/api/v1/query-feedback/feedback",
            json={"query": "How do decorators work?", "chunk_id": "doc-1", "relevance": 3},
        )
        await client.post(
            "/api/v1/query-feedback/feedback",
            json={"query": "  how do   decorators work? ", "chunk_id": "doc-2", "relevance": 2},
        )
        await client.post(
            "/api/v1/query-feedback/feedback",
            json={"query": "Other query", "chunk_id": "doc-3", "relevance": 1},
        )
        response = await client.get(
            "/api/v1/query-feedback/feedback",
            params={"query": "How do decorators work?"},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["normalized_query"] == "how do decorators work?"
    assert [item["chunk_id"] for item in payload["items"]] == ["doc-2", "doc-1"]


@pytest.mark.anyio
async def test_query_feedback_get_feedback_by_query_applies_session_filter():
    app = _build_app()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        await client.post(
            "/api/v1/query-feedback/feedback",
            json={"query": "How do decorators work?", "chunk_id": "doc-1", "relevance": 3, "session_id": "session-a"},
        )
        await client.post(
            "/api/v1/query-feedback/feedback",
            json={"query": "How do decorators work?", "chunk_id": "doc-2", "relevance": 2, "session_id": "session-b"},
        )
        response = await client.get(
            "/api/v1/query-feedback/feedback",
            params={"query": "How do decorators work?", "session_id": "session-a"},
        )

    assert response.status_code == 200
    assert [item["chunk_id"] for item in response.json()["items"]] == ["doc-1"]


@pytest.mark.anyio
async def test_query_feedback_get_feedback_by_query_requires_query_parameter():
    app = _build_app()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        response = await client.get("/api/v1/query-feedback/feedback")

    assert response.status_code == 422


@pytest.mark.anyio
async def test_query_feedback_get_feedback_by_query_rejects_whitespace_query():
    app = _build_app()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        response = await client.get(
            "/api/v1/query-feedback/feedback",
            params={"query": "   "},
        )

    assert response.status_code == 400


@pytest.mark.anyio
async def test_query_feedback_search_with_feedback_without_saved_feedback_keeps_scores():
    app = _build_app()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        response = await client.post(
            "/api/v1/query-feedback/search-with-feedback",
            json={
                "query": "python decorator",
                "top_k": 2,
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["feedback_applied"] is False
    assert payload["feedback_items_used"] == 0
    assert payload["results"][0]["adjusted_score"] == payload["results"][0]["original_score"]


@pytest.mark.anyio
async def test_query_feedback_search_with_feedback_applies_exact_feedback():
    app = _build_app()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        await client.post(
            "/api/v1/query-feedback/feedback",
            json={
                "query": "python decorator",
                "chunk_id": "doc-1",
                "relevance": 3,
            },
        )
        response = await client.post(
            "/api/v1/query-feedback/search-with-feedback",
            json={
                "query": "python decorator",
                "top_k": 2,
            },
        )

    assert response.status_code == 200
    payload = response.json()
    doc_1 = next(item for item in payload["results"] if item["chunk_id"] == "doc-1")
    assert payload["feedback_applied"] is True
    assert doc_1["feedback_match_type"] == "exact"
    assert doc_1["adjusted_score"] > doc_1["original_score"]


@pytest.mark.anyio
async def test_query_feedback_search_with_feedback_applies_negative_exact_feedback():
    app = _build_app()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        await client.post(
            "/api/v1/query-feedback/feedback",
            json={
                "query": "python decorator",
                "chunk_id": "doc-1",
                "relevance": 0,
            },
        )
        response = await client.post(
            "/api/v1/query-feedback/search-with-feedback",
            json={
                "query": "python decorator",
                "top_k": 2,
            },
        )

    assert response.status_code == 200
    doc_1 = next(item for item in response.json()["results"] if item["chunk_id"] == "doc-1")
    assert doc_1["adjusted_score"] < doc_1["original_score"]


@pytest.mark.anyio
async def test_query_feedback_search_with_feedback_applies_semantic_feedback():
    app = _build_app()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        await client.post(
            "/api/v1/query-feedback/feedback",
            json={
                "query": "How do decorators work?",
                "chunk_id": "doc-1",
                "relevance": 3,
            },
        )
        response = await client.post(
            "/api/v1/query-feedback/search-with-feedback",
            json={
                "query": "Explain Python decorators",
                "top_k": 2,
                "semantic_feedback_enabled": True,
                "semantic_similarity_threshold": 0.92,
            },
        )

    assert response.status_code == 200
    payload = response.json()
    doc_1 = next(item for item in payload["results"] if item["chunk_id"] == "doc-1")
    assert doc_1["feedback_match_type"] == "semantic"
    assert doc_1["feedback_applied"] is True
    assert payload["matched_feedback_queries"]
    assert payload["matched_feedback_queries"][0]["query"] == "How do decorators work?"
    assert float(payload["matched_feedback_queries"][0]["similarity"]) >= 0.92


@pytest.mark.anyio
async def test_query_feedback_search_with_feedback_supports_2d_numpy_embeddings():
    app = _build_app(embedding_model=FakeEmbeddingModel2D())

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        await client.post(
            "/api/v1/query-feedback/feedback",
            json={
                "query": "How do decorators work?",
                "chunk_id": "doc-1",
                "relevance": 3,
            },
        )
        response = await client.post(
            "/api/v1/query-feedback/search-with-feedback",
            json={
                "query": "Explain Python decorators",
                "top_k": 2,
                "semantic_feedback_enabled": True,
                "semantic_similarity_threshold": 0.92,
            },
        )

    assert response.status_code == 200
    doc_1 = next(item for item in response.json()["results"] if item["chunk_id"] == "doc-1")
    assert doc_1["feedback_match_type"] == "semantic"


@pytest.mark.anyio
async def test_query_feedback_search_with_feedback_respects_semantic_threshold():
    app = _build_app()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        await client.post(
            "/api/v1/query-feedback/feedback",
            json={
                "query": "How do decorators work?",
                "chunk_id": "doc-1",
                "relevance": 3,
            },
        )
        response = await client.post(
            "/api/v1/query-feedback/search-with-feedback",
            json={
                "query": "Explain Python decorators",
                "top_k": 2,
                "semantic_feedback_enabled": True,
                "semantic_similarity_threshold": 1.0,
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["feedback_applied"] is False
    assert payload["matched_feedback_queries"] == []


@pytest.mark.anyio
async def test_query_feedback_search_with_feedback_prefers_exact_over_semantic():
    app = _build_app()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        await client.post(
            "/api/v1/query-feedback/feedback",
            json={
                "query": "How do decorators work?",
                "chunk_id": "doc-1",
                "relevance": 0,
            },
        )
        await client.post(
            "/api/v1/query-feedback/feedback",
            json={
                "query": "Explain Python decorators",
                "chunk_id": "doc-1",
                "relevance": 3,
            },
        )
        response = await client.post(
            "/api/v1/query-feedback/search-with-feedback",
            json={
                "query": "Explain Python decorators",
                "top_k": 2,
            },
        )

    assert response.status_code == 200
    doc_1 = next(item for item in response.json()["results"] if item["chunk_id"] == "doc-1")
    assert doc_1["feedback_match_type"] == "exact"
    assert doc_1["feedback_relevance"] == 3


@pytest.mark.anyio
async def test_query_feedback_search_with_feedback_rejects_empty_query():
    app = _build_app()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        response = await client.post(
            "/api/v1/query-feedback/search-with-feedback",
            json={
                "query": "",
            },
        )

    assert response.status_code == 422


@pytest.mark.anyio
async def test_query_feedback_search_with_feedback_rejects_invalid_similarity_threshold():
    app = _build_app()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        response = await client.post(
            "/api/v1/query-feedback/search-with-feedback",
            json={
                "query": "python decorator",
                "semantic_similarity_threshold": 1.2,
            },
        )

    assert response.status_code == 422
