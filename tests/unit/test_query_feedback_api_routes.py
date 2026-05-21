from __future__ import annotations

import sys
from dataclasses import dataclass
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
import httpx
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

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


def _build_app() -> FastAPI:
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
