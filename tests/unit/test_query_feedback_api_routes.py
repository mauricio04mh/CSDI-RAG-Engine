from __future__ import annotations

import sys
from dataclasses import dataclass
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
import httpx

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

from src.query_feedback.api.routes import router


@dataclass(slots=True)
class FakeSearchResult:
    doc_id: str
    score: float


@dataclass(slots=True)
class FakeChunk:
    chunk_id: str
    source_id: str
    title: str
    breadcrumb: str
    text: str


class FakeHybridRetriever:
    def __init__(self, results: list[FakeSearchResult]) -> None:
        self._results = results

    def search(self, query: str, top_k: int) -> list[FakeSearchResult]:
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
            title="Closures wrappers",
            breadcrumb="Python Functions",
            text="Decorators often use closures and wrappers.",
        ),
        "doc-2": FakeChunk(
            chunk_id="doc-2",
            source_id="mdn_js",
            title="Promises callbacks",
            breadcrumb="JavaScript Async",
            text="Callbacks and promises support async flows.",
        ),
        "doc-3": FakeChunk(
            chunk_id="doc-3",
            source_id="python_docs",
            title="Descriptors generators",
            breadcrumb="Python Classes",
            text="Descriptors and generators complement decorators.",
        ),
    })
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
