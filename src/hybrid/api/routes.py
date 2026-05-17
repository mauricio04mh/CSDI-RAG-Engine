from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from src.database.repositories.chunk_repository import ChunkRepository
from src.hybrid.pipeline.hybrid_retriever import HybridRetriever

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1", tags=["search"])


class SearchRequest(BaseModel):
    query: str = Field(..., min_length=1, description="Programming question or search query.")
    top_k: int = Field(default=10, ge=1, le=50, description="Maximum number of results.")
    source_ids: list[str] | None = Field(
        default=None,
        description="If set, only return chunks from these source IDs. Result count may be less than top_k.",
    )


class SearchResultItem(BaseModel):
    chunk_id: str
    score: float
    source_id: str
    url: str
    title: str
    breadcrumb: str
    text: str


class SearchResponse(BaseModel):
    query: str
    results: list[SearchResultItem]


@router.post("/search/web-cache", response_model=SearchResponse)
def search_web_cache(payload: SearchRequest, request: Request) -> SearchResponse:
    """Hybrid search restricted to the web-cache index (results from previous web searches)."""
    retriever: HybridRetriever = request.app.state.web_cache_hybrid_retriever
    chunk_repo: ChunkRepository = request.app.state.web_cache_chunk_repo

    try:
        hits = retriever.search(query=payload.query, top_k=payload.top_k)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("web_cache_search_failed query=%s", payload.query)
        raise HTTPException(status_code=500, detail="Search failed.") from exc

    chunk_ids = [h.doc_id for h in hits]
    chunks = chunk_repo.get_chunks(chunk_ids)
    score_map = {h.doc_id: h.score for h in hits}

    results: list[SearchResultItem] = []
    for chunk_id in chunk_ids:
        chunk = chunks.get(chunk_id)
        if chunk is None:
            continue
        results.append(SearchResultItem(
            chunk_id=chunk_id,
            score=score_map[chunk_id],
            source_id=chunk.source_id,
            url=chunk.url,
            title=chunk.title,
            breadcrumb=chunk.breadcrumb,
            text=chunk.text,
        ))

    if payload.source_ids is not None:
        sid_set = set(payload.source_ids)
        results = [item for item in results if item.source_id in sid_set]

    return SearchResponse(query=payload.query, results=results)


@router.post("/search", response_model=SearchResponse)
def search(payload: SearchRequest, request: Request) -> SearchResponse:
    """Hybrid BM25 + dense vector search fused with Reciprocal Rank Fusion."""
    retriever: HybridRetriever = request.app.state.hybrid_retriever
    chunk_repo: ChunkRepository = request.app.state.chunk_repo

    try:
        hits = retriever.search(query=payload.query, top_k=payload.top_k)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("hybrid_search_failed query=%s", payload.query)
        raise HTTPException(status_code=500, detail="Search failed.") from exc

    chunk_ids = [h.doc_id for h in hits]
    chunks = chunk_repo.get_chunks(chunk_ids)
    score_map = {h.doc_id: h.score for h in hits}
    missing_chunks = [chunk_id for chunk_id in chunk_ids if chunk_id not in chunks]
    logger.info(
        "hybrid_search_trace query=%s requested_top_k=%s retrieved_hits=%s resolved_chunks=%s missing_chunks=%s top_ids=%s",
        payload.query,
        payload.top_k,
        len(chunk_ids),
        len(chunks),
        len(missing_chunks),
        chunk_ids[:10],
    )

    results: list[SearchResultItem] = []
    for chunk_id in chunk_ids:
        chunk = chunks.get(chunk_id)
        if chunk is None:
            continue
        results.append(SearchResultItem(
            chunk_id=chunk_id,
            score=score_map[chunk_id],
            source_id=chunk.source_id,
            url=chunk.url,
            title=chunk.title,
            breadcrumb=chunk.breadcrumb,
            text=chunk.text,
        ))

    if payload.source_ids is not None:
        sid_set = set(payload.source_ids)
        results = [item for item in results if item.source_id in sid_set]

    return SearchResponse(query=payload.query, results=results)
