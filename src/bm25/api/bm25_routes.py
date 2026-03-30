from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from src.bm25.pipeline.bm25_retriever import BM25Retriever
from src.database.repositories.chunk_repository import ChunkRepository

logger = logging.getLogger(__name__)
router = APIRouter(tags=["bm25"])


class BM25SearchRequest(BaseModel):
    """Payload for lexical BM25 search."""

    query: str = Field(..., min_length=1, description="User query to score against the inverted index.")
    top_k: int = Field(default=20, ge=1, le=100, description="Maximum number of ranked hits to return.")


class BM25SearchResultItem(BaseModel):
    """One BM25-ranked search hit."""

    doc_id: str
    score: float
    source_id: str
    url: str
    title: str
    breadcrumb: str
    text: str


class BM25SearchResponse(BaseModel):
    """Response returned by the BM25 retrieval endpoint."""

    results: list[BM25SearchResultItem]


@router.post("/api/v1/search/bm25", response_model=BM25SearchResponse)
def search_bm25(payload: BM25SearchRequest, request: Request) -> BM25SearchResponse:
    """Run BM25 lexical retrieval against the persisted inverted index."""
    retriever: BM25Retriever = request.app.state.bm25_retriever
    chunk_repo: ChunkRepository = request.app.state.chunk_repo
    try:
        results = retriever.search(query=payload.query, top_k=payload.top_k)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("bm25_search_failed query=%s", payload.query)
        raise HTTPException(status_code=500, detail="BM25 search failed.") from exc

    doc_ids = [r.doc_id for r in results]
    chunks = chunk_repo.get_chunks(doc_ids)
    score_map = {r.doc_id: r.score for r in results}

    enriched: list[BM25SearchResultItem] = []
    for doc_id in doc_ids:
        chunk = chunks.get(doc_id)
        if chunk is None:
            continue
        enriched.append(BM25SearchResultItem(
            doc_id=doc_id,
            score=score_map[doc_id],
            source_id=chunk.source_id,
            url=chunk.url,
            title=chunk.title,
            breadcrumb=chunk.breadcrumb,
            text=chunk.text,
        ))

    return BM25SearchResponse(results=enriched)
