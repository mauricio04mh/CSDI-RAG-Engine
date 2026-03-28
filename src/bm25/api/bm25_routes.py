from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from src.bm25.pipeline.bm25_retriever import BM25Retriever

logger = logging.getLogger(__name__)
router = APIRouter(tags=["bm25"])


class BM25SearchRequest(BaseModel):
    query: str = Field(..., min_length=1)
    top_k: int = Field(default=20, ge=1, le=100)


class BM25SearchResultItem(BaseModel):
    doc_id: str
    score: float


class BM25SearchResponse(BaseModel):
    results: list[BM25SearchResultItem]


@router.post("/api/v1/search/bm25", response_model=BM25SearchResponse)
def search_bm25(payload: BM25SearchRequest, request: Request) -> BM25SearchResponse:
    """Run BM25 lexical retrieval against the persisted inverted index."""
    retriever: BM25Retriever = request.app.state.bm25_retriever
    try:
        results = retriever.search(query=payload.query, top_k=payload.top_k)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("bm25_search_failed query=%s", payload.query)
        raise HTTPException(status_code=500, detail="BM25 search failed.") from exc

    return BM25SearchResponse(
        results=[BM25SearchResultItem(doc_id=r.doc_id, score=r.score) for r in results]
    )
