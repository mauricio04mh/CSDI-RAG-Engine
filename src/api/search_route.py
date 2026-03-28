from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from src.hybrid.pipeline.hybrid_retriever import HybridRetriever

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1", tags=["search"])


class SearchRequest(BaseModel):
    query: str = Field(..., min_length=1, description="Programming question or search query.")
    top_k: int = Field(default=10, ge=1, le=50, description="Maximum number of results.")


class SearchResultItem(BaseModel):
    doc_id: str
    score: float


class SearchResponse(BaseModel):
    query: str
    results: list[SearchResultItem]


@router.post("/search", response_model=SearchResponse)
def search(payload: SearchRequest, request: Request) -> SearchResponse:
    """Hybrid BM25 + dense vector search fused with Reciprocal Rank Fusion."""
    retriever: HybridRetriever = request.app.state.hybrid_retriever
    try:
        results = retriever.search(query=payload.query, top_k=payload.top_k)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("hybrid_search_failed query=%s", payload.query)
        raise HTTPException(status_code=500, detail="Search failed.") from exc

    return SearchResponse(
        query=payload.query,
        results=[SearchResultItem(doc_id=r.doc_id, score=r.score) for r in results],
    )
