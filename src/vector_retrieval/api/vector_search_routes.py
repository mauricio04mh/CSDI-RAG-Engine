from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from src.vector_retrieval.pipeline.vector_retriever import VectorRetriever

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/vector", tags=["vector-retrieval"])


class VectorSearchRequest(BaseModel):
    query: str = Field(..., min_length=1, description="Natural language query to embed and search.")
    top_k: int = Field(default=10, ge=1, le=100, description="Maximum number of results to return.")


class VectorSearchResultItem(BaseModel):
    doc_id: str
    score: float


class VectorSearchResponse(BaseModel):
    results: list[VectorSearchResultItem]


@router.post("/search", response_model=VectorSearchResponse)
def search_vector(payload: VectorSearchRequest, request: Request) -> VectorSearchResponse:
    """Embed the query and run ANN search against the in-memory FAISS index."""
    retriever: VectorRetriever = request.app.state.vector_retriever
    try:
        results = retriever.search(query=payload.query, top_k=payload.top_k)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("vector_search_failed query=%s", payload.query)
        raise HTTPException(status_code=500, detail="Vector search failed.") from exc

    return VectorSearchResponse(
        results=[VectorSearchResultItem(doc_id=r.doc_id, score=r.score) for r in results]
    )
