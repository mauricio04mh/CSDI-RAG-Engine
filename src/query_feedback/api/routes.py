from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from src.query_feedback.service import QueryFeedbackService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/query-feedback", tags=["query-feedback"])


class QueryExpansionRequest(BaseModel):
    query: str = Field(..., min_length=1)
    top_k_feedback: int = Field(default=5, ge=1, le=50)
    max_expansion_terms: int = Field(default=6, ge=0, le=20)
    source_ids: list[str] | None = None


class QueryExpansionResponse(BaseModel):
    original_query: str
    expanded_query: str
    expansion_terms: list[str]
    method: str
    feedback_documents_used: int


@router.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "module": "query-feedback"}


@router.post("/expand", response_model=QueryExpansionResponse)
async def expand_query(
    payload: QueryExpansionRequest,
    request: Request,
) -> QueryExpansionResponse:
    service = QueryFeedbackService(
        hybrid_retriever=request.app.state.hybrid_retriever,
        chunk_repo=request.app.state.chunk_repo,
    )
    try:
        result = service.expand_query(
            query=payload.query,
            top_k_feedback=payload.top_k_feedback,
            max_expansion_terms=payload.max_expansion_terms,
            source_ids=payload.source_ids,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("query_feedback_expand_failed")
        raise HTTPException(status_code=500, detail="Query expansion failed.") from exc

    return QueryExpansionResponse(
        original_query=result.original_query,
        expanded_query=result.expanded_query,
        expansion_terms=result.expansion_terms,
        method=result.method,
        feedback_documents_used=result.feedback_documents_used,
    )
