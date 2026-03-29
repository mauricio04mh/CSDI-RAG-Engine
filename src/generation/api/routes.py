from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from src.generation.rag_pipeline import RAGPipeline

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/rag", tags=["rag"])


class RAGRequest(BaseModel):
    query: str = Field(..., min_length=1, description="Question to answer using the documentation.")


class SourceItem(BaseModel):
    chunk_id: str
    url: str
    title: str


class RAGResponse(BaseModel):
    query: str
    answer: str
    sources: list[SourceItem]
    model: str
    prompt_tokens: int
    completion_tokens: int


@router.post("/query", response_model=RAGResponse)
def rag_query(payload: RAGRequest, request: Request) -> RAGResponse:
    """Retrieve relevant documentation chunks and generate an answer using the configured LLM."""
    pipeline: RAGPipeline = request.app.state.rag_pipeline

    try:
        result = pipeline.query(payload.query)
    except Exception as exc:
        logger.exception("rag_query_failed query=%s", payload.query)
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return RAGResponse(
        query=result.query,
        answer=result.answer,
        sources=[SourceItem(chunk_id=s.chunk_id, url=s.url, title=s.title) for s in result.sources],
        model=result.model,
        prompt_tokens=result.prompt_tokens,
        completion_tokens=result.completion_tokens,
    )
