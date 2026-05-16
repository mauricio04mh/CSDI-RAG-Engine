from __future__ import annotations

import logging
import re

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from src.generation.chat_history import ChatHistoryStore
from src.generation.rag_pipeline import RAGPipeline

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/rag", tags=["rag"])


class RAGRequest(BaseModel):
    query: str = Field(..., min_length=1, description="Question to answer using the documentation.")
    session_id: str | None = Field(default=None, description="Client chat session identifier.")


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


class ChatHistoryMessage(BaseModel):
    id: str
    type: str
    content: str
    timestamp: str
    sources: list[SourceItem] | None = None
    model: str | None = None


class ChatHistoryResponse(BaseModel):
    session_id: str
    messages: list[ChatHistoryMessage]


SESSION_ID_PATTERN = re.compile(r"^[a-zA-Z0-9_-]{8,128}$")


def _validate_session_id(session_id: str) -> None:
    if not SESSION_ID_PATTERN.match(session_id):
        raise HTTPException(status_code=400, detail="Invalid session_id format")


@router.post("/query", response_model=RAGResponse)
def rag_query(payload: RAGRequest, request: Request) -> RAGResponse:
    """Retrieve relevant documentation chunks and generate an answer using the configured LLM."""
    pipeline: RAGPipeline = request.app.state.rag_pipeline

    try:
        result = pipeline.query(payload.query)
    except Exception as exc:
        logger.exception("rag_query_failed query=%s", payload.query)
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    response = RAGResponse(
        query=result.query,
        answer=result.answer,
        sources=[SourceItem(chunk_id=s.chunk_id, url=s.url, title=s.title) for s in result.sources],
        model=result.model,
        prompt_tokens=result.prompt_tokens,
        completion_tokens=result.completion_tokens,
    )

    if payload.session_id:
        _validate_session_id(payload.session_id)
        history_store: ChatHistoryStore | None = getattr(request.app.state, "chat_history_store", None)
        if history_store is not None:
            history_store.append_exchange(
                session_id=payload.session_id,
                query=payload.query,
                answer=response.answer,
                sources=[{"chunk_id": s.chunk_id, "url": s.url, "title": s.title} for s in response.sources],
                model=response.model,
            )

    return response


@router.get("/history/{session_id}", response_model=ChatHistoryResponse)
def get_chat_history(session_id: str, request: Request) -> ChatHistoryResponse:
    _validate_session_id(session_id)
    history_store: ChatHistoryStore | None = getattr(request.app.state, "chat_history_store", None)
    if history_store is None:
        return ChatHistoryResponse(session_id=session_id, messages=[])

    messages = history_store.get_history(session_id)
    parsed_messages: list[ChatHistoryMessage] = []
    for message in messages:
        sources_payload = message.get("sources")
        sources = None
        if isinstance(sources_payload, list):
            safe_sources: list[SourceItem] = []
            for source in sources_payload:
                if isinstance(source, dict):
                    chunk_id = source.get("chunk_id")
                    url = source.get("url")
                    title = source.get("title")
                    if isinstance(chunk_id, str) and isinstance(url, str) and isinstance(title, str):
                        safe_sources.append(SourceItem(chunk_id=chunk_id, url=url, title=title))
            sources = safe_sources

        if (
            isinstance(message.get("id"), str)
            and isinstance(message.get("type"), str)
            and isinstance(message.get("content"), str)
            and isinstance(message.get("timestamp"), str)
        ):
            parsed_messages.append(
                ChatHistoryMessage(
                    id=message["id"],
                    type=message["type"],
                    content=message["content"],
                    timestamp=message["timestamp"],
                    sources=sources,
                    model=message.get("model") if isinstance(message.get("model"), str) else None,
                )
            )

    return ChatHistoryResponse(session_id=session_id, messages=parsed_messages)
