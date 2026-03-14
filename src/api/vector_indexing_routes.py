from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from src.vector_indexing.pipeline.vector_index_builder import VectorIndexBuilder

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/vector", tags=["vector-indexing"])


class VectorIndexDocumentRequest(BaseModel):
    """Payload for indexing one dense document."""

    doc_id: str = Field(..., min_length=1, description="Unique external document identifier.")
    text: str = Field(..., min_length=1, description="Raw text used to generate dense embeddings.")


class VectorIndexDocumentResponse(BaseModel):
    """API response after indexing a dense document."""

    status: str
    doc_id: str
    buffered_documents: int
    indexed_documents: int
    persisted: bool


class VectorSearchRequest(BaseModel):
    """Payload for dense retrieval queries."""

    query: str = Field(..., min_length=1, description="User query to embed and search.")
    top_k: int = Field(default=10, ge=1, le=100, description="Maximum number of nearest neighbors to return.")


class VectorSearchResultItem(BaseModel):
    """One similarity search hit."""

    doc_id: str
    score: float


class VectorSearchResponse(BaseModel):
    """Similarity search response."""

    results: list[VectorSearchResultItem]


@router.post("/index", response_model=VectorIndexDocumentResponse)
async def index_document(payload: VectorIndexDocumentRequest, request: Request) -> VectorIndexDocumentResponse:
    """Generate an embedding and enqueue the document into the FAISS pipeline."""
    builder: VectorIndexBuilder = request.app.state.vector_index_builder
    try:
        result = builder.add_document(doc_id=payload.doc_id, text=payload.text)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - defensive API guard
        logger.exception("vector_indexing_failed doc_id=%s", payload.doc_id)
        raise HTTPException(status_code=500, detail="Vector indexing failed.") from exc

    return VectorIndexDocumentResponse(
        status="indexed",
        doc_id=result.doc_id,
        buffered_documents=result.buffered_documents,
        indexed_documents=result.indexed_documents,
        persisted=result.persisted,
    )


@router.post("/search", response_model=VectorSearchResponse)
async def search(payload: VectorSearchRequest, request: Request) -> VectorSearchResponse:
    """Encode the query and run nearest-neighbor search against FAISS."""
    builder: VectorIndexBuilder = request.app.state.vector_index_builder
    try:
        results = builder.search(query=payload.query, top_k=payload.top_k)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - defensive API guard
        logger.exception("vector_search_failed query=%s", payload.query)
        raise HTTPException(status_code=500, detail="Vector search failed.") from exc

    return VectorSearchResponse(
        results=[VectorSearchResultItem(doc_id=result.doc_id, score=result.score) for result in results]
    )
