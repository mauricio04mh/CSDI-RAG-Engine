from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from src.indexing.builder.index_builder import IndexBuilder

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/indexing", tags=["indexing"])


class IndexDocumentRequest(BaseModel):
    """Payload for indexing one tokenized document."""

    doc_id: str = Field(..., min_length=1, description="Unique document identifier.")
    tokens: list[str] = Field(..., min_length=1, description="Preprocessed document tokens.")


class IndexDocumentResponse(BaseModel):
    """API response after indexing a document."""

    status: str
    doc_id: str
    buffered_documents: int
    flushed: bool
    segment_id: str | None = None


class MergeOperationResponse(BaseModel):
    """Details for one merge operation."""

    merged_segment_id: str
    source_segment_ids: list[str]
    documents_merged: int
    terms_merged: int


class MergeResponse(BaseModel):
    """API response for a merge request."""

    status: str
    total_merges: int
    operations: list[MergeOperationResponse]


@router.post("/index", response_model=IndexDocumentResponse)
def index_document(payload: IndexDocumentRequest, request: Request) -> IndexDocumentResponse:
    """Receive a tokenized document and add it to the active BM25 index segment."""
    index_builder: IndexBuilder = request.app.state.index_builder
    try:
        result = index_builder.add_document(doc_id=payload.doc_id, tokens=payload.tokens)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - defensive API guard
        logger.exception("indexing_failed doc_id=%s", payload.doc_id)
        raise HTTPException(status_code=500, detail="Indexing failed.") from exc

    return IndexDocumentResponse(
        status="indexed",
        doc_id=result.doc_id,
        buffered_documents=result.buffered_documents,
        flushed=result.flushed,
        segment_id=result.segment_id,
    )


@router.post("/merge", response_model=MergeResponse)
def merge_segments(request: Request) -> MergeResponse:
    """Trigger a manual merge for persisted BM25 index segments."""
    index_builder: IndexBuilder = request.app.state.index_builder
    try:
        result = index_builder.merge_segments()
    except Exception as exc:  # pragma: no cover - defensive API guard
        logger.exception("segment_merge_failed")
        raise HTTPException(status_code=500, detail="Segment merge failed.") from exc

    return MergeResponse(
        status="merged",
        total_merges=result.total_merges,
        operations=[
            MergeOperationResponse(
                merged_segment_id=merge_result.merged_segment_id,
                source_segment_ids=merge_result.source_segment_ids,
                documents_merged=merge_result.documents_merged,
                terms_merged=merge_result.terms_merged,
            )
            for merge_result in result.merges
        ],
    )
