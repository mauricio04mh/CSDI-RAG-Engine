from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from src.bm25.pipeline.bm25_retriever import BM25Retriever
from src.orchestrator.ingestion_orchestrator import IngestionOrchestrator

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/ingest", tags=["ingestion"])


class IngestSourceRequest(BaseModel):
    source_id: str = Field(..., min_length=1)


class IngestResponse(BaseModel):
    source_id: str
    status: str
    pages_crawled: int
    pages_scraped: int
    chunks_produced: int
    chunks_indexed: int


@router.post("", response_model=IngestResponse)
def ingest_source(payload: IngestSourceRequest, request: Request) -> IngestResponse:
    """Crawl, scrape, chunk and index all pages for a configured source."""
    orchestrator: IngestionOrchestrator = request.app.state.ingestion_orchestrator
    source_repo = request.app.state.source_repo

    if not source_repo.exists(payload.source_id):
        raise HTTPException(status_code=404, detail=f"Source '{payload.source_id}' not found.")

    try:
        report = orchestrator.ingest(payload.source_id)
    except Exception as exc:
        logger.exception("ingestion_failed source=%s", payload.source_id)
        raise HTTPException(status_code=500, detail="Ingestion failed.") from exc

    # Reload BM25 in-memory index so new segments are visible to search immediately
    if report.chunks_indexed > 0:
        bm25_retriever: BM25Retriever = request.app.state.bm25_retriever
        bm25_retriever.reload()
        logger.info("bm25_reloaded after_ingest source=%s new_chunks=%s", payload.source_id, report.chunks_indexed)

    return IngestResponse(
        source_id=report.source_id,
        status="completed",
        pages_crawled=report.pages_crawled,
        pages_scraped=report.pages_scraped,
        chunks_produced=report.chunks_produced,
        chunks_indexed=report.chunks_indexed,
    )


@router.get("/sources", tags=["ingestion"])
def list_sources(request: Request) -> list[dict]:
    """List all configured ingestion sources."""
    source_repo = request.app.state.source_repo
    return [
        {
            "source_id": s.source_id,
            "name": s.name,
            "base_url": s.base_url,
            "technology": s.technology,
            "seed_urls": s.seed_urls,
            "max_depth": s.max_depth,
        }
        for s in source_repo.list_sources()
    ]
