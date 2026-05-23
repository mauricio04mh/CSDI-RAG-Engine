from __future__ import annotations

import hashlib
import logging
from urllib.parse import urlparse

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from src.bm25.pipeline.bm25_retriever import BM25Retriever
from src.ingestion.progress_tracker import IngestionTracker
from src.orchestrator.ingestion_orchestrator import IngestionOrchestrator
from src.sources_config.schemas import ScraperConfig, SourceConfig

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


class DeindexResponse(BaseModel):
    source_id: str
    chunks_deleted: int
    vectors_deleted: int
    documents_deleted: int


class IngestProgressResponse(BaseModel):
    source_id: str
    phase: str
    pages_total: int
    pages_scraped: int
    chunks_indexed: int
    progress_pct: float
    started_at: str | None
    finished_at: str | None
    last_ingest_at: str | None
    error: str | None


def _is_http_url(value: str) -> bool:
    parsed = urlparse(value)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _build_dynamic_source(url: str) -> SourceConfig:
    parsed = urlparse(url)
    normalized_url = parsed._replace(fragment="").geturl()
    host = parsed.netloc.lower()
    path_prefix = parsed.path or "/"
    source_hash = hashlib.sha1(normalized_url.encode("utf-8")).hexdigest()[:10]
    source_id = f"url:{host}:{source_hash}"

    return SourceConfig(
        source_id=source_id,
        name=f"URL Source {host}",
        base_url=f"{parsed.scheme}://{host}",
        allowed_domains=[host],
        seed_urls=[normalized_url],
        allowed_path_prefixes=[path_prefix],
        blocked_path_patterns=[],
        max_depth=0,
        technology=["ad_hoc_url"],
        respect_robots=True,
        max_pages=1,
        scraper=ScraperConfig(
            title_selectors=["h1", "title"],
            main_content_selectors=["main", "article", "body"],
            breadcrumb_selectors=[],
            code_block_selectors=["pre code", "code"],
            exclude_selectors=["script", "style", "noscript"],
        ),
    )


@router.post("", response_model=IngestResponse)
def ingest_source(payload: IngestSourceRequest, request: Request) -> IngestResponse:
    """Crawl, scrape, chunk and index all pages for a configured source."""
    orchestrator: IngestionOrchestrator = request.app.state.ingestion_orchestrator
    tracker: IngestionTracker = request.app.state.ingestion_tracker
    source_repo = request.app.state.source_repo
    user_source_repo = request.app.state.user_source_repo

    # For URL manual sources the tracker runs under the dynamic source_id, not the raw URL.
    effective_source_id = payload.source_id

    try:
        if source_repo.exists(payload.source_id):
            report = orchestrator.ingest(payload.source_id)
        elif _is_http_url(payload.source_id):
            dynamic_source = _build_dynamic_source(payload.source_id)
            effective_source_id = dynamic_source.source_id
            report = orchestrator.ingest_source(dynamic_source)
            user_source_repo.register(
                source_id=dynamic_source.source_id,
                name=dynamic_source.name,
                base_url=payload.source_id,
                source_kind="url_manual",
            )
        else:
            raise HTTPException(status_code=404, detail=f"Source '{payload.source_id}' not found.")
    except HTTPException:
        tracker.fail(effective_source_id, "not_found")
        raise
    except Exception as exc:
        tracker.fail(effective_source_id, str(exc))
        logger.exception("ingestion_failed source=%s", effective_source_id)
        raise HTTPException(status_code=500, detail="Ingestion failed.") from exc

    tracker.complete(effective_source_id)

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


@router.delete("/sources/{source_id}", response_model=DeindexResponse)
def deindex_source(source_id: str, request: Request) -> DeindexResponse:
    """Remove all indexed data for a source (chunks, vectors, source documents).

    Configured sources remain in the config but are reset to 0 chunks.
    User-added sources are fully removed from the system.
    """
    chunk_repo = request.app.state.chunk_repo
    source_document_repo = request.app.state.source_document_repo
    vector_index_builder = request.app.state.vector_index_builder
    index_builder = request.app.state.index_builder
    bm25_retriever: BM25Retriever = request.app.state.bm25_retriever
    tracker: IngestionTracker = request.app.state.ingestion_tracker
    user_source_repo = request.app.state.user_source_repo
    source_repo = request.app.state.source_repo

    is_configured = source_repo.exists(source_id)
    is_user_source = any(s.source_id == source_id for s in user_source_repo.list_sources())

    if not is_configured and not is_user_source:
        raise HTTPException(status_code=404, detail=f"Source '{source_id}' not found.")

    # 1. Collect chunk_ids before deleting (needed for vector soft-delete)
    chunk_ids = chunk_repo.get_chunk_ids_by_source_id(source_id)

    # 2. Soft-delete vectors and tombstone in-memory (instant effect on searches)
    vectors_deleted = vector_index_builder.remove_documents(chunk_ids) if chunk_ids else 0

    # 3. Clean up BM25 postings and doc_lengths for these chunk_ids
    if chunk_ids:
        index_builder.delete_documents(chunk_ids)

    # 4. Hard-delete chunks and source documents
    chunks_deleted = chunk_repo.delete_by_source_id(source_id)
    documents_deleted = source_document_repo.delete_by_source_id(source_id)

    # 5. Remove user source entry if applicable
    if is_user_source:
        user_source_repo.remove_source(source_id)

    # 6. Clear ingestion history
    tracker.clear_source(source_id)

    # 7. Reload BM25 in-memory index (now without the deleted docs)
    bm25_retriever.reload()

    logger.info(
        "source_deindexed source=%s chunks=%s vectors=%s documents=%s",
        source_id, chunks_deleted, vectors_deleted, documents_deleted,
    )

    return DeindexResponse(
        source_id=source_id,
        chunks_deleted=chunks_deleted,
        vectors_deleted=vectors_deleted,
        documents_deleted=documents_deleted,
    )


@router.get("/progress/{source_id}", response_model=IngestProgressResponse)
def get_ingest_progress(source_id: str, request: Request) -> IngestProgressResponse:
    """Return the current ingestion progress for a source."""
    tracker: IngestionTracker = request.app.state.ingestion_tracker
    data = tracker.to_dict(source_id)
    return IngestProgressResponse(**data)


@router.get("/sources", tags=["ingestion"])
def list_sources(request: Request) -> list[dict]:
    """List all configured ingestion sources plus user-added sources."""
    source_repo = request.app.state.source_repo
    chunk_repo = request.app.state.chunk_repo
    tracker: IngestionTracker = request.app.state.ingestion_tracker
    user_source_repo = request.app.state.user_source_repo

    configured = source_repo.list_sources()
    user_sources = user_source_repo.list_sources()

    all_ids = [s.source_id for s in configured] + [s.source_id for s in user_sources]
    counts = chunk_repo.count_by_source_ids(all_ids)

    result = [
        {
            "source_id": s.source_id,
            "name": s.name,
            "base_url": s.base_url,
            "technology": s.technology,
            "seed_urls": s.seed_urls,
            "max_depth": s.max_depth,
            "indexed_chunks": counts.get(s.source_id, 0),
            "last_ingest_at": tracker.last_ingest_at(s.source_id),
            "ingest_status": tracker.to_dict(s.source_id)["phase"],
            "progress_pct": tracker.to_dict(s.source_id)["progress_pct"],
            "source_kind": "configured",
        }
        for s in configured
    ]

    for s in user_sources:
        result.append({
            "source_id": s.source_id,
            "name": s.name,
            "base_url": s.base_url,
            "technology": [],
            "seed_urls": [],
            "max_depth": 0,
            "indexed_chunks": counts.get(s.source_id, 0),
            "last_ingest_at": tracker.last_ingest_at(s.source_id),
            "ingest_status": tracker.to_dict(s.source_id)["phase"],
            "progress_pct": tracker.to_dict(s.source_id)["progress_pct"],
            "source_kind": s.source_kind,
        })

    return result
