from __future__ import annotations

import logging
from dataclasses import dataclass

from src.crawler.crawler import Crawler
from src.document_processing.chunker import Chunker
from src.ingestion.chunk_ingestion_service import ChunkIngestionService
from src.ingestion.pipeline_services import (
    ChunkIndexingStageService,
    ChunkPreparationService,
    CrawlStageService,
    ScrapeStageService,
    SourceDocumentPersistenceService,
)
from src.ingestion.progress_tracker import IngestionTracker
from src.scraper.scraper import Scraper
from src.sources_config.schemas import SourceConfig
from src.sources_config.source_config_repository import SourceConfigRepository

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class IngestionReport:
    source_id: str
    pages_crawled: int
    pages_scraped: int
    chunks_produced: int
    chunks_indexed: int


class IngestionOrchestrator:
    """Coordinates the full ingestion pipeline for one source.

    Stages:
        crawl -> scrape -> persist source document -> build chunks -> index chunks
    """

    def __init__(
        self,
        source_repo: SourceConfigRepository,
        chunk_ingestion: ChunkIngestionService,
        source_document_repo=None,
        chunk_size: int = 256,
        chunk_overlap: int = 32,
        crawler_timeout: float = 15.0,
        tracker: IngestionTracker | None = None,
    ) -> None:
        self._source_repo = source_repo
        self._tracker = tracker
        crawler = Crawler(timeout=crawler_timeout)
        scraper = Scraper()
        chunker = Chunker(chunk_size=chunk_size, chunk_overlap=chunk_overlap)

        self._crawl_service = CrawlStageService(crawler)
        self._scrape_service = ScrapeStageService(scraper)
        self._source_document_service = SourceDocumentPersistenceService(source_document_repo)
        self._chunk_preparation_service = ChunkPreparationService(chunker)
        self._chunk_indexing_service = ChunkIndexingStageService(chunk_ingestion)

    def ingest(self, source_id: str) -> IngestionReport:
        """Run the full pipeline for a configured source and return a summary report."""
        source = self._source_repo.get_source(source_id)
        return self.ingest_source(source)

    def ingest_source(self, source: SourceConfig) -> IngestionReport:
        """Run the full pipeline for an explicit SourceConfig and return a summary report."""
        source_id = source.source_id
        tracker = self._tracker
        logger.info("ingestion_started source=%s", source_id)

        if tracker:
            tracker.start(source_id)

        pages_crawled = 0
        pages_scraped = 0
        chunks_produced = 0
        chunks_indexed = 0
        indexing_started = False

        for page, pages_estimate in self._crawl_service.crawl_iter(source):
            pages_crawled += 1

            if tracker:
                if not indexing_started:
                    tracker.set_pages_total(source_id, pages_estimate)
                    indexing_started = True
                # Advance progress immediately on crawl so the UI updates at ~1 req/sec
                # rather than waiting for the slow embed+index pipeline (large pages can
                # take minutes to index, making progress appear frozen at 1%).
                tracker.page_crawled(source_id, pages_estimate)

            doc = self._scrape_service.scrape_page(page=page, source=source)
            if doc is None:
                continue

            pages_scraped += 1
            document_id = self._source_document_service.persist(page=page, doc=doc)
            if document_id is not None:
                logger.debug(
                    "source_document_persisted source=%s document_id=%s",
                    doc.source_id,
                    document_id,
                )

            chunks = self._chunk_preparation_service.build_chunks(doc)
            chunks_produced += len(chunks)
            ingestion_result = self._chunk_indexing_service.ingest(chunks)
            chunks_indexed += ingestion_result.indexed_chunks

            if tracker:
                tracker.add_indexed_chunks(source_id, ingestion_result.indexed_chunks)

            if ingestion_result.new_chunks == 0:
                logger.debug("page_all_chunks_exist url=%s skipping=%s", doc.url, len(chunks))
            elif ingestion_result.skipped_existing > 0:
                logger.debug(
                    "page_partial_chunks_exist url=%s new=%s skipped=%s",
                    doc.url,
                    ingestion_result.new_chunks,
                    ingestion_result.skipped_existing,
                )

        flushed = self._chunk_indexing_service.finalize(reload_bm25=False).vector_flushed
        if flushed:
            logger.debug("vector_buffer_flushed count=%s", flushed)

        report = IngestionReport(
            source_id=source_id,
            pages_crawled=pages_crawled,
            pages_scraped=pages_scraped,
            chunks_produced=chunks_produced,
            chunks_indexed=chunks_indexed,
        )
        logger.info(
            "ingestion_finished source=%s pages_crawled=%s scraped=%s chunks=%s indexed=%s",
            source_id,
            report.pages_crawled,
            report.pages_scraped,
            report.chunks_produced,
            report.chunks_indexed,
        )
        return report
