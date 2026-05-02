from __future__ import annotations

import logging
from dataclasses import dataclass

from src.crawler.crawler import Crawler
from src.document_processing.chunker import Chunker
from src.ingestion.chunk_ingestion_service import ChunkIngestionService
from src.scraper.scraper import Scraper
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
    """Coordinates the full ingestion pipeline for one source:

        Crawler → Scraper → Chunker → IndexBuilder + VectorIndexBuilder
    """

    def __init__(
        self,
        source_repo: SourceConfigRepository,
        chunk_ingestion: ChunkIngestionService,
        chunk_size: int = 256,
        chunk_overlap: int = 32,
        crawler_timeout: float = 15.0,
    ) -> None:
        self._source_repo = source_repo
        self._chunk_ingestion = chunk_ingestion
        self._crawler = Crawler(timeout=crawler_timeout)
        self._scraper = Scraper()
        self._chunker = Chunker(chunk_size=chunk_size, chunk_overlap=chunk_overlap)

    def ingest(self, source_id: str) -> IngestionReport:
        """Run the full pipeline for a source and return a summary report."""
        source = self._source_repo.get_source(source_id)
        logger.info("ingestion_started source=%s", source_id)

        # 1. Crawl
        crawl_result = self._crawler.crawl(source)
        logger.info("crawl_done source=%s pages=%s", source_id, crawl_result.total)

        # 2. Scrape + chunk + index
        pages_scraped = 0
        chunks_produced = 0
        chunks_indexed = 0

        for page in crawl_result.pages:
            doc = self._scraper.parse(
                url=page.url,
                html=page.html,
                config=source.scraper,
                source_id=source_id,
            )
            if doc is None:
                continue

            pages_scraped += 1
            chunks = self._chunker.chunk(
                source_id=source_id,
                url=doc.url,
                title=doc.title,
                breadcrumb=doc.breadcrumb,
                content=doc.content,
            )
            chunks_produced += len(chunks)
            ingestion_result = self._chunk_ingestion.ingest_chunks(chunks)
            chunks_indexed += ingestion_result.indexed_chunks
            if ingestion_result.new_chunks == 0:
                logger.debug("page_all_chunks_exist url=%s skipping=%s", doc.url, len(chunks))
            elif ingestion_result.skipped_existing > 0:
                logger.debug(
                    "page_partial_chunks_exist url=%s new=%s skipped=%s",
                    doc.url,
                    ingestion_result.new_chunks,
                    ingestion_result.skipped_existing,
                )

        # Flush any vectors remaining in the batch buffer (last partial batch)
        flushed = self._chunk_ingestion.finalize(reload_bm25=False).vector_flushed
        if flushed:
            logger.debug("vector_buffer_flushed count=%s", flushed)

        report = IngestionReport(
            source_id=source_id,
            pages_crawled=crawl_result.total,
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
