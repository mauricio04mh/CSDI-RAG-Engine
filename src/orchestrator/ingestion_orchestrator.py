from __future__ import annotations

import logging
from dataclasses import dataclass, field

from src.crawler.crawler import Crawler
from src.database.repositories.chunk_repository import ChunkRepository
from src.document_processing.chunker import Chunker, DocumentChunk
from src.indexing.builder.index_builder import IndexBuilder
from src.scraper.scraper import Scraper
from src.sources_config.source_config_repository import SourceConfigRepository
from src.vector_indexing.pipeline.vector_index_builder import VectorIndexBuilder

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
        index_builder: IndexBuilder,
        vector_index_builder: VectorIndexBuilder,
        chunk_repo: ChunkRepository,
        chunk_size: int = 256,
        chunk_overlap: int = 32,
        crawler_timeout: float = 15.0,
    ) -> None:
        self._source_repo = source_repo
        self._index_builder = index_builder
        self._vector_index_builder = vector_index_builder
        self._chunk_repo = chunk_repo
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

            existing_ids = self._chunk_repo.get_existing_chunk_ids([c.chunk_id for c in chunks])
            new_chunks = [c for c in chunks if c.chunk_id not in existing_ids]

            if not new_chunks:
                logger.debug("page_all_chunks_exist url=%s skipping=%s", doc.url, len(chunks))
                continue

            if existing_ids:
                logger.debug("page_partial_chunks_exist url=%s new=%s skipped=%s", doc.url, len(new_chunks), len(existing_ids))

            self._chunk_repo.save_chunks(new_chunks)

            for chunk in new_chunks:
                indexed = self._index_chunk(chunk)
                if indexed:
                    chunks_indexed += 1

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

    def _index_chunk(self, chunk: DocumentChunk) -> bool:
        tokens = chunk.text.lower().split()
        try:
            self._index_builder.add_document(doc_id=chunk.chunk_id, tokens=tokens)
            self._vector_index_builder.add_document(doc_id=chunk.chunk_id, text=chunk.text)
            return True
        except ValueError as exc:
            # Should not happen after pre-filtering — signals index/DB inconsistency
            logger.warning("chunk_index_conflict chunk_id=%s reason=%s", chunk.chunk_id, exc)
            return False
        except Exception:
            logger.exception("chunk_index_failed chunk_id=%s", chunk.chunk_id)
            return False
