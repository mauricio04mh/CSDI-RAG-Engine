from __future__ import annotations

from typing import TYPE_CHECKING

from src.crawler.crawler import CrawlResult, CrawledPage, Crawler
from src.document_processing.chunker import Chunker, DocumentChunk
from src.ingestion.chunk_ingestion_service import (
    ChunkIngestionFinalizeResult,
    ChunkIngestionResult,
    ChunkIngestionService,
)
from src.ingestion.source_documents import SourceDocumentInput
from src.scraper.scraper import ScrapedDocument, Scraper
from src.sources_config.schemas import SourceConfig

if TYPE_CHECKING:
    from src.database.repositories.source_document_repository import SourceDocumentRepository


class CrawlStageService:
    def __init__(self, crawler: Crawler) -> None:
        self._crawler = crawler

    def crawl(self, source: SourceConfig) -> CrawlResult:
        return self._crawler.crawl(source)


class ScrapeStageService:
    def __init__(self, scraper: Scraper) -> None:
        self._scraper = scraper

    def scrape_page(
        self,
        *,
        page: CrawledPage,
        source: SourceConfig,
    ) -> ScrapedDocument | None:
        return self._scraper.parse(
            url=page.url,
            html=page.html,
            config=source.scraper,
            source_id=source.source_id,
        )


class SourceDocumentPersistenceService:
    def __init__(self, source_document_repo: SourceDocumentRepository | None = None) -> None:
        self._source_document_repo = source_document_repo

    def persist(self, *, page: CrawledPage, doc: ScrapedDocument) -> str | None:
        if self._source_document_repo is None:
            return None

        payload = SourceDocumentInput(
            source_id=doc.source_id,
            url=page.url,
            normalized_url=page.url,
            title=doc.title,
            breadcrumb=doc.breadcrumb,
            text_content=doc.content,
            raw_html=page.html,
            code_blocks=list(doc.code_blocks),
            content_type=page.content_type,
            http_status=page.status_code,
            fetch_method="http",
            crawl_depth=page.depth,
            discovered_from_url=page.discovered_from_url,
        )
        return self._source_document_repo.save_document(payload)


class ChunkPreparationService:
    def __init__(self, chunker: Chunker) -> None:
        self._chunker = chunker

    def build_chunks(self, doc: ScrapedDocument) -> list[DocumentChunk]:
        return self._chunker.chunk(
            source_id=doc.source_id,
            url=doc.url,
            title=doc.title,
            breadcrumb=doc.breadcrumb,
            content=doc.content,
        )


class ChunkIndexingStageService:
    def __init__(self, chunk_ingestion: ChunkIngestionService) -> None:
        self._chunk_ingestion = chunk_ingestion

    def ingest(self, chunks: list[DocumentChunk]) -> ChunkIngestionResult:
        return self._chunk_ingestion.ingest_chunks(chunks)

    def finalize(self, *, reload_bm25: bool = False) -> ChunkIngestionFinalizeResult:
        return self._chunk_ingestion.finalize(reload_bm25=reload_bm25)
