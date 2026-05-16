from __future__ import annotations

import sys
from dataclasses import dataclass
from types import SimpleNamespace

try:
    import httpx  # noqa: F401
except ModuleNotFoundError:
    class _HTTPError(Exception):
        pass

    class _Client:
        def __init__(self, *args, **kwargs) -> None:
            pass

    sys.modules["httpx"] = SimpleNamespace(HTTPError=_HTTPError, Client=_Client)

if "snowballstemmer" not in sys.modules:
    sys.modules["snowballstemmer"] = SimpleNamespace(
        stemmer=lambda _language: SimpleNamespace(stemWord=lambda token: token)
    )

from src.crawler.crawler import CrawledPage, CrawlResult
from src.document_processing.chunker import DocumentChunk
from src.orchestrator.ingestion_orchestrator import IngestionOrchestrator
from src.scraper.scraper import ScrapedDocument
from src.sources_config.schemas import ScraperConfig, SourceConfig


def make_source() -> SourceConfig:
    return SourceConfig(
        source_id="docs",
        name="Docs",
        base_url="https://example.com",
        allowed_domains=["example.com"],
        seed_urls=["https://example.com/docs/start"],
        allowed_path_prefixes=["/docs/"],
        blocked_path_patterns=[],
        max_depth=1,
        use_browser_fallback=False,
        technology=["python"],
        scraper=ScraperConfig(),
    )


class _FakeSourceRepo:
    def __init__(self, source: SourceConfig) -> None:
        self.source = source
        self.calls: list[str] = []

    def get_source(self, source_id: str) -> SourceConfig:
        self.calls.append(source_id)
        return self.source


class _FakeCrawler:
    def __init__(self, result: CrawlResult) -> None:
        self.result = result
        self.calls: list[SourceConfig] = []

    def crawl(self, source: SourceConfig) -> CrawlResult:
        self.calls.append(source)
        return self.result


class _FakeCrawlStageService:
    def __init__(self, crawler: _FakeCrawler) -> None:
        self._crawler = crawler

    def crawl(self, source: SourceConfig) -> CrawlResult:
        return self._crawler.crawl(source)


class _FakeScraper:
    def __init__(self, docs_by_url: dict[str, ScrapedDocument | None]) -> None:
        self.docs_by_url = docs_by_url
        self.calls: list[str] = []

    def parse(self, *, url: str, html: str, config: ScraperConfig, source_id: str):
        self.calls.append(url)
        return self.docs_by_url[url]


class _FakeScrapeStageService:
    def __init__(self, scraper: _FakeScraper) -> None:
        self._scraper = scraper

    def scrape_page(self, *, page: CrawledPage, source: SourceConfig):
        return self._scraper.parse(
            url=page.url,
            html=page.html,
            config=source.scraper,
            source_id=source.source_id,
        )


class _FakeChunker:
    def __init__(self, chunks_by_url: dict[str, list[DocumentChunk]]) -> None:
        self.chunks_by_url = chunks_by_url
        self.calls: list[str] = []

    def chunk(
        self,
        *,
        source_id: str,
        url: str,
        title: str,
        breadcrumb: str,
        content: str,
    ) -> list[DocumentChunk]:
        self.calls.append(url)
        return self.chunks_by_url[url]


class _FakeChunkPreparationService:
    def __init__(self, chunker: _FakeChunker) -> None:
        self._chunker = chunker

    def build_chunks(self, doc: ScrapedDocument) -> list[DocumentChunk]:
        return self._chunker.chunk(
            source_id=doc.source_id,
            url=doc.url,
            title=doc.title,
            breadcrumb=doc.breadcrumb,
            content=doc.content,
        )


class _FakeSourceDocumentRepo:
    def __init__(self) -> None:
        self.calls: list[object] = []

    def save_document(self, payload) -> str:
        self.calls.append(payload)
        return payload.document_id


class _FakeSourceDocumentPersistenceService:
    def __init__(self, repo: _FakeSourceDocumentRepo) -> None:
        self._repo = repo

    def persist(self, *, page: CrawledPage, doc: ScrapedDocument) -> str:
        payload = SimpleNamespace(
            document_id=f"{doc.source_id}:persisted",
            normalized_url=page.url,
            content_type=page.content_type,
            crawl_depth=page.depth,
            discovered_from_url=page.discovered_from_url,
        )
        return self._repo.save_document(payload)


@dataclass
class _FakeChunkIngestion:
    results: list[object]
    ingest_calls: list[list[DocumentChunk]]
    finalize_calls: list[bool]

    def __init__(self, results: list[object]) -> None:
        self.results = list(results)
        self.ingest_calls = []
        self.finalize_calls = []

    def ingest_chunks(self, chunks: list[DocumentChunk]):
        self.ingest_calls.append(chunks)
        return self.results.pop(0)

    def finalize(self, *, reload_bm25: bool = False):
        self.finalize_calls.append(reload_bm25)
        return SimpleNamespace(vector_flushed=0, bm25_segment_id=None)


class _FakeChunkIndexingStageService:
    def __init__(self, chunk_ingestion: _FakeChunkIngestion) -> None:
        self._chunk_ingestion = chunk_ingestion

    def ingest(self, chunks: list[DocumentChunk]):
        return self._chunk_ingestion.ingest_chunks(chunks)

    def finalize(self, *, reload_bm25: bool = False):
        return self._chunk_ingestion.finalize(reload_bm25=reload_bm25)


def make_chunk(url: str, index: int) -> DocumentChunk:
    return DocumentChunk(
        chunk_id=f"docs:{index}",
        source_id="docs",
        url=url,
        title="Title",
        breadcrumb="Home > Docs",
        text=f"chunk {index}",
    )


def test_ingest_reports_counts_and_skips_unscrapable_pages() -> None:
    source = make_source()
    pages = [
        CrawledPage(url="https://example.com/docs/one", html="<html>one</html>", status_code=200, content_type="text/html", depth=0),
        CrawledPage(url="https://example.com/docs/two", html="<html>two</html>", status_code=200, content_type="text/html", depth=1, discovered_from_url="https://example.com/docs/one"),
    ]
    docs_by_url = {
        "https://example.com/docs/one": ScrapedDocument(
            url="https://example.com/docs/one",
            title="One",
            content="content one",
            breadcrumb="Home > One",
            code_blocks=[],
            source_id="docs",
        ),
        "https://example.com/docs/two": None,
    }
    chunks_by_url = {
        "https://example.com/docs/one": [
            make_chunk("https://example.com/docs/one", 0),
            make_chunk("https://example.com/docs/one", 1),
        ]
    }
    chunk_ingestion = _FakeChunkIngestion(
        results=[
            SimpleNamespace(
                total_chunks=2,
                new_chunks=2,
                indexed_chunks=2,
                skipped_existing=0,
            )
        ]
    )
    source_document_repo = _FakeSourceDocumentRepo()
    orchestrator = IngestionOrchestrator(
        source_repo=_FakeSourceRepo(source),
        chunk_ingestion=chunk_ingestion,
        source_document_repo=source_document_repo,
    )
    orchestrator._crawl_service = _FakeCrawlStageService(_FakeCrawler(CrawlResult(source_id="docs", pages=pages)))
    orchestrator._scrape_service = _FakeScrapeStageService(_FakeScraper(docs_by_url))
    orchestrator._source_document_service = _FakeSourceDocumentPersistenceService(source_document_repo)
    orchestrator._chunk_preparation_service = _FakeChunkPreparationService(_FakeChunker(chunks_by_url))
    orchestrator._chunk_indexing_service = _FakeChunkIndexingStageService(chunk_ingestion)

    report = orchestrator.ingest("docs")

    assert report.source_id == "docs"
    assert report.pages_crawled == 2
    assert report.pages_scraped == 1
    assert report.chunks_produced == 2
    assert report.chunks_indexed == 2
    assert len(chunk_ingestion.ingest_calls) == 1
    assert chunk_ingestion.finalize_calls == [False]
    assert len(source_document_repo.calls) == 1
    assert source_document_repo.calls[0].normalized_url == "https://example.com/docs/one"
    assert source_document_repo.calls[0].content_type == "text/html"
    assert source_document_repo.calls[0].crawl_depth == 0


def test_ingest_accumulates_chunk_counts_across_pages() -> None:
    source = make_source()
    pages = [
        CrawledPage(url="https://example.com/docs/one", html="<html>one</html>", status_code=200, content_type="text/html", depth=0),
        CrawledPage(url="https://example.com/docs/two", html="<html>two</html>", status_code=200, content_type="text/html", depth=1, discovered_from_url="https://example.com/docs/one"),
    ]
    docs_by_url = {
        "https://example.com/docs/one": ScrapedDocument(
            url="https://example.com/docs/one",
            title="One",
            content="content one",
            breadcrumb="Home > One",
            code_blocks=[],
            source_id="docs",
        ),
        "https://example.com/docs/two": ScrapedDocument(
            url="https://example.com/docs/two",
            title="Two",
            content="content two",
            breadcrumb="Home > Two",
            code_blocks=[],
            source_id="docs",
        ),
    }
    chunks_by_url = {
        "https://example.com/docs/one": [
            make_chunk("https://example.com/docs/one", 0),
            make_chunk("https://example.com/docs/one", 1),
        ],
        "https://example.com/docs/two": [make_chunk("https://example.com/docs/two", 2)],
    }
    chunk_ingestion = _FakeChunkIngestion(
        results=[
            SimpleNamespace(
                total_chunks=2,
                new_chunks=2,
                indexed_chunks=1,
                skipped_existing=1,
            ),
            SimpleNamespace(
                total_chunks=1,
                new_chunks=0,
                indexed_chunks=0,
                skipped_existing=1,
            ),
        ]
    )
    source_document_repo = _FakeSourceDocumentRepo()
    orchestrator = IngestionOrchestrator(
        source_repo=_FakeSourceRepo(source),
        chunk_ingestion=chunk_ingestion,
        source_document_repo=source_document_repo,
    )
    orchestrator._crawl_service = _FakeCrawlStageService(_FakeCrawler(CrawlResult(source_id="docs", pages=pages)))
    orchestrator._scrape_service = _FakeScrapeStageService(_FakeScraper(docs_by_url))
    orchestrator._source_document_service = _FakeSourceDocumentPersistenceService(source_document_repo)
    orchestrator._chunk_preparation_service = _FakeChunkPreparationService(_FakeChunker(chunks_by_url))
    orchestrator._chunk_indexing_service = _FakeChunkIndexingStageService(chunk_ingestion)

    report = orchestrator.ingest("docs")

    assert report.pages_crawled == 2
    assert report.pages_scraped == 2
    assert report.chunks_produced == 3
    assert report.chunks_indexed == 1
    assert len(chunk_ingestion.ingest_calls) == 2
    assert chunk_ingestion.finalize_calls == [False]
    assert len(source_document_repo.calls) == 2
    assert source_document_repo.calls[1].discovered_from_url == "https://example.com/docs/one"
