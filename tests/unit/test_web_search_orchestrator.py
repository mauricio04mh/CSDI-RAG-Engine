from __future__ import annotations

from src.document_processing.chunker import Chunker
from src.web_search.orchestrator import WebSearchOrchestrator, WebSearchSettings
from src.web_search.schemas import WebSearchDocument, WebSearchHit


class _FakeProvider:
    def __init__(self, hits: list[WebSearchHit]) -> None:
        self.hits = hits
        self.calls: list[tuple[str, int]] = []

    def search(self, query: str, top_k: int) -> list[WebSearchHit]:
        self.calls.append((query, top_k))
        return self.hits


class _FakeFetcher:
    def __init__(self, docs_by_url: dict[str, WebSearchDocument | None]) -> None:
        self.docs_by_url = docs_by_url
        self.calls: list[str] = []

    def fetch(self, hit: WebSearchHit) -> WebSearchDocument | None:
        self.calls.append(hit.url)
        return self.docs_by_url.get(hit.url)


class _FakeChunkIngestion:
    def __init__(self, indexed_chunks: int) -> None:
        self.indexed_chunks = indexed_chunks
        self.ingest_calls = 0
        self.finalize_calls = 0

    def ingest_chunks(self, chunks):
        self.ingest_calls += 1
        return _IngestResult(indexed_chunks=self.indexed_chunks)

    def finalize(self, *, reload_bm25: bool = False):
        self.finalize_calls += 1
        return None


class _IngestResult:
    def __init__(self, indexed_chunks: int) -> None:
        self.indexed_chunks = indexed_chunks


class _FakeWebSearchRepo:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def save_run(self, **kwargs) -> int:
        self.calls.append(kwargs)
        return 1


def test_orchestrator_returns_empty_when_disabled() -> None:
    provider = _FakeProvider(hits=[WebSearchHit(title="t", url="u", snippet="s")])
    orchestrator = WebSearchOrchestrator(
        provider=provider,
        settings=WebSearchSettings(enabled=False, top_k=3),
    )

    result = orchestrator.run("python")

    assert result.query == "python"
    assert result.hits == []
    assert result.indexed_count == 0
    assert provider.calls == []


def test_orchestrator_calls_provider_with_top_k() -> None:
    hits = [WebSearchHit(title="t", url="u", snippet="s")]
    provider = _FakeProvider(hits=hits)
    orchestrator = WebSearchOrchestrator(
        provider=provider,
        settings=WebSearchSettings(enabled=True, top_k=7),
    )

    result = orchestrator.run(" python decorators ")

    assert provider.calls == [("python decorators", 7)]
    assert result.query == "python decorators"
    assert result.hits == hits
    assert result.documents == []
    assert result.indexed_count == 0


def test_orchestrator_skips_empty_query() -> None:
    provider = _FakeProvider(hits=[WebSearchHit(title="t", url="u", snippet="s")])
    orchestrator = WebSearchOrchestrator(
        provider=provider,
        settings=WebSearchSettings(enabled=True, top_k=3),
    )

    result = orchestrator.run("   ")

    assert result.query == "   "
    assert result.hits == []
    assert provider.calls == []


def test_orchestrator_fetches_documents_ingests_and_persists_run() -> None:
    hits = [WebSearchHit(title="t", url="https://example.com", snippet="s", provider="duckduckgo")]
    provider = _FakeProvider(hits=hits)
    fetcher = _FakeFetcher(
        docs_by_url={
            "https://example.com": WebSearchDocument(
                url="https://example.com",
                title="Doc",
                text="Python decorators explained",
                metadata={"provider": "duckduckgo"},
            )
        }
    )
    chunk_ingestion = _FakeChunkIngestion(indexed_chunks=2)
    repo = _FakeWebSearchRepo()
    orchestrator = WebSearchOrchestrator(
        provider=provider,
        settings=WebSearchSettings(enabled=True, top_k=3),
        fetcher=fetcher,
        chunk_ingestion=chunk_ingestion,
        chunker=Chunker(chunk_size=2, chunk_overlap=1),
        web_search_repo=repo,
    )

    result = orchestrator.run("python decorators")

    assert result.query == "python decorators"
    assert len(result.hits) == 1
    assert len(result.documents) == 1
    assert result.indexed_count == 2
    assert fetcher.calls == ["https://example.com"]
    assert chunk_ingestion.ingest_calls == 1
    assert chunk_ingestion.finalize_calls == 1
    assert len(repo.calls) == 1
    assert repo.calls[0]["query"] == "python decorators"
    assert repo.calls[0]["documents_count"] == 1
    assert repo.calls[0]["indexed_count"] == 2
