from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from src.document_processing.chunker import Chunker
from src.web_search.fetchers.base import DocumentFetcher
from src.web_search.providers.base import SearchProvider
from src.web_search.schemas import WebSearchRunResult

if TYPE_CHECKING:
    from src.ingestion.chunk_ingestion_service import ChunkIngestionService

logger = logging.getLogger(__name__)
_SAFE_SOURCE = re.compile(r"[^a-z0-9.-]+")


@dataclass(slots=True)
class WebSearchSettings:
    enabled: bool = True
    top_k: int = 5


class WebSearchOrchestrator:
    def __init__(
        self,
        provider: SearchProvider,
        settings: WebSearchSettings,
        fetcher: DocumentFetcher | None = None,
        web_cache_ingestion: ChunkIngestionService | None = None,
        chunk_ingestion: ChunkIngestionService | None = None,
        chunker: Chunker | None = None,
        web_search_repo=None,
        web_cache_document_repo=None,
    ) -> None:
        self._provider = provider
        self._settings = settings
        self._fetcher = fetcher
        self._web_cache_ingestion = web_cache_ingestion or chunk_ingestion
        self._chunker = chunker or Chunker()
        self._web_search_repo = web_search_repo
        self._web_cache_document_repo = web_cache_document_repo

    @property
    def enabled(self) -> bool:
        return self._settings.enabled

    def run(self, query: str) -> WebSearchRunResult:
        if not self._settings.enabled:
            return WebSearchRunResult(
                query=query,
                hits=[],
                documents=[],
                indexed_count=0,
            )

        normalized_query = query.strip()
        if not normalized_query:
            logger.debug("web_search_skipped_empty_query")
            return WebSearchRunResult(
                query=query,
                hits=[],
                documents=[],
                indexed_count=0,
            )

        hits = self._provider.search(query=normalized_query, top_k=self._settings.top_k)

        logger.info(
            "web_search query=%s hits=%s",
            normalized_query,
            len(hits),
        )

        documents = self._fetch_documents(hits)
        indexed_count = self._ingest_documents(documents)
        self._persist_run(
            query=normalized_query,
            hits=hits,
            documents_count=len(documents),
            indexed_count=indexed_count,
        )

        return WebSearchRunResult(
            query=normalized_query,
            hits=hits,
            documents=documents,
            indexed_count=indexed_count,
        )

    def _fetch_documents(self, hits):
        if self._fetcher is None:
            return []
        documents = []
        for hit in hits:
            doc = self._fetcher.fetch(hit)
            if doc is None:
                continue
            documents.append(doc)
        return documents

    def _ingest_documents(self, documents) -> int:
        if not documents:
            return 0

        if self._web_cache_document_repo is not None:
            try:
                self._web_cache_document_repo.save_documents(documents)
            except Exception:
                logger.exception("web_cache_documents_persist_failed count=%s", len(documents))

        if self._web_cache_ingestion is None:
            return 0

        chunks = []
        for doc in documents:
            source_id = _source_id_from_url(doc.url, provider=str(doc.metadata.get("provider") or "web"))
            chunks.extend(
                self._chunker.chunk(
                    source_id=source_id,
                    url=doc.url,
                    title=doc.title,
                    breadcrumb="web-search",
                    content=doc.text,
                )
            )

        ingestion_result = self._web_cache_ingestion.ingest_chunks(chunks)
        if ingestion_result.indexed_chunks > 0:
            self._web_cache_ingestion.finalize(reload_bm25=True)
        return ingestion_result.indexed_chunks

    def _persist_run(
        self,
        *,
        query: str,
        hits,
        documents_count: int,
        indexed_count: int,
    ) -> None:
        if self._web_search_repo is None:
            return
        provider = hits[0].provider if hits and hits[0].provider else "unknown"
        try:
            self._web_search_repo.save_run(
                query=query,
                provider=provider,
                hits=hits,
                documents_count=documents_count,
                indexed_count=indexed_count,
            )
        except Exception:
            logger.exception("web_search_run_persist_failed query=%s", query)


def _source_id_from_url(url: str, *, provider: str) -> str:
    parsed = urlparse(url)
    domain = parsed.netloc.lower() or "unknown-domain"
    safe_provider = _SAFE_SOURCE.sub("-", provider.lower()).strip("-") or "web"
    safe_domain = _SAFE_SOURCE.sub("-", domain).strip("-") or "unknown-domain"
    return f"web:{safe_provider}:{safe_domain}"
