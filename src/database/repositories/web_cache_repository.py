from __future__ import annotations

import hashlib
import logging

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from src.database.models.web_cache_models import (
    WebCacheBM25DocLength,
    WebCacheBM25Posting,
    WebCacheBM25Segment,
    WebCacheBM25Term,
    WebCacheChunk,
    WebCacheDocument,
    WebCacheVectorDocument,
    WebCacheVectorIndexMetadata,
)
from src.database.repositories.bm25_repository import BM25Repository
from src.database.repositories.chunk_repository import ChunkRepository
from src.database.repositories.vector_repository import VectorRepository
from src.web_search.schemas import WebSearchDocument

logger = logging.getLogger(__name__)


class WebCacheDocumentRepository:
    """Persistence operations for fetched web documents used as cache."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def save_documents(self, documents: list[WebSearchDocument]) -> None:
        if not documents:
            return

        rows = []
        for doc in documents:
            provider = str(doc.metadata.get("provider") or "web")
            rows.append(
                {
                    "url": doc.url,
                    "title": doc.title,
                    "text_content": doc.text,
                    "provider": provider,
                    "content_hash": hashlib.sha256(doc.text.encode("utf-8")).hexdigest(),
                    "metadata": dict(doc.metadata),
                }
            )

        stmt = pg_insert(WebCacheDocument).values(rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=["url"],
            set_={
                "title": stmt.excluded.title,
                "text_content": stmt.excluded.text_content,
                "provider": stmt.excluded.provider,
                "content_hash": stmt.excluded.content_hash,
                "metadata": stmt.excluded["metadata"],
                "fetched_at": func.now(),
                "updated_at": func.now(),
            },
        )
        with Session(self.engine) as session, session.begin():
            session.execute(stmt)
        logger.info("web_cache_documents_saved count=%s", len(rows))

    def get_by_url(self, url: str) -> WebCacheDocument | None:
        with Session(self.engine) as session:
            return session.execute(
                select(WebCacheDocument).where(WebCacheDocument.url == url)
            ).scalar_one_or_none()


class WebCacheChunkRepository(ChunkRepository):
    """Chunk repository bound to web-cache chunk rows."""

    chunk_model = WebCacheChunk


class WebCacheVectorRepository(VectorRepository):
    """Vector repository bound to web-cache vector rows."""

    document_model = WebCacheVectorDocument
    metadata_model = WebCacheVectorIndexMetadata


class WebCacheBM25Repository(BM25Repository):
    """BM25 repository bound to web-cache BM25 rows."""

    segment_model = WebCacheBM25Segment
    term_model = WebCacheBM25Term
    posting_model = WebCacheBM25Posting
    doc_length_model = WebCacheBM25DocLength
