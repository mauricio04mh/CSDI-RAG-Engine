from __future__ import annotations

import logging
from datetime import datetime, timezone

from sqlalchemy import delete as sa_delete
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

from src.database.models.source_document_models import SourceDocument
from src.ingestion.source_documents import SourceDocumentInput


class SourceDocumentRepository:
    """Persistence operations for crawled/scraped source documents."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def save_document(self, document: SourceDocumentInput) -> str:
        now = datetime.now(timezone.utc)
        fetched_at = document.fetched_at or now
        last_seen_at = document.last_seen_at or fetched_at
        values = {
            "document_id": document.document_id,
            "source_id": document.source_id,
            "url": document.url,
            "normalized_url": document.normalized_url,
            "title": document.title,
            "breadcrumb": document.breadcrumb,
            "text_content": document.text_content,
            "raw_html": document.raw_html,
            "code_blocks": document.code_blocks,
            "content_type": document.content_type,
            "http_status": document.http_status,
            "fetch_method": document.fetch_method,
            "crawl_depth": document.crawl_depth,
            "discovered_from_url": document.discovered_from_url,
            "published_at": document.published_at,
            "document_updated_at": document.document_updated_at,
            "content_hash": document.content_hash,
            "is_active": True,
            "fetched_at": fetched_at,
            "last_seen_at": last_seen_at,
            "updated_at": now,
        }
        stmt = (
            pg_insert(SourceDocument)
            .values(values)
            .on_conflict_do_update(
                index_elements=["normalized_url"],
                set_={
                    **values,
                    "created_at": SourceDocument.created_at,
                },
            )
            .returning(SourceDocument.document_id)
        )
        with Session(self.engine) as session, session.begin():
            return session.execute(stmt).scalar_one()

    def delete_by_source_id(self, source_id: str) -> int:
        """Hard-delete all source documents for a source. Returns count deleted."""
        with Session(self.engine) as session, session.begin():
            result = session.execute(
                sa_delete(SourceDocument).where(SourceDocument.source_id == source_id)
            )
        logger.info("source_documents_deleted source_id=%s count=%s", source_id, result.rowcount)
        return result.rowcount

    def get_by_document_id(self, document_id: str) -> SourceDocument | None:
        with Session(self.engine) as session:
            return session.execute(
                select(SourceDocument).where(SourceDocument.document_id == document_id)
            ).scalar_one_or_none()
