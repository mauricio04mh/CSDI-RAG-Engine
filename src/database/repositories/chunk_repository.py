from __future__ import annotations

import logging

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from src.database.models.chunk_models import Chunk
from src.document_processing.chunker import DocumentChunk

logger = logging.getLogger(__name__)


class ChunkRepository:
    """All SQL operations for chunk metadata storage."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def save_chunks(self, chunks: list[DocumentChunk]) -> None:
        """Insert chunks, skipping any that already exist (upsert by chunk_id)."""
        if not chunks:
            return
        rows = [
            {
                "chunk_id": c.chunk_id,
                "source_id": c.source_id,
                "url": c.url,
                "title": c.title,
                "breadcrumb": c.breadcrumb,
                "text": c.text,
            }
            for c in chunks
        ]
        stmt = pg_insert(Chunk).values(rows).on_conflict_do_nothing(index_elements=["chunk_id"])
        with Session(self.engine) as session, session.begin():
            session.execute(stmt)
        logger.info("chunks_saved count=%s", len(rows))

    def get_existing_chunk_ids(self, chunk_ids: list[str]) -> set[str]:
        """Return the subset of chunk_ids that already exist in the database."""
        if not chunk_ids:
            return set()
        with Session(self.engine) as session:
            rows = session.execute(
                select(Chunk.chunk_id).where(Chunk.chunk_id.in_(chunk_ids))
            ).all()
        return {row.chunk_id for row in rows}

    def get_chunk(self, chunk_id: str) -> Chunk | None:
        with Session(self.engine) as session:
            return session.execute(
                select(Chunk).where(Chunk.chunk_id == chunk_id)
            ).scalar_one_or_none()

    def get_chunks(self, chunk_ids: list[str]) -> dict[str, Chunk]:
        """Fetch multiple chunks by ID. Returns a dict keyed by chunk_id."""
        if not chunk_ids:
            return {}
        with Session(self.engine) as session:
            rows = session.execute(
                select(Chunk).where(Chunk.chunk_id.in_(chunk_ids))
            ).scalars().all()
        return {row.chunk_id: row for row in rows}
