from __future__ import annotations

import logging
from dataclasses import dataclass

from src.bm25.text.tokenizer import tokenize
from src.document_processing.chunker import DocumentChunk

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class ChunkIngestionResult:
    total_chunks: int
    new_chunks: int
    indexed_chunks: int
    skipped_existing: int


@dataclass(slots=True)
class ChunkIngestionFinalizeResult:
    vector_flushed: int
    bm25_segment_id: str | None


class ChunkIngestionService:
    """Shared chunk ingestion logic used by multiple pipelines.

    Responsibilities:
    - Deduplicate by chunk_id against the chunks table
    - Persist new chunk metadata
    - Index in BM25 (IndexBuilder) and vector index (VectorIndexBuilder)
    - Finalize buffers (vector flush + forced BM25 flush) and optionally reload BM25 retriever
    """

    def __init__(
        self,
        *,
        chunk_repo,
        index_builder,
        vector_index_builder,
        bm25_retriever=None,
    ) -> None:
        self._chunk_repo = chunk_repo
        self._index_builder = index_builder
        self._vector_index_builder = vector_index_builder
        self._bm25_retriever = bm25_retriever

    def ingest_chunks(self, chunks: list[DocumentChunk]) -> ChunkIngestionResult:
        if not chunks:
            return ChunkIngestionResult(
                total_chunks=0,
                new_chunks=0,
                indexed_chunks=0,
                skipped_existing=0,
            )

        existing_ids = self._chunk_repo.get_existing_chunk_ids([c.chunk_id for c in chunks])
        new_chunks = [c for c in chunks if c.chunk_id not in existing_ids]

        if not new_chunks:
            return ChunkIngestionResult(
                total_chunks=len(chunks),
                new_chunks=0,
                indexed_chunks=0,
                skipped_existing=len(chunks),
            )

        self._chunk_repo.save_chunks(new_chunks)

        indexed_chunks = 0
        for chunk in new_chunks:
            if self._index_chunk(chunk):
                indexed_chunks += 1

        skipped_existing = len(chunks) - len(new_chunks)
        return ChunkIngestionResult(
            total_chunks=len(chunks),
            new_chunks=len(new_chunks),
            indexed_chunks=indexed_chunks,
            skipped_existing=skipped_existing,
        )

    def finalize(self, *, reload_bm25: bool = False) -> ChunkIngestionFinalizeResult:
        vector_flushed = self._vector_index_builder.flush()
        bm25_segment_id = self._index_builder.flush(force=True)

        if reload_bm25 and self._bm25_retriever is not None:
            self._bm25_retriever.reload()

        return ChunkIngestionFinalizeResult(
            vector_flushed=vector_flushed,
            bm25_segment_id=bm25_segment_id,
        )

    def _index_chunk(self, chunk: DocumentChunk) -> bool:
        tokens = tokenize(chunk.text)
        try:
            self._index_builder.add_document(doc_id=chunk.chunk_id, tokens=tokens)
            embed_text = " | ".join(filter(None, [chunk.title, chunk.breadcrumb, chunk.text]))
            self._vector_index_builder.add_document(doc_id=chunk.chunk_id, text=embed_text)
            return True
        except ValueError as exc:
            logger.warning("chunk_index_conflict chunk_id=%s reason=%s", chunk.chunk_id, exc)
            return False
        except Exception:
            logger.exception("chunk_index_failed chunk_id=%s", chunk.chunk_id)
            return False
