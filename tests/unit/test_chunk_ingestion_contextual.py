from __future__ import annotations

from unittest.mock import MagicMock, call

import pytest

from src.document_processing.chunker import DocumentChunk
from src.ingestion.chunk_ingestion_service import ChunkIngestionService


def _make_chunk(
    chunk_id: str = "s:abc:0",
    title: str = "Title",
    breadcrumb: str = "Home > Docs",
    text: str = "chunk body text",
) -> DocumentChunk:
    return DocumentChunk(
        chunk_id=chunk_id,
        source_id="s",
        url="http://example.com",
        title=title,
        breadcrumb=breadcrumb,
        text=text,
    )


def _make_service() -> tuple[ChunkIngestionService, MagicMock, MagicMock]:
    chunk_repo = MagicMock()
    chunk_repo.get_existing_chunk_ids.return_value = set()
    index_builder = MagicMock()
    vector_builder = MagicMock()
    svc = ChunkIngestionService(
        chunk_repo=chunk_repo,
        index_builder=index_builder,
        vector_index_builder=vector_builder,
    )
    return svc, index_builder, vector_builder


def test_contextual_embed_combines_title_breadcrumb_text():
    svc, _, vector_builder = _make_service()
    chunk = _make_chunk(title="Title", breadcrumb="Home", text="Body")
    svc.ingest_chunks([chunk])
    vector_builder.add_document.assert_called_once_with(
        doc_id="s:abc:0", text="Title | Home | Body"
    )


def test_contextual_embed_skips_empty_breadcrumb():
    svc, _, vector_builder = _make_service()
    chunk = _make_chunk(title="Title", breadcrumb="", text="Body")
    svc.ingest_chunks([chunk])
    vector_builder.add_document.assert_called_once_with(
        doc_id="s:abc:0", text="Title | Body"
    )


def test_contextual_embed_skips_empty_title():
    svc, _, vector_builder = _make_service()
    chunk = _make_chunk(title="", breadcrumb="Crumb", text="Body")
    svc.ingest_chunks([chunk])
    vector_builder.add_document.assert_called_once_with(
        doc_id="s:abc:0", text="Crumb | Body"
    )


def test_contextual_embed_text_only_when_no_title_breadcrumb():
    svc, _, vector_builder = _make_service()
    chunk = _make_chunk(title="", breadcrumb="", text="Body only")
    svc.ingest_chunks([chunk])
    vector_builder.add_document.assert_called_once_with(
        doc_id="s:abc:0", text="Body only"
    )


def test_chunks_table_text_unchanged():
    """The text stored in the DB must remain chunk.text, not the enriched embed_text."""
    svc, _, _ = _make_service()
    chunk = _make_chunk(title="Title", breadcrumb="Crumb", text="Original body")
    assert chunk.text == "Original body"  # dataclass is not mutated
    svc.ingest_chunks([chunk])
    assert chunk.text == "Original body"
