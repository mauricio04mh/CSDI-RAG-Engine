from __future__ import annotations

"""
End-to-end deduplication test for the IngestionOrchestrator pipeline.

Uses real ChunkRepository + IndexBuilder with a test PostgreSQL.
The embedding model (SentenceTransformer) is replaced with a fast stub
that returns random normalized 384-dim vectors — no model download needed.
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pytest
from sqlalchemy.engine import Engine

from src.bm25.config.settings import BM25Settings
from src.bm25.pipeline.bm25_retriever import BM25Retriever
from src.database.repositories.bm25_repository import BM25Repository
from src.database.repositories.chunk_repository import ChunkRepository
from src.database.repositories.vector_repository import VectorRepository
from src.document_processing.chunker import Chunker, DocumentChunk
from src.indexing.builder.index_builder import IndexBuilder
from src.indexing.config.settings import Settings
from src.vector_indexing.config.settings import VectorSettings
from src.vector_indexing.pipeline.vector_index_builder import VectorIndexBuilder


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fake_vector(dim: int = 384) -> np.ndarray:
    v = np.random.randn(dim).astype(np.float32)
    return v / np.linalg.norm(v)


def make_settings(tmp_path: Path) -> tuple[Settings, BM25Settings, VectorSettings]:
    env = tmp_path / ".env"
    env.write_text("")
    index_s = Settings(
        index_buffer_size=100,
        index_max_segments_in_memory=3,
        index_flush_interval=60,
        log_level="DEBUG",
        env_path=env,
        project_root=tmp_path,
    )
    bm25_s = BM25Settings(bm25_k1=1.5, bm25_b=0.75, env_path=env, project_root=tmp_path)
    vec_s = VectorSettings(
        embedding_model="stub",
        vector_dimension=384,
        faiss_index_type="HNSW",
        hnsw_m=16,
        hnsw_ef_construction=100,
        hnsw_ef_search=50,
        vector_batch_size=100,
        log_level="DEBUG",
        env_path=env,
        project_root=tmp_path,
    )
    return index_s, bm25_s, vec_s


def make_chunks(source_id: str, page: str, count: int) -> list[DocumentChunk]:
    return [
        DocumentChunk(
            chunk_id=f"{source_id}:{page}:{i}",
            source_id=source_id,
            url=f"http://example.com/{page}",
            title="Test",
            breadcrumb="",
            text=f"This is chunk number {i} about python programming and data structures",
        )
        for i in range(count)
    ]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def components(engine: Engine, tmp_path: Path):
    index_s, bm25_s, vec_s = make_settings(tmp_path)

    chunk_repo = ChunkRepository(engine)
    index_builder = IndexBuilder(settings=index_s, engine=engine)

    # Patch EmbeddingModel so no SentenceTransformer download happens
    with patch("src.vector_indexing.pipeline.vector_index_builder.EmbeddingModel") as MockEmb:
        mock_model = MagicMock()
        mock_model.encode_one.side_effect = lambda _text: _fake_vector()
        MockEmb.return_value = mock_model

        vec_builder = VectorIndexBuilder(settings=vec_s, engine=engine)

    index_builder.start()
    vec_builder.start()

    yield chunk_repo, index_builder, vec_builder

    vec_builder.stop()
    index_builder.stop()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_first_ingest_indexes_all_chunks(components, engine):
    chunk_repo, index_builder, vec_builder = components
    chunks = make_chunks("src", "page1", 3)

    existing = chunk_repo.get_existing_chunk_ids([c.chunk_id for c in chunks])
    new_chunks = [c for c in chunks if c.chunk_id not in existing]

    chunk_repo.save_chunks(new_chunks)
    indexed = 0
    for chunk in new_chunks:
        tokens = chunk.text.lower().split()
        index_builder.add_document(chunk.chunk_id, tokens)
        vec_builder.add_document(chunk.chunk_id, chunk.text)
        indexed += 1

    assert indexed == 3
    assert len(chunk_repo.get_chunks([c.chunk_id for c in chunks])) == 3


def test_reingest_skips_all_existing_chunks(components, engine):
    chunk_repo, index_builder, vec_builder = components
    chunks = make_chunks("src", "page1", 3)

    # First ingest
    chunk_repo.save_chunks(chunks)
    for chunk in chunks:
        index_builder.add_document(chunk.chunk_id, chunk.text.lower().split())
        vec_builder.add_document(chunk.chunk_id, chunk.text)

    index_builder.flush(force=True)
    vec_builder.stop()  # flush vector buffer

    # Re-ingest: the filter must skip all
    existing = chunk_repo.get_existing_chunk_ids([c.chunk_id for c in chunks])
    new_chunks = [c for c in chunks if c.chunk_id not in existing]

    assert new_chunks == []


def test_reingest_does_not_duplicate_bm25_postings(components, engine):
    chunk_repo, index_builder, vec_builder = components
    chunks = make_chunks("src", "page1", 2)

    # First ingest
    chunk_repo.save_chunks(chunks)
    for chunk in chunks:
        index_builder.add_document(chunk.chunk_id, chunk.text.lower().split())
    index_builder.flush(force=True)

    # Second ingest — filtered correctly, nothing re-indexed
    existing = chunk_repo.get_existing_chunk_ids([c.chunk_id for c in chunks])
    new_chunks = [c for c in chunks if c.chunk_id not in existing]
    assert new_chunks == []

    # Verify BM25 has no duplicate doc_ids
    bm25_repo = BM25Repository(engine)
    raw_postings, _, doc_lengths, _, _ = bm25_repo.load_full_index()

    for term, postings in raw_postings.items():
        doc_ids = [p["doc_id"] for p in postings]
        assert len(doc_ids) == len(set(doc_ids)), (
            f"BM25 term '{term}' has duplicate doc_ids after re-ingest: {doc_ids}"
        )

    chunk_ids = [c.chunk_id for c in chunks]
    for chunk_id in chunk_ids:
        count = sum(1 for k in doc_lengths if k == chunk_id)
        assert count == 1, f"doc_id '{chunk_id}' appears {count} times in doc_lengths"


def test_partial_reingest_only_indexes_new_chunks(components, engine):
    chunk_repo, index_builder, vec_builder = components

    old_chunks = make_chunks("src", "page1", 2)
    new_chunks_expected = make_chunks("src", "page2", 3)

    # First ingest: only old chunks
    chunk_repo.save_chunks(old_chunks)
    for chunk in old_chunks:
        index_builder.add_document(chunk.chunk_id, chunk.text.lower().split())

    # Second ingest: old chunks (page1) + new chunks (page2)
    all_chunks = old_chunks + new_chunks_expected
    existing = chunk_repo.get_existing_chunk_ids([c.chunk_id for c in all_chunks])
    new_chunks_actual = [c for c in all_chunks if c.chunk_id not in existing]

    assert len(new_chunks_actual) == 3
    assert all(c.chunk_id.startswith("src:page2") for c in new_chunks_actual)


def test_search_returns_results_after_ingest(components, engine, tmp_path):
    chunk_repo, index_builder, vec_builder = components
    chunks = make_chunks("src", "page1", 5)

    chunk_repo.save_chunks(chunks)
    for chunk in chunks:
        index_builder.add_document(chunk.chunk_id, chunk.text.lower().split())
    index_builder.flush(force=True)

    env = tmp_path / ".env"
    env.write_text("")
    bm25_s = BM25Settings(bm25_k1=1.5, bm25_b=0.75, env_path=env, project_root=tmp_path)
    retriever = BM25Retriever(settings=bm25_s, engine=engine)
    retriever.start()

    results = retriever.search("python programming", top_k=10)
    assert len(results) > 0
    assert all(r.score > 0 for r in results)
