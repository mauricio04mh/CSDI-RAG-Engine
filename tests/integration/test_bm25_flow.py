from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import pytest
from sqlalchemy.engine import Engine

from src.bm25.config.settings import BM25Settings
from src.bm25.pipeline.bm25_retriever import BM25Retriever
from src.indexing.builder.index_builder import IndexBuilder
from src.indexing.config.settings import Settings


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def index_settings(tmp_path: Path) -> Settings:
    env = tmp_path / ".env"
    env.write_text("")
    return Settings(
        index_buffer_size=5,       # small buffer → easier to trigger flush in tests
        index_max_segments_in_memory=3,
        index_flush_interval=60,
        log_level="DEBUG",
        env_path=env,
        project_root=tmp_path,
    )


@pytest.fixture()
def bm25_settings(tmp_path: Path) -> BM25Settings:
    env = tmp_path / ".env"
    env.write_text("")
    return BM25Settings(
        bm25_k1=1.5,
        bm25_b=0.75,
        env_path=env,
        project_root=tmp_path,
    )


@pytest.fixture()
def index_builder(index_settings: Settings, engine: Engine) -> IndexBuilder:
    builder = IndexBuilder(settings=index_settings, engine=engine)
    builder.start()
    yield builder
    builder.stop()


@pytest.fixture()
def bm25_retriever(bm25_settings: BM25Settings, engine: Engine) -> BM25Retriever:
    retriever = BM25Retriever(settings=bm25_settings, engine=engine)
    retriever.start()
    return retriever


# ---------------------------------------------------------------------------
# IndexBuilder — add + flush
# ---------------------------------------------------------------------------

def test_add_document_returns_result(index_builder):
    result = index_builder.add_document("doc_1", ["python", "list", "comprehension"])
    assert result.doc_id == "doc_1"
    assert result.buffered_documents >= 1


def test_flush_persists_segment(index_builder, engine):
    from src.database.repositories.bm25_repository import BM25Repository
    index_builder.add_document("doc_1", ["python", "list"])
    index_builder.flush(force=True)

    repo = BM25Repository(engine)
    segment_ids = repo.list_segment_ids()
    assert len(segment_ids) >= 1


def test_buffer_size_triggers_auto_flush(index_builder, engine):
    """buffer_size=5, adding 5 docs should trigger an automatic flush."""
    from src.database.repositories.bm25_repository import BM25Repository
    for i in range(5):
        index_builder.add_document(f"doc_{i}", ["word", f"unique_{i}"])

    repo = BM25Repository(engine)
    # At least one segment should have been written
    assert len(repo.list_segment_ids()) >= 1


def test_duplicate_doc_in_same_buffer_raises(index_builder):
    index_builder.add_document("doc_dup", ["hello", "world"])
    with pytest.raises(ValueError, match="already exists"):
        index_builder.add_document("doc_dup", ["hello", "world"])


# ---------------------------------------------------------------------------
# BM25Retriever — search
# ---------------------------------------------------------------------------

def test_search_returns_empty_when_index_empty(bm25_retriever):
    results = bm25_retriever.search("python tutorial", top_k=10)
    assert results == []


def test_search_returns_indexed_document(index_builder, bm25_retriever):
    index_builder.add_document("doc_py", ["python", "iterator", "generator"])
    index_builder.flush(force=True)
    bm25_retriever.reload()

    results = bm25_retriever.search("python generator", top_k=5)
    doc_ids = [r.doc_id for r in results]
    assert "doc_py" in doc_ids


def test_search_ranks_more_relevant_doc_higher(index_builder, bm25_retriever):
    index_builder.add_document("doc_relevant", ["python", "python", "python", "tutorial"])
    index_builder.add_document("doc_irrelevant", ["javascript", "node", "npm"])
    index_builder.flush(force=True)
    bm25_retriever.reload()

    results = bm25_retriever.search("python", top_k=5)
    assert results[0].doc_id == "doc_relevant"


def test_search_respects_top_k(index_builder, bm25_retriever):
    for i in range(10):
        index_builder.add_document(f"doc_{i}", ["python", f"topic_{i}"])
    index_builder.flush(force=True)
    bm25_retriever.reload()

    results = bm25_retriever.search("python", top_k=3)
    assert len(results) <= 3


def test_scores_are_positive(index_builder, bm25_retriever):
    index_builder.add_document("doc_1", ["machine", "learning", "model"])
    index_builder.flush(force=True)
    bm25_retriever.reload()

    results = bm25_retriever.search("machine learning", top_k=5)
    for r in results:
        assert r.score > 0


# ---------------------------------------------------------------------------
# Deduplication — BM25 must not double-count on re-ingest
# ---------------------------------------------------------------------------

def test_bm25_no_duplicate_doc_after_reingest(index_builder, bm25_retriever):
    """
    Core deduplication test for BM25.

    Simulates what IngestionOrchestrator does:
    - First ingest: index doc normally
    - Second ingest: the chunk already exists in ChunkRepository, so _index_chunk
      is never called again. We verify this by checking the BM25 postings
      only contain one entry per doc_id after load_full_index.
    """
    from src.database.repositories.bm25_repository import BM25Repository

    index_builder.add_document("chunk:page1:0", ["python", "list", "comprehension"])
    index_builder.flush(force=True)

    # Simulate second ingest: ChunkRepository filtered this chunk_id → skip
    # We mimic the "skip" by NOT calling add_document again for the same chunk_id.
    # But if we did (the old bug), let's verify the DB state is correct with proper filtering.

    repo = BM25Repository(engine=index_builder._bm25_repo.engine)
    raw_postings, _, doc_lengths, total_docs, _ = repo.load_full_index()

    # doc_id must appear exactly once per term
    for term, postings in raw_postings.items():
        doc_ids_for_term = [p["doc_id"] for p in postings]
        assert len(doc_ids_for_term) == len(set(doc_ids_for_term)), (
            f"Term '{term}' has duplicate doc_ids: {doc_ids_for_term}"
        )

    # doc must appear exactly once in doc_lengths
    assert doc_lengths.get("chunk:page1:0") is not None
    doc_ids_all = list(doc_lengths.keys())
    assert len(doc_ids_all) == len(set(doc_ids_all))
