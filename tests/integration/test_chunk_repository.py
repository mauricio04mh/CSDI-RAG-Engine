from __future__ import annotations

import pytest
from sqlalchemy.engine import Engine

from src.database.repositories.chunk_repository import ChunkRepository
from src.document_processing.chunker import DocumentChunk


def make_chunk(chunk_id: str, source_id: str = "src", text: str = "hello world") -> DocumentChunk:
    return DocumentChunk(
        chunk_id=chunk_id,
        source_id=source_id,
        url=f"http://example.com/{chunk_id}",
        title="Test Page",
        breadcrumb="Home",
        text=text,
    )


@pytest.fixture()
def repo(engine: Engine) -> ChunkRepository:
    return ChunkRepository(engine)


# ---------------------------------------------------------------------------
# save and retrieve
# ---------------------------------------------------------------------------

def test_save_and_retrieve_single_chunk(repo):
    chunk = make_chunk("src:abc:0")
    repo.save_chunks([chunk])
    result = repo.get_chunk("src:abc:0")
    assert result is not None
    assert result.chunk_id == "src:abc:0"
    assert result.text == "hello world"


def test_save_multiple_chunks(repo):
    chunks = [make_chunk(f"src:abc:{i}") for i in range(3)]
    repo.save_chunks(chunks)
    result = repo.get_chunks(["src:abc:0", "src:abc:1", "src:abc:2"])
    assert len(result) == 3


def test_get_chunks_returns_dict_keyed_by_chunk_id(repo):
    chunks = [make_chunk("src:abc:0"), make_chunk("src:abc:1")]
    repo.save_chunks(chunks)
    result = repo.get_chunks(["src:abc:0", "src:abc:1"])
    assert "src:abc:0" in result
    assert "src:abc:1" in result


def test_get_chunk_returns_none_for_missing(repo):
    assert repo.get_chunk("nonexistent") is None


def test_get_chunks_empty_input(repo):
    assert repo.get_chunks([]) == {}


# ---------------------------------------------------------------------------
# deduplication
# ---------------------------------------------------------------------------

def test_save_chunks_ignores_duplicates(repo):
    chunk = make_chunk("src:abc:0", text="original text")
    repo.save_chunks([chunk])

    # Re-save the same chunk_id — should be silently ignored
    duplicate = make_chunk("src:abc:0", text="different text")
    repo.save_chunks([duplicate])

    stored = repo.get_chunk("src:abc:0")
    assert stored.text == "original text"  # original preserved


def test_get_existing_chunk_ids_returns_correct_subset(repo):
    chunks = [make_chunk(f"src:abc:{i}") for i in range(3)]
    repo.save_chunks(chunks)

    existing = repo.get_existing_chunk_ids(["src:abc:0", "src:abc:2", "src:abc:99"])
    assert existing == {"src:abc:0", "src:abc:2"}


def test_get_existing_chunk_ids_empty_input(repo):
    assert repo.get_existing_chunk_ids([]) == set()


def test_get_existing_chunk_ids_none_exist(repo):
    result = repo.get_existing_chunk_ids(["ghost:0", "ghost:1"])
    assert result == set()


def test_get_existing_chunk_ids_all_exist(repo):
    chunks = [make_chunk(f"src:abc:{i}") for i in range(4)]
    repo.save_chunks(chunks)
    ids = [f"src:abc:{i}" for i in range(4)]
    assert repo.get_existing_chunk_ids(ids) == set(ids)


# ---------------------------------------------------------------------------
# reingest deduplication pattern — mirrors IngestionOrchestrator logic
# ---------------------------------------------------------------------------

def test_reingest_only_saves_new_chunks(repo):
    """Simulates what IngestionOrchestrator does on second ingest of same source."""
    original_chunks = [make_chunk(f"src:page1:{i}") for i in range(3)]
    repo.save_chunks(original_chunks)

    # Second ingest produces the same chunks (content unchanged)
    all_chunk_ids = [c.chunk_id for c in original_chunks]
    existing = repo.get_existing_chunk_ids(all_chunk_ids)
    new_chunks = [c for c in original_chunks if c.chunk_id not in existing]

    assert new_chunks == []  # nothing new to save


def test_reingest_with_new_and_old_chunks(repo):
    """Partially new content: some chunks already exist, some are new."""
    existing_chunks = [make_chunk(f"src:page1:{i}") for i in range(2)]
    repo.save_chunks(existing_chunks)

    # Re-ingest produces old chunks + 2 new ones
    all_chunks = existing_chunks + [make_chunk(f"src:page1:{i}") for i in range(2, 4)]
    all_ids = [c.chunk_id for c in all_chunks]

    existing_ids = repo.get_existing_chunk_ids(all_ids)
    new_chunks = [c for c in all_chunks if c.chunk_id not in existing_ids]

    assert len(new_chunks) == 2
    assert {c.chunk_id for c in new_chunks} == {"src:page1:2", "src:page1:3"}
    repo.save_chunks(new_chunks)

    # Total 4 distinct chunks in DB
    result = repo.get_chunks(all_ids)
    assert len(result) == 4
