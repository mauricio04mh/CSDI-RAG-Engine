from __future__ import annotations

import pytest

from src.document_processing.chunker import Chunker, DocumentChunk, _url_to_hash


def make_chunker(size: int = 10, overlap: int = 2) -> Chunker:
    return Chunker(chunk_size=size, chunk_overlap=overlap)


# ---------------------------------------------------------------------------
# chunk_id determinism
# ---------------------------------------------------------------------------

def test_chunk_id_is_deterministic():
    chunker = make_chunker()
    chunks_a = chunker.chunk("src", "http://example.com/a", "T", "", "word " * 20)
    chunks_b = chunker.chunk("src", "http://example.com/a", "T", "", "word " * 20)
    assert [c.chunk_id for c in chunks_a] == [c.chunk_id for c in chunks_b]


def test_chunk_id_differs_for_different_urls():
    chunker = make_chunker()
    chunks_a = chunker.chunk("src", "http://example.com/a", "T", "", "word " * 20)
    chunks_b = chunker.chunk("src", "http://example.com/b", "T", "", "word " * 20)
    assert chunks_a[0].chunk_id != chunks_b[0].chunk_id


def test_chunk_id_format():
    chunker = make_chunker()
    chunks = chunker.chunk("my_source", "http://example.com/page", "T", "", "word " * 20)
    for i, chunk in enumerate(chunks):
        assert chunk.chunk_id == f"my_source:{_url_to_hash('http://example.com/page')}:{i}"


# ---------------------------------------------------------------------------
# chunking logic
# ---------------------------------------------------------------------------

def test_empty_content_returns_no_chunks():
    chunker = make_chunker()
    assert chunker.chunk("src", "http://x.com", "T", "", "") == []


def test_whitespace_only_returns_no_chunks():
    chunker = make_chunker()
    assert chunker.chunk("src", "http://x.com", "T", "", "   \n\t  ") == []


def test_single_chunk_when_content_fits():
    chunker = Chunker(chunk_size=100, chunk_overlap=10)
    chunks = chunker.chunk("src", "http://x.com", "T", "", "hello world")
    assert len(chunks) == 1
    assert chunks[0].text == "hello world"


def test_multiple_chunks_produced():
    chunker = Chunker(chunk_size=5, chunk_overlap=1)
    # 12 words → step=4 → starts at 0,4,8 → 3 chunks
    content = " ".join(f"w{i}" for i in range(12))
    chunks = chunker.chunk("src", "http://x.com", "T", "", content)
    assert len(chunks) == 3


def test_overlap_is_respected():
    chunker = Chunker(chunk_size=5, chunk_overlap=2)
    words = [f"w{i}" for i in range(10)]
    content = " ".join(words)
    chunks = chunker.chunk("src", "http://x.com", "T", "", content)
    # First chunk: words[0:5], second chunk: words[3:8] (step=3)
    assert chunks[0].text == " ".join(words[0:5])
    assert chunks[1].text == " ".join(words[3:8])


def test_chunk_carries_metadata():
    chunker = make_chunker(size=100, overlap=2)
    chunks = chunker.chunk("src1", "http://x.com/page", "My Title", "Home > Page", "some content here")
    assert chunks[0].source_id == "src1"
    assert chunks[0].url == "http://x.com/page"
    assert chunks[0].title == "My Title"
    assert chunks[0].breadcrumb == "Home > Page"


def test_overlap_must_be_less_than_size():
    with pytest.raises(ValueError, match="chunk_overlap must be less than chunk_size"):
        Chunker(chunk_size=10, chunk_overlap=10)


def test_whitespace_is_normalized():
    chunker = Chunker(chunk_size=100, chunk_overlap=0)
    chunks = chunker.chunk("src", "http://x.com", "T", "", "hello   \n  world\t!")
    assert chunks[0].text == "hello world !"
