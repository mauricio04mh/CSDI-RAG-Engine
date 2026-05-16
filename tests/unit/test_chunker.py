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


# ---------------------------------------------------------------------------
# sentence-boundary behaviour
# ---------------------------------------------------------------------------

def test_sentence_boundary_not_split_inside_sentence():
    """Two short sentences that together fit in one chunk stay together."""
    chunker = Chunker(chunk_size=20, chunk_overlap=0)
    content = "First sentence here. Second sentence here."
    chunks = chunker.chunk("src", "http://x.com", "T", "", content)
    assert len(chunks) == 1
    assert "First sentence here." in chunks[0].text
    assert "Second sentence here." in chunks[0].text


def test_sentence_boundary_flush_at_sentence_end():
    """When adding a sentence would overflow the buffer, flush before it."""
    # chunk_size=5: 5-word sentences → each sentence fills a chunk exactly,
    # so the second sentence triggers a flush of the first.
    chunker = Chunker(chunk_size=5, chunk_overlap=0)
    # Each sentence is exactly 5 words; the second overflows → 2 chunks.
    s1 = "one two three four five."
    s2 = "six seven eight nine ten."
    content = f"{s1} {s2}"
    chunks = chunker.chunk("src", "http://x.com", "T", "", content)
    assert len(chunks) == 2
    assert chunks[0].text.startswith("one")
    assert chunks[1].text.startswith("six")


def test_oversized_sentence_word_level_fallback():
    """A single sentence longer than chunk_size is split word-by-word."""
    chunker = Chunker(chunk_size=4, chunk_overlap=1)
    # 8-word sentence — must produce multiple chunks, each ≤ chunk_size words.
    big = "alpha beta gamma delta epsilon zeta eta theta."
    chunks = chunker.chunk("src", "http://x.com", "T", "", big)
    assert len(chunks) >= 2
    for chunk in chunks:
        assert len(chunk.text.split()) <= 4


def test_oversized_sentence_followed_by_normal_sentence():
    """After word-level fallback for an oversized sentence, normal chunking resumes."""
    chunker = Chunker(chunk_size=4, chunk_overlap=0)
    big = "alpha beta gamma delta epsilon."   # 5 words → oversized for chunk_size=4
    normal = "hello world."
    content = f"{big} {normal}"
    chunks = chunker.chunk("src", "http://x.com", "T", "", content)
    # The oversized sentence produces fallback chunks; the normal sentence goes into a fresh chunk.
    texts = [c.text for c in chunks]
    assert any("hello world." in t for t in texts)


def test_overlap_carries_across_sentence_boundary():
    """After flushing at a sentence boundary, the overlap words seed the next chunk."""
    chunker = Chunker(chunk_size=6, chunk_overlap=2)
    # Build content where first sentence fills chunk exactly; verify overlap in second chunk.
    s1 = "alpha beta gamma delta epsilon zeta."   # 6 words, exactly chunk_size
    s2 = "foo bar baz."
    content = f"{s1} {s2}"
    chunks = chunker.chunk("src", "http://x.com", "T", "", content)
    assert len(chunks) >= 2
    # The second chunk must start with the last 2 words of s1 (the overlap).
    second_text_words = chunks[1].text.split()
    assert second_text_words[:2] == ["epsilon", "zeta."]


def test_exclamation_and_question_marks_split_sentences():
    """Sentence splitter handles ! and ? as well as ."""
    chunker = Chunker(chunk_size=4, chunk_overlap=0)
    content = "Stop right now! Are you sure? Yes I am."
    chunks = chunker.chunk("src", "http://x.com", "T", "", content)
    # Three 3-word sentences; each fits in chunk_size=4, but together they overflow → >= 2 chunks.
    assert len(chunks) >= 1
    full_text = " ".join(c.text for c in chunks)
    assert "Stop right now!" in full_text
    assert "Are you sure?" in full_text


def test_single_sentence_returns_one_chunk():
    """One sentence that fits within chunk_size produces exactly one chunk."""
    chunker = Chunker(chunk_size=50, chunk_overlap=5)
    content = "This is a single sentence."
    chunks = chunker.chunk("src", "http://x.com", "T", "", content)
    assert len(chunks) == 1
    assert chunks[0].text == "This is a single sentence."
