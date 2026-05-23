from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime

_WHITESPACE = re.compile(r"\s+")
# Split after sentence-ending punctuation followed by whitespace.
# Handles "Hello world. Next sentence" and "End! New" and "Done? Yes".
_SENTENCE_SPLIT = re.compile(r'(?<=[.!?])\s+')


@dataclass(slots=True)
class DocumentChunk:
    """One indexable unit derived from a scraped page."""

    chunk_id: str       # "{source_id}:{url_hash}:{chunk_index}"
    source_id: str
    url: str
    title: str
    breadcrumb: str
    text: str           # cleaned, ready to embed or tokenize
    published_at: datetime | None = None
    document_updated_at: datetime | None = None


def _make_chunk(
    source_id: str,
    url_hash: str,
    url: str,
    title: str,
    breadcrumb: str,
    words: list[str],
    index: int,
    published_at: datetime | None = None,
    document_updated_at: datetime | None = None,
) -> DocumentChunk:
    return DocumentChunk(
        chunk_id=f"{source_id}:{url_hash}:{index}",
        source_id=source_id,
        url=url,
        title=title,
        breadcrumb=breadcrumb,
        text=" ".join(words),
        published_at=published_at,
        document_updated_at=document_updated_at,
    )


class Chunker:
    """Splits a scraped document into overlapping chunks that respect sentence boundaries.

    Sentences are accumulated until the word count reaches chunk_size, then a chunk
    is emitted. The last chunk_overlap words carry over as a seed for the next chunk.
    For sentences longer than chunk_size, falls back to word-level splitting.

    Args:
        chunk_size:    Target number of words per chunk.
        chunk_overlap: Number of words shared between consecutive chunks.
    """

    def __init__(self, chunk_size: int = 256, chunk_overlap: int = 32) -> None:
        if chunk_overlap >= chunk_size:
            raise ValueError("chunk_overlap must be less than chunk_size.")
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap

    def chunk(
        self,
        source_id: str,
        url: str,
        title: str,
        breadcrumb: str,
        content: str,
        published_at: datetime | None = None,
        document_updated_at: datetime | None = None,
    ) -> list[DocumentChunk]:
        cleaned = self._clean(content)
        if not cleaned:
            return []

        url_hash = _url_to_hash(url)
        sentences = [s for s in _SENTENCE_SPLIT.split(cleaned) if s.strip()]
        if not sentences:
            return []

        chunks: list[DocumentChunk] = []
        current_words: list[str] = []
        index = 0

        for sentence in sentences:
            s_words = sentence.split()

            # Edge case: single sentence exceeds chunk_size — word-level fallback.
            if len(s_words) > self.chunk_size:
                if current_words:
                    chunks.append(_make_chunk(
                        source_id,
                        url_hash,
                        url,
                        title,
                        breadcrumb,
                        current_words,
                        index,
                        published_at,
                        document_updated_at,
                    ))
                    index += 1

                step = self.chunk_size - self.chunk_overlap
                for start in range(0, len(s_words), step):
                    window = s_words[start : start + self.chunk_size]
                    if window:
                        chunks.append(_make_chunk(
                            source_id,
                            url_hash,
                            url,
                            title,
                            breadcrumb,
                            window,
                            index,
                            published_at,
                            document_updated_at,
                        ))
                        index += 1
                current_words = []
                continue

            # Flush and carry overlap when adding this sentence would overflow.
            if current_words and len(current_words) + len(s_words) > self.chunk_size:
                chunks.append(_make_chunk(
                    source_id,
                    url_hash,
                    url,
                    title,
                    breadcrumb,
                    current_words,
                    index,
                    published_at,
                    document_updated_at,
                ))
                index += 1
                current_words = current_words[-self.chunk_overlap:] if self.chunk_overlap else []

            current_words.extend(s_words)

        if current_words:
            chunks.append(_make_chunk(
                source_id,
                url_hash,
                url,
                title,
                breadcrumb,
                current_words,
                index,
                published_at,
                document_updated_at,
            ))

        return chunks

    def _clean(self, text: str) -> str:
        return _WHITESPACE.sub(" ", text).strip()


def _url_to_hash(url: str) -> str:
    """Short deterministic identifier derived from a URL."""
    import hashlib
    return hashlib.sha1(url.encode()).hexdigest()[:12]
