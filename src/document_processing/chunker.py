from __future__ import annotations

import re
from dataclasses import dataclass

_WHITESPACE = re.compile(r"\s+")


@dataclass(slots=True)
class DocumentChunk:
    """One indexable unit derived from a scraped page."""

    chunk_id: str       # "{source_id}:{url_hash}:{chunk_index}"
    source_id: str
    url: str
    title: str
    breadcrumb: str
    text: str           # cleaned, ready to embed or tokenize


class Chunker:
    """Splits a scraped document into fixed-size overlapping text chunks.

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
    ) -> list[DocumentChunk]:
        cleaned = self._clean(content)
        words = cleaned.split()

        if not words:
            return []

        url_hash = _url_to_hash(url)
        chunks: list[DocumentChunk] = []
        step = self.chunk_size - self.chunk_overlap
        index = 0

        for start in range(0, len(words), step):
            window = words[start : start + self.chunk_size]
            if not window:
                break
            chunks.append(
                DocumentChunk(
                    chunk_id=f"{source_id}:{url_hash}:{index}",
                    source_id=source_id,
                    url=url,
                    title=title,
                    breadcrumb=breadcrumb,
                    text=" ".join(window),
                )
            )
            index += 1

        return chunks

    def _clean(self, text: str) -> str:
        return _WHITESPACE.sub(" ", text).strip()


def _url_to_hash(url: str) -> str:
    """Short deterministic identifier derived from a URL."""
    import hashlib
    return hashlib.sha1(url.encode()).hexdigest()[:12]
