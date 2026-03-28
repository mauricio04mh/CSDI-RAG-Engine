from __future__ import annotations

from dataclasses import dataclass

from src.bm25.structures.postings_list import PostingsList


@dataclass(slots=True)
class BM25Index:
    """In-memory representation of the lexical index used by BM25."""

    dictionary: dict[str, int]
    postings: dict[str, PostingsList]
    doc_lengths: dict[str, int]
    total_documents: int
    avg_document_length: float
