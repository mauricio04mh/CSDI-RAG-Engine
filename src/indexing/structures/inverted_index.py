from __future__ import annotations
from collections import defaultdict
from typing import Iterable
from src.indexing.structures.postings_list import PostingsList


class InvertedIndex:
    """Maintains term to postings-list mappings for the active segment."""

    def __init__(self) -> None:
        self._index: dict[str, PostingsList] = defaultdict(PostingsList)

    def add_term(self, term: str, doc_id: str, tf: int) -> None:
        """Add a term occurrence for a document."""
        self._index[term].add(doc_id=doc_id, term_frequency=tf)

    def add_terms(self, terms: Iterable[tuple[str, int]], doc_id: str) -> None:
        """Add multiple terms for a single document."""
        for term, term_frequency in terms:
            self.add_term(term=term, doc_id=doc_id, tf=term_frequency)

    def get_postings(self, term: str) -> list[dict[str, int | str]]:
        """Return postings for a term as serializable dictionaries."""
        postings = self._index.get(term)
        if postings is None:
            return []
        return postings.to_list()

    def to_serializable(self) -> tuple[dict[str, int], dict[str, list[dict[str, int | str]]]]:
        """Split the dictionary and postings payload for compact segment storage."""
        dictionary: dict[str, int] = {}
        postings_payload: dict[str, list[dict[str, int | str]]] = {}

        for term in sorted(self._index.keys()):
            postings_list = self._index[term]
            dictionary[term] = len(postings_list)
            postings_payload[term] = postings_list.to_list()

        return dictionary, postings_payload

    def clear(self) -> None:
        """Reset the in-memory index."""
        self._index.clear()

    def __len__(self) -> int:
        return len(self._index)
