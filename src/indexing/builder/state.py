from __future__ import annotations
from dataclasses import dataclass, field
from src.indexing.structures.corpus_stats import CorpusStats
from src.indexing.structures.inverted_index import InvertedIndex


@dataclass(slots=True)
class ActiveIndexState:
    """Encapsulates the temporary in-memory structures for the active segment."""

    inverted_index: InvertedIndex = field(default_factory=InvertedIndex)
    doc_lengths: dict[str, int] = field(default_factory=dict)
    corpus_stats: CorpusStats = field(default_factory=CorpusStats)
    buffered_documents: int = 0

    def record_document(self, doc_id: str, term_frequencies: dict[str, int], document_length: int) -> None:
        """Add one processed document to the active in-memory state."""
        if doc_id in self.doc_lengths:
            raise ValueError(f"Document '{doc_id}' already exists in the active segment.")

        for term, term_frequency in term_frequencies.items():
            self.inverted_index.add_term(term=term, doc_id=doc_id, tf=term_frequency)

        self.doc_lengths[doc_id] = document_length
        self.corpus_stats.update(document_length=document_length)
        self.buffered_documents += 1

    def has_documents(self) -> bool:
        """Return whether the active in-memory state contains pending documents."""
        return self.buffered_documents > 0

