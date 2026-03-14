from __future__ import annotations
from dataclasses import asdict, dataclass

@dataclass(slots=True)
class CorpusStats:
    """Tracks aggregate statistics needed by BM25-style retrieval."""

    total_documents: int = 0
    total_terms: int = 0
    average_document_length: float = 0.0

    def update(self, document_length: int) -> None:
        """Update corpus statistics after indexing a document."""
        self.total_documents += 1
        self.total_terms += document_length
        if self.total_documents == 0:
            self.average_document_length = 0.0
        else:
            self.average_document_length = self.total_terms / self.total_documents

    def to_dict(self) -> dict[str, int | float]:
        """Serialize stats to a dictionary."""
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: dict[str, int | float]) -> "CorpusStats":
        """Create stats from a persisted representation."""
        return cls(
            total_documents=int(payload.get("total_documents", 0)),
            total_terms=int(payload.get("total_terms", 0)),
            average_document_length=float(payload.get("average_document_length", 0.0)),
        )
