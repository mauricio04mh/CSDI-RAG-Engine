from __future__ import annotations
from dataclasses import asdict, dataclass
from typing import Iterator


@dataclass(slots=True)
class Posting:
    """Represents a term occurrence summary for a single document."""

    doc_id: str
    tf: int

    def to_dict(self) -> dict[str, int | str]:
        """Serialize a posting to a plain dictionary."""
        return asdict(self)


class PostingsList:
    """Stores postings for a term in append-only order."""

    def __init__(self) -> None:
        self._postings: list[Posting] = []

    def add(self, doc_id: str, term_frequency: int) -> None:
        """Append a posting for a newly indexed document."""
        self._postings.append(Posting(doc_id=doc_id, tf=term_frequency))

    def to_list(self) -> list[dict[str, int | str]]:
        """Return postings as serializable dictionaries."""
        return [posting.to_dict() for posting in self._postings]

    def __iter__(self) -> Iterator[Posting]:
        return iter(self._postings)

    def __len__(self) -> int:
        return len(self._postings)
