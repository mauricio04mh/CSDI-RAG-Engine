from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Iterator


@dataclass(slots=True)
class Posting:
    """Represents one lexical posting entry for a term."""

    doc_id: str
    tf: int


class PostingsList:
    """Stores postings for a term in memory."""

    def __init__(self, postings: Iterable[Posting] | None = None) -> None:
        self._postings = list(postings or [])

    @classmethod
    def from_serialized(cls, payload: list[dict[str, int | str]]) -> "PostingsList":
        """Create a postings list from deserialized dictionaries."""
        return cls(Posting(doc_id=str(item["doc_id"]), tf=int(item["tf"])) for item in payload)

    def __iter__(self) -> Iterator[Posting]:
        return iter(self._postings)

    def __len__(self) -> int:
        return len(self._postings)
