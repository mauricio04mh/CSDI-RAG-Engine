from __future__ import annotations
import json
from pathlib import Path
import msgpack
from src.indexing.builder.segment_builder import IndexSegment
from src.indexing.storage.segment_files import (
    DICTIONARY_FILE,
    DOC_LENGTHS_FILE,
    POSTINGS_FILE,
    STATS_FILE,
)


class SegmentReader:
    """Loads previously persisted segments without mutating them."""

    def __init__(self, base_path: str) -> None:
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)

    def list_segments(self) -> list[Path]:
        """Return segment directories ordered by creation name."""
        return sorted(
            path
            for path in self.base_path.iterdir()
            if path.is_dir() and path.name.startswith("segment_") and not path.name.endswith(".tmp")
        )

    def count_segments(self) -> int:
        """Return the current number of active segments on disk."""
        return len(self.list_segments())

    def read(self, segment_path: str | Path) -> IndexSegment:
        """Load a segment from disk."""
        path = Path(segment_path)
        with (path / DICTIONARY_FILE).open("rb") as file_obj:
            dictionary = msgpack.unpack(file_obj, raw=False)
        with (path / POSTINGS_FILE).open("rb") as file_obj:
            postings = msgpack.unpack(file_obj, raw=False)
        with (path / DOC_LENGTHS_FILE).open("rb") as file_obj:
            doc_lengths = msgpack.unpack(file_obj, raw=False)
        with (path / STATS_FILE).open("r", encoding="utf-8") as file_obj:
            stats = json.load(file_obj)

        return IndexSegment(
            segment_id=path.name,
            dictionary=dictionary,
            postings=postings,
            doc_lengths=doc_lengths,
            stats=stats,
        )
