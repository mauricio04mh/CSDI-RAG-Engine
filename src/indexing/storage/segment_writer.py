from __future__ import annotations
from pathlib import Path

import json
import logging
import shutil
import msgpack

from src.indexing.builder.segment_builder import IndexSegment
from src.indexing.storage.segment_files import (
    DICTIONARY_FILE,
    DOC_LENGTHS_FILE,
    POSTINGS_FILE,
    STATS_FILE,
)

logger = logging.getLogger(__name__)


class SegmentWriter:
    """Writes immutable segments to disk using atomic file replacement."""

    def __init__(self, base_path: str) -> None:
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)

    def write(self, segment: IndexSegment) -> Path:
        """Persist a segment to disk and return its directory."""
        segment_path = self.base_path / segment.segment_id
        temporary_segment_path = self.base_path / f"{segment.segment_id}.tmp"
        temporary_segment_path.mkdir(parents=False, exist_ok=False)

        self._write_msgpack(temporary_segment_path / DICTIONARY_FILE, segment.dictionary)
        self._write_msgpack(temporary_segment_path / POSTINGS_FILE, segment.postings)
        self._write_msgpack(temporary_segment_path / DOC_LENGTHS_FILE, segment.doc_lengths)
        self._write_json(temporary_segment_path / STATS_FILE, segment.stats)
        temporary_segment_path.replace(segment_path)

        logger.info("segment_persisted segment_id=%s path=%s", segment.segment_id, segment_path)
        return segment_path

    def _write_msgpack(self, path: Path, payload: object) -> None:
        """Write a msgpack file atomically."""
        temporary_path = path.with_suffix(f"{path.suffix}.tmp")
        with temporary_path.open("wb") as file_obj:
            msgpack.pack(payload, file_obj, use_bin_type=True)
        temporary_path.replace(path)

    def _write_json(self, path: Path, payload: dict[str, int | float]) -> None:
        """Write a JSON file atomically."""
        temporary_path = path.with_suffix(f"{path.suffix}.tmp")
        with temporary_path.open("w", encoding="utf-8") as file_obj:
            json.dump(payload, file_obj, indent=2, sort_keys=True)
        temporary_path.replace(path)

    def delete(self, segment_path: str | Path) -> None:
        """Delete a persisted segment after a successful merge."""
        path = Path(segment_path)
        if not path.exists():
            return
        shutil.rmtree(path)
        logger.info("segment_deleted path=%s", path)
