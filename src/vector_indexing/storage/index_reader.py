from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import faiss
import numpy as np

from src.vector_indexing.index.vector_store import VectorStore
from src.vector_indexing.storage.index_writer import IndexWriter


@dataclass(slots=True)
class PersistedIndex:
    """Describes a vector index loaded from persistent storage."""

    index: faiss.Index
    vector_store: VectorStore
    metadata: dict[str, int | str]


class IndexReader:
    """Reads FAISS indexes and metadata if they already exist on disk."""

    def __init__(self, base_path: str) -> None:
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)

    def load(self) -> PersistedIndex | None:
        """Load a persisted index if all required files are present."""
        index_path = self.base_path / IndexWriter.INDEX_FILENAME
        doc_ids_path = self.base_path / IndexWriter.DOC_IDS_FILENAME
        metadata_path = self.base_path / IndexWriter.METADATA_FILENAME

        if not index_path.exists() or not doc_ids_path.exists() or not metadata_path.exists():
            return None

        index = faiss.read_index(str(index_path))
        with doc_ids_path.open("rb") as file_obj:
            doc_ids = np.load(file_obj, allow_pickle=False)
        with metadata_path.open("r", encoding="utf-8") as file_obj:
            metadata = json.load(file_obj)

        return PersistedIndex(
            index=index,
            vector_store=VectorStore.from_numpy(doc_ids),
            metadata=metadata,
        )
