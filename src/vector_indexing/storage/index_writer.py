from __future__ import annotations

import json
import logging
from pathlib import Path

import faiss
import numpy as np

from src.vector_indexing.index.vector_store import VectorStore

logger = logging.getLogger(__name__)


class IndexWriter:
    """Persists the FAISS index and the document mapping atomically."""

    INDEX_FILENAME = "faiss.index"
    DOC_IDS_FILENAME = "doc_ids.npy"
    METADATA_FILENAME = "metadata.json"

    def __init__(self, base_path: str) -> None:
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)

    def write(self, index: faiss.Index, vector_store: VectorStore, metadata: dict[str, int | str]) -> None:
        """Write the FAISS index and its metadata to disk."""
        index_path = self.base_path / self.INDEX_FILENAME
        doc_ids_path = self.base_path / self.DOC_IDS_FILENAME
        metadata_path = self.base_path / self.METADATA_FILENAME

        temporary_index_path = index_path.with_suffix(".index.tmp")
        temporary_doc_ids_path = doc_ids_path.with_suffix(".npy.tmp")
        temporary_metadata_path = metadata_path.with_suffix(".json.tmp")

        faiss.write_index(index, str(temporary_index_path))
        with temporary_doc_ids_path.open("wb") as file_obj:
            np.save(file_obj, vector_store.to_numpy(), allow_pickle=False)
        with temporary_metadata_path.open("w", encoding="utf-8") as file_obj:
            json.dump(metadata, file_obj, indent=2, sort_keys=True)

        temporary_index_path.replace(index_path)
        temporary_doc_ids_path.replace(doc_ids_path)
        temporary_metadata_path.replace(metadata_path)

        logger.info("vector_index_persisted path=%s vectors=%s", self.base_path, len(vector_store))
