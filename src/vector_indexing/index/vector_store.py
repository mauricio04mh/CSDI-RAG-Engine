from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np


@dataclass(slots=True)
class VectorStore:
    """Tracks the mapping between FAISS vector positions and external document IDs."""

    vector_ids_to_doc_ids: list[str] = field(default_factory=list)
    doc_ids_to_vector_ids: dict[str, int] = field(default_factory=dict)

    def add_documents(self, doc_ids: list[str]) -> list[int]:
        """Register document IDs in insertion order and return their vector IDs."""
        vector_ids: list[int] = []
        for doc_id in doc_ids:
            if doc_id in self.doc_ids_to_vector_ids:
                raise ValueError(f"Document '{doc_id}' already exists in the vector store.")
            vector_id = len(self.vector_ids_to_doc_ids)
            self.vector_ids_to_doc_ids.append(doc_id)
            self.doc_ids_to_vector_ids[doc_id] = vector_id
            vector_ids.append(vector_id)
        return vector_ids

    def get_doc_id(self, vector_id: int) -> str | None:
        """Resolve a FAISS vector ID back to a document ID."""
        if vector_id < 0 or vector_id >= len(self.vector_ids_to_doc_ids):
            return None
        return self.vector_ids_to_doc_ids[vector_id]

    def to_numpy(self) -> np.ndarray:
        """Serialize document IDs into a numpy array."""
        return np.asarray(self.vector_ids_to_doc_ids, dtype=np.str_)

    @classmethod
    def from_numpy(cls, doc_ids: np.ndarray) -> "VectorStore":
        """Rebuild a vector store from persisted document IDs."""
        values = [str(doc_id) for doc_id in doc_ids.tolist()]
        return cls(
            vector_ids_to_doc_ids=values,
            doc_ids_to_vector_ids={doc_id: index for index, doc_id in enumerate(values)},
        )

    def __len__(self) -> int:
        return len(self.vector_ids_to_doc_ids)
