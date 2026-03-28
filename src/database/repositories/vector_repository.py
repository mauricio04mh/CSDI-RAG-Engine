from __future__ import annotations

import logging

import numpy as np
<<<<<<< HEAD
from sqlalchemy import select
=======
from sqlalchemy import delete, select
>>>>>>> 0869b5537c8feab5210ece8b099d72c680234530
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from src.database.models.vector_models import VectorDocument, VectorIndexMetadata

logger = logging.getLogger(__name__)


class VectorRepository:
    """All SQL operations for the dense vector index domain."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def save_documents(self, doc_ids: list[str], vectors: np.ndarray) -> None:
<<<<<<< HEAD
        rows = [{"doc_id": doc_id, "embedding": v.tolist()} for doc_id, v in zip(doc_ids, vectors)]
        with Session(self.engine) as session, session.begin():
            session.execute(VectorDocument.__table__.insert(), rows)
        logger.info("vector_documents_saved count=%s", len(rows))

    def load_all_documents(self) -> tuple[list[str], np.ndarray]:
        with Session(self.engine) as session:
            rows = session.execute(
                select(VectorDocument.doc_id, VectorDocument.embedding).order_by(VectorDocument.id.asc())
            ).all()
        if not rows:
            return [], np.empty((0,), dtype=np.float32)
        doc_ids = [r.doc_id for r in rows]
        vectors = np.array([r.embedding for r in rows], dtype=np.float32)
=======
        """Bulk insert new vector documents."""
        rows = [
            {"doc_id": doc_id, "embedding": vector.tolist()}
            for doc_id, vector in zip(doc_ids, vectors)
        ]
        with Session(self.engine) as session:
            with session.begin():
                session.execute(VectorDocument.__table__.insert(), rows)
        logger.info("vector_documents_saved count=%s", len(rows))

    def load_all_documents(self) -> tuple[list[str], np.ndarray]:
        """Load all vector documents ordered by insertion (id ASC).

        Returns:
            (doc_ids, vectors) where vectors[i] corresponds to doc_ids[i].
        """
        with Session(self.engine) as session:
            rows = session.execute(
                select(VectorDocument.doc_id, VectorDocument.embedding)
                .order_by(VectorDocument.id.asc())
            ).all()

        if not rows:
            return [], np.empty((0,), dtype=np.float32)

        doc_ids = [row.doc_id for row in rows]
        vectors = np.array([row.embedding for row in rows], dtype=np.float32)
>>>>>>> 0869b5537c8feab5210ece8b099d72c680234530
        logger.info("vector_documents_loaded count=%s", len(doc_ids))
        return doc_ids, vectors

    def save_metadata(self, metadata: dict) -> None:
<<<<<<< HEAD
        with Session(self.engine) as session, session.begin():
            stmt = pg_insert(VectorIndexMetadata).values(
                id=1,
                embedding_model=metadata["embedding_model"],
                vector_dimension=int(metadata["vector_dimension"]),
                faiss_index_type=metadata["faiss_index_type"],
                hnsw_m=int(metadata["hnsw_m"]),
                hnsw_ef_construction=int(metadata["hnsw_ef_construction"]),
                hnsw_ef_search=int(metadata["hnsw_ef_search"]),
                vector_count=int(metadata["vector_count"]),
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=["id"],
                set_={
                    "embedding_model": stmt.excluded.embedding_model,
                    "vector_dimension": stmt.excluded.vector_dimension,
                    "faiss_index_type": stmt.excluded.faiss_index_type,
                    "hnsw_m": stmt.excluded.hnsw_m,
                    "hnsw_ef_construction": stmt.excluded.hnsw_ef_construction,
                    "hnsw_ef_search": stmt.excluded.hnsw_ef_search,
                    "vector_count": stmt.excluded.vector_count,
                },
            )
            session.execute(stmt)

    def load_metadata(self) -> dict | None:
        with Session(self.engine) as session:
            row = session.execute(select(VectorIndexMetadata).where(VectorIndexMetadata.id == 1)).scalar_one_or_none()
=======
        """Upsert the single vector index metadata row (id=1)."""
        with Session(self.engine) as session:
            with session.begin():
                stmt = pg_insert(VectorIndexMetadata).values(
                    id=1,
                    embedding_model=metadata["embedding_model"],
                    vector_dimension=int(metadata["vector_dimension"]),
                    faiss_index_type=metadata["faiss_index_type"],
                    hnsw_m=int(metadata["hnsw_m"]),
                    hnsw_ef_construction=int(metadata["hnsw_ef_construction"]),
                    hnsw_ef_search=int(metadata["hnsw_ef_search"]),
                    vector_count=int(metadata["vector_count"]),
                )
                stmt = stmt.on_conflict_do_update(
                    index_elements=["id"],
                    set_={
                        "embedding_model": stmt.excluded.embedding_model,
                        "vector_dimension": stmt.excluded.vector_dimension,
                        "faiss_index_type": stmt.excluded.faiss_index_type,
                        "hnsw_m": stmt.excluded.hnsw_m,
                        "hnsw_ef_construction": stmt.excluded.hnsw_ef_construction,
                        "hnsw_ef_search": stmt.excluded.hnsw_ef_search,
                        "vector_count": stmt.excluded.vector_count,
                    },
                )
                session.execute(stmt)

    def load_metadata(self) -> dict | None:
        """Load the vector index metadata row, or None if not found."""
        with Session(self.engine) as session:
            row = session.execute(
                select(VectorIndexMetadata).where(VectorIndexMetadata.id == 1)
            ).scalar_one_or_none()

>>>>>>> 0869b5537c8feab5210ece8b099d72c680234530
        if row is None:
            return None
        return {
            "embedding_model": row.embedding_model,
            "vector_dimension": row.vector_dimension,
            "faiss_index_type": row.faiss_index_type,
            "hnsw_m": row.hnsw_m,
            "hnsw_ef_construction": row.hnsw_ef_construction,
            "hnsw_ef_search": row.hnsw_ef_search,
            "vector_count": row.vector_count,
        }
<<<<<<< HEAD
=======

    def doc_exists(self, doc_id: str) -> bool:
        """Return True if a document with this ID is already indexed."""
        with Session(self.engine) as session:
            result = session.execute(
                select(VectorDocument.id).where(VectorDocument.doc_id == doc_id).limit(1)
            ).first()
        return result is not None
>>>>>>> 0869b5537c8feab5210ece8b099d72c680234530
