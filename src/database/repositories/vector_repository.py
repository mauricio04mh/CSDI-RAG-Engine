from __future__ import annotations

import logging

import numpy as np
from sqlalchemy import select
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
        logger.info("vector_documents_loaded count=%s", len(doc_ids))
        return doc_ids, vectors

    def save_metadata(self, metadata: dict) -> None:
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
