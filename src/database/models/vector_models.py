from __future__ import annotations

from sqlalchemy import Integer, Text, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from pgvector.sqlalchemy import Vector


class Base(DeclarativeBase):
    pass


class VectorDocument(Base):
    __tablename__ = "vector_documents"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    doc_id: Mapped[str] = mapped_column(Text, nullable=False, unique=True, index=True)
    embedding: Mapped[list[float]] = mapped_column(Vector(384), nullable=False)
    indexed_at: Mapped[str] = mapped_column(server_default=func.now(), nullable=False)


class VectorIndexMetadata(Base):
    __tablename__ = "vector_index_metadata"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, default=1)
    embedding_model: Mapped[str] = mapped_column(Text, nullable=False)
    vector_dimension: Mapped[int] = mapped_column(Integer, nullable=False)
    faiss_index_type: Mapped[str] = mapped_column(Text, nullable=False)
    hnsw_m: Mapped[int] = mapped_column(Integer, nullable=False)
    hnsw_ef_construction: Mapped[int] = mapped_column(Integer, nullable=False)
    hnsw_ef_search: Mapped[int] = mapped_column(Integer, nullable=False)
    vector_count: Mapped[int] = mapped_column(Integer, nullable=False)
    updated_at: Mapped[str] = mapped_column(server_default=func.now(), nullable=False)
