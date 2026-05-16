from __future__ import annotations

from datetime import datetime

from pgvector.sqlalchemy import Vector
from sqlalchemy import BigInteger, Float, ForeignKey, Integer, Text, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class WebCacheDocument(Base):
    __tablename__ = "web_cache_documents"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    url: Mapped[str] = mapped_column(Text, nullable=False, unique=True, index=True)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    text_content: Mapped[str] = mapped_column(Text, nullable=False)
    provider: Mapped[str] = mapped_column(Text, nullable=False, default="web")
    content_hash: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    extra_metadata: Mapped[dict] = mapped_column("metadata", JSONB, nullable=False, default=dict)
    fetched_at: Mapped[str] = mapped_column(server_default=func.now(), nullable=False)
    created_at: Mapped[str] = mapped_column(server_default=func.now(), nullable=False)
    updated_at: Mapped[str] = mapped_column(server_default=func.now(), onupdate=func.now(), nullable=False)


class WebCacheChunk(Base):
    __tablename__ = "web_cache_chunks"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    chunk_id: Mapped[str] = mapped_column(Text, nullable=False, unique=True, index=True)
    source_id: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    url: Mapped[str] = mapped_column(Text, nullable=False)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    breadcrumb: Mapped[str] = mapped_column(Text, nullable=False, default="")
    text: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[str] = mapped_column(server_default=func.now(), nullable=False)


class WebCacheVectorDocument(Base):
    __tablename__ = "web_cache_vector_documents"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    doc_id: Mapped[str] = mapped_column(Text, nullable=False, unique=True, index=True)
    embedding: Mapped[list[float]] = mapped_column(Vector(384), nullable=False)
    indexed_at: Mapped[str] = mapped_column(server_default=func.now(), nullable=False)


class WebCacheVectorIndexMetadata(Base):
    __tablename__ = "web_cache_vector_index_metadata"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, default=1)
    embedding_model: Mapped[str] = mapped_column(Text, nullable=False)
    vector_dimension: Mapped[int] = mapped_column(Integer, nullable=False)
    faiss_index_type: Mapped[str] = mapped_column(Text, nullable=False)
    hnsw_m: Mapped[int] = mapped_column(Integer, nullable=False)
    hnsw_ef_construction: Mapped[int] = mapped_column(Integer, nullable=False)
    hnsw_ef_search: Mapped[int] = mapped_column(Integer, nullable=False)
    vector_count: Mapped[int] = mapped_column(Integer, nullable=False)
    updated_at: Mapped[str] = mapped_column(server_default=func.now(), onupdate=func.now(), nullable=False)


class WebCacheBM25Segment(Base):
    __tablename__ = "web_cache_bm25_segments"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    segment_id: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now(), nullable=False)
    total_documents: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    total_terms: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    avg_doc_length: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)

    terms: Mapped[list[WebCacheBM25Term]] = relationship(
        "WebCacheBM25Term", back_populates="segment", cascade="all, delete-orphan"
    )
    doc_lengths: Mapped[list[WebCacheBM25DocLength]] = relationship(
        "WebCacheBM25DocLength", back_populates="segment", cascade="all, delete-orphan"
    )


class WebCacheBM25Term(Base):
    __tablename__ = "web_cache_bm25_terms"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    segment_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("web_cache_bm25_segments.id", ondelete="CASCADE"), nullable=False, index=True
    )
    term: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    doc_freq: Mapped[int] = mapped_column(Integer, nullable=False)

    segment: Mapped[WebCacheBM25Segment] = relationship("WebCacheBM25Segment", back_populates="terms")
    postings: Mapped[list[WebCacheBM25Posting]] = relationship(
        "WebCacheBM25Posting", back_populates="term_obj", cascade="all, delete-orphan"
    )


class WebCacheBM25Posting(Base):
    __tablename__ = "web_cache_bm25_postings"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    term_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("web_cache_bm25_terms.id", ondelete="CASCADE"), nullable=False, index=True
    )
    doc_id: Mapped[str] = mapped_column(Text, nullable=False)
    tf: Mapped[int] = mapped_column(Integer, nullable=False)

    term_obj: Mapped[WebCacheBM25Term] = relationship("WebCacheBM25Term", back_populates="postings")


class WebCacheBM25DocLength(Base):
    __tablename__ = "web_cache_bm25_doc_lengths"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    segment_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("web_cache_bm25_segments.id", ondelete="CASCADE"), nullable=False, index=True
    )
    doc_id: Mapped[str] = mapped_column(Text, nullable=False)
    doc_length: Mapped[int] = mapped_column(Integer, nullable=False)

    segment: Mapped[WebCacheBM25Segment] = relationship("WebCacheBM25Segment", back_populates="doc_lengths")
