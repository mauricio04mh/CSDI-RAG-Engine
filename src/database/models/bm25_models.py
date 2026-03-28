from __future__ import annotations

from datetime import datetime

from sqlalchemy import BigInteger, Float, ForeignKey, Integer, Text, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class BM25Segment(Base):
    __tablename__ = "bm25_segments"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    segment_id: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now(), nullable=False)
    total_documents: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    total_terms: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    avg_doc_length: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)

    terms: Mapped[list[BM25Term]] = relationship(
        "BM25Term", back_populates="segment", cascade="all, delete-orphan"
    )
    doc_lengths: Mapped[list[BM25DocLength]] = relationship(
        "BM25DocLength", back_populates="segment", cascade="all, delete-orphan"
    )


class BM25Term(Base):
    __tablename__ = "bm25_terms"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    segment_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("bm25_segments.id", ondelete="CASCADE"), nullable=False, index=True
    )
    term: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    doc_freq: Mapped[int] = mapped_column(Integer, nullable=False)

    segment: Mapped[BM25Segment] = relationship("BM25Segment", back_populates="terms")
    postings: Mapped[list[BM25Posting]] = relationship(
        "BM25Posting", back_populates="term_obj", cascade="all, delete-orphan"
    )


class BM25Posting(Base):
    __tablename__ = "bm25_postings"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    term_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("bm25_terms.id", ondelete="CASCADE"), nullable=False, index=True
    )
    doc_id: Mapped[str] = mapped_column(Text, nullable=False)
    tf: Mapped[int] = mapped_column(Integer, nullable=False)

    term_obj: Mapped[BM25Term] = relationship("BM25Term", back_populates="postings")


class BM25DocLength(Base):
    __tablename__ = "bm25_doc_lengths"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    segment_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("bm25_segments.id", ondelete="CASCADE"), nullable=False, index=True
    )
    doc_id: Mapped[str] = mapped_column(Text, nullable=False)
    doc_length: Mapped[int] = mapped_column(Integer, nullable=False)

    segment: Mapped[BM25Segment] = relationship("BM25Segment", back_populates="doc_lengths")
