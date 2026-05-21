from __future__ import annotations

from datetime import datetime

from sqlalchemy import CheckConstraint, DateTime, Integer, Text, UniqueConstraint, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class QueryFeedback(Base):
    __tablename__ = "query_feedback"
    __table_args__ = (
        CheckConstraint("relevance >= 0 AND relevance <= 3", name="ck_query_feedback_relevance_range"),
        UniqueConstraint(
            "normalized_query",
            "chunk_id",
            "session_id",
            name="uq_query_feedback_query_chunk_session",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    query: Mapped[str] = mapped_column(Text, nullable=False)
    normalized_query: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    chunk_id: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    source_id: Mapped[str | None] = mapped_column(Text, nullable=True, index=True)
    relevance: Mapped[int] = mapped_column(Integer, nullable=False)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    session_id: Mapped[str | None] = mapped_column(Text, nullable=True, index=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )
