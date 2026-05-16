from __future__ import annotations

from sqlalchemy import Boolean, Integer, Text, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class SourceDocument(Base):
    __tablename__ = "source_documents"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    document_id: Mapped[str] = mapped_column(Text, nullable=False, unique=True, index=True)
    source_id: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    url: Mapped[str] = mapped_column(Text, nullable=False)
    normalized_url: Mapped[str] = mapped_column(Text, nullable=False, unique=True, index=True)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    breadcrumb: Mapped[str] = mapped_column(Text, nullable=False, default="")
    text_content: Mapped[str] = mapped_column(Text, nullable=False)
    raw_html: Mapped[str | None] = mapped_column(Text, nullable=True)
    code_blocks: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list)
    content_type: Mapped[str] = mapped_column(Text, nullable=False, default="text/html")
    http_status: Mapped[int] = mapped_column(Integer, nullable=False, default=200)
    fetch_method: Mapped[str] = mapped_column(Text, nullable=False, default="http")
    crawl_depth: Mapped[int | None] = mapped_column(Integer, nullable=True)
    discovered_from_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    content_hash: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default="true")
    fetched_at: Mapped[str] = mapped_column(nullable=False, server_default=func.now())
    last_seen_at: Mapped[str] = mapped_column(nullable=False, server_default=func.now(), index=True)
    created_at: Mapped[str] = mapped_column(nullable=False, server_default=func.now())
    updated_at: Mapped[str] = mapped_column(nullable=False, server_default=func.now(), onupdate=func.now())
