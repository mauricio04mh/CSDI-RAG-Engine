"""source documents table

Revision ID: 0004
Revises: 0003
Create Date: 2026-05-13
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0004"
down_revision: Union[str, None] = "0003"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "source_documents",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("document_id", sa.Text(), nullable=False),
        sa.Column("source_id", sa.Text(), nullable=False),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("normalized_url", sa.Text(), nullable=False),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("breadcrumb", sa.Text(), nullable=False, server_default=""),
        sa.Column("text_content", sa.Text(), nullable=False),
        sa.Column("raw_html", sa.Text(), nullable=True),
        sa.Column("code_blocks", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'[]'::jsonb")),
        sa.Column("content_type", sa.Text(), nullable=False, server_default="text/html"),
        sa.Column("http_status", sa.Integer(), nullable=False, server_default="200"),
        sa.Column("fetch_method", sa.Text(), nullable=False, server_default="http"),
        sa.Column("crawl_depth", sa.Integer(), nullable=True),
        sa.Column("discovered_from_url", sa.Text(), nullable=True),
        sa.Column("content_hash", sa.Text(), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("fetched_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("last_seen_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("document_id"),
        sa.UniqueConstraint("normalized_url"),
    )
    op.create_index("idx_source_documents_document_id", "source_documents", ["document_id"])
    op.create_index("idx_source_documents_source_id", "source_documents", ["source_id"])
    op.create_index("idx_source_documents_normalized_url", "source_documents", ["normalized_url"])
    op.create_index("idx_source_documents_content_hash", "source_documents", ["content_hash"])
    op.create_index("idx_source_documents_last_seen_at", "source_documents", ["last_seen_at"])


def downgrade() -> None:
    op.drop_index("idx_source_documents_last_seen_at", table_name="source_documents")
    op.drop_index("idx_source_documents_content_hash", table_name="source_documents")
    op.drop_index("idx_source_documents_normalized_url", table_name="source_documents")
    op.drop_index("idx_source_documents_source_id", table_name="source_documents")
    op.drop_index("idx_source_documents_document_id", table_name="source_documents")
    op.drop_table("source_documents")
