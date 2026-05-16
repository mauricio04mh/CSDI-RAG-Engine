"""web cache tables

Revision ID: 0005
Revises: 0004
Create Date: 2026-05-16
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0005"
down_revision: Union[str, None] = "0004"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "web_cache_documents",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("text_content", sa.Text(), nullable=False),
        sa.Column("provider", sa.Text(), nullable=False, server_default="web"),
        sa.Column("content_hash", sa.Text(), nullable=False),
        sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("fetched_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("url"),
    )
    op.create_index("idx_web_cache_documents_url", "web_cache_documents", ["url"])
    op.create_index("idx_web_cache_documents_content_hash", "web_cache_documents", ["content_hash"])

    op.create_table(
        "web_cache_chunks",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("chunk_id", sa.Text(), nullable=False),
        sa.Column("source_id", sa.Text(), nullable=False),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("breadcrumb", sa.Text(), nullable=False, server_default=""),
        sa.Column("text", sa.Text(), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("chunk_id"),
    )
    op.create_index("idx_web_cache_chunks_chunk_id", "web_cache_chunks", ["chunk_id"])
    op.create_index("idx_web_cache_chunks_source_id", "web_cache_chunks", ["source_id"])

    op.create_table(
        "web_cache_vector_documents",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("doc_id", sa.Text(), nullable=False),
        sa.Column("embedding", sa.Text(), nullable=False),
        sa.Column("indexed_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("doc_id"),
    )
    op.execute("ALTER TABLE web_cache_vector_documents ALTER COLUMN embedding TYPE vector(384) USING embedding::vector(384)")
    op.create_index("idx_web_cache_vector_documents_doc_id", "web_cache_vector_documents", ["doc_id"])
    op.execute(
        "CREATE INDEX idx_web_cache_vector_embedding_hnsw ON web_cache_vector_documents "
        "USING hnsw (embedding vector_ip_ops)"
    )

    op.create_table(
        "web_cache_vector_index_metadata",
        sa.Column("id", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("embedding_model", sa.Text(), nullable=False),
        sa.Column("vector_dimension", sa.Integer(), nullable=False),
        sa.Column("faiss_index_type", sa.Text(), nullable=False),
        sa.Column("hnsw_m", sa.Integer(), nullable=False),
        sa.Column("hnsw_ef_construction", sa.Integer(), nullable=False),
        sa.Column("hnsw_ef_search", sa.Integer(), nullable=False),
        sa.Column("vector_count", sa.Integer(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_table(
        "web_cache_bm25_segments",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("segment_id", sa.Text(), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("total_documents", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("total_terms", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("avg_doc_length", sa.Float(), nullable=False, server_default="0.0"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("segment_id"),
    )
    op.create_table(
        "web_cache_bm25_terms",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("segment_id", sa.Integer(), nullable=False),
        sa.Column("term", sa.Text(), nullable=False),
        sa.Column("doc_freq", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["segment_id"], ["web_cache_bm25_segments.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("idx_web_cache_bm25_terms_segment", "web_cache_bm25_terms", ["segment_id"])
    op.create_index("idx_web_cache_bm25_terms_term", "web_cache_bm25_terms", ["term"])
    op.create_table(
        "web_cache_bm25_postings",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("term_id", sa.BigInteger(), nullable=False),
        sa.Column("doc_id", sa.Text(), nullable=False),
        sa.Column("tf", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["term_id"], ["web_cache_bm25_terms.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("idx_web_cache_bm25_postings_term_id", "web_cache_bm25_postings", ["term_id"])
    op.create_table(
        "web_cache_bm25_doc_lengths",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("segment_id", sa.Integer(), nullable=False),
        sa.Column("doc_id", sa.Text(), nullable=False),
        sa.Column("doc_length", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["segment_id"], ["web_cache_bm25_segments.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("idx_web_cache_bm25_doc_lengths_segment", "web_cache_bm25_doc_lengths", ["segment_id"])


def downgrade() -> None:
    op.drop_index("idx_web_cache_bm25_doc_lengths_segment", table_name="web_cache_bm25_doc_lengths")
    op.drop_table("web_cache_bm25_doc_lengths")
    op.drop_index("idx_web_cache_bm25_postings_term_id", table_name="web_cache_bm25_postings")
    op.drop_table("web_cache_bm25_postings")
    op.drop_index("idx_web_cache_bm25_terms_term", table_name="web_cache_bm25_terms")
    op.drop_index("idx_web_cache_bm25_terms_segment", table_name="web_cache_bm25_terms")
    op.drop_table("web_cache_bm25_terms")
    op.drop_table("web_cache_bm25_segments")
    op.drop_table("web_cache_vector_index_metadata")
    op.execute("DROP INDEX IF EXISTS idx_web_cache_vector_embedding_hnsw")
    op.drop_index("idx_web_cache_vector_documents_doc_id", table_name="web_cache_vector_documents")
    op.drop_table("web_cache_vector_documents")
    op.drop_index("idx_web_cache_chunks_source_id", table_name="web_cache_chunks")
    op.drop_index("idx_web_cache_chunks_chunk_id", table_name="web_cache_chunks")
    op.drop_table("web_cache_chunks")
    op.drop_index("idx_web_cache_documents_content_hash", table_name="web_cache_documents")
    op.drop_index("idx_web_cache_documents_url", table_name="web_cache_documents")
    op.drop_table("web_cache_documents")
