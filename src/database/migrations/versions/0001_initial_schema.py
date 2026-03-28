"""initial schema

Revision ID: 0001
Revises:
Create Date: 2026-03-28
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Enable pgvector extension
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")

    # ------------------------------------------------------------------ #
    # BM25 tables
    # ------------------------------------------------------------------ #
    op.create_table(
        "bm25_segments",
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
        "bm25_terms",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("segment_id", sa.Integer(), nullable=False),
        sa.Column("term", sa.Text(), nullable=False),
        sa.Column("doc_freq", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["segment_id"], ["bm25_segments.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("idx_bm25_terms_segment", "bm25_terms", ["segment_id"])
    op.create_index("idx_bm25_terms_term", "bm25_terms", ["term"])

    op.create_table(
        "bm25_postings",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("term_id", sa.BigInteger(), nullable=False),
        sa.Column("doc_id", sa.Text(), nullable=False),
        sa.Column("tf", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["term_id"], ["bm25_terms.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("idx_bm25_postings_term_id", "bm25_postings", ["term_id"])

    op.create_table(
        "bm25_doc_lengths",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("segment_id", sa.Integer(), nullable=False),
        sa.Column("doc_id", sa.Text(), nullable=False),
        sa.Column("doc_length", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["segment_id"], ["bm25_segments.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("idx_bm25_doc_lengths_segment", "bm25_doc_lengths", ["segment_id"])

    # ------------------------------------------------------------------ #
    # Vector tables
    # ------------------------------------------------------------------ #
    op.create_table(
        "vector_documents",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("doc_id", sa.Text(), nullable=False),
        sa.Column("embedding", sa.Text(), nullable=False),  # overridden below
        sa.Column("indexed_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("doc_id"),
    )
    # Replace the placeholder TEXT column with a proper vector(384) column
    op.execute("ALTER TABLE vector_documents ALTER COLUMN embedding TYPE vector(384) USING embedding::vector(384)")
    op.create_index("idx_vector_documents_doc_id", "vector_documents", ["doc_id"])
    op.execute(
        "CREATE INDEX idx_vector_embedding_hnsw ON vector_documents "
        "USING hnsw (embedding vector_ip_ops)"
    )

    op.create_table(
        "vector_index_metadata",
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


def downgrade() -> None:
    op.drop_table("vector_index_metadata")
    op.execute("DROP INDEX IF EXISTS idx_vector_embedding_hnsw")
    op.drop_index("idx_vector_documents_doc_id", table_name="vector_documents")
    op.drop_table("vector_documents")
    op.drop_index("idx_bm25_doc_lengths_segment", table_name="bm25_doc_lengths")
    op.drop_table("bm25_doc_lengths")
    op.drop_index("idx_bm25_postings_term_id", table_name="bm25_postings")
    op.drop_table("bm25_postings")
    op.drop_index("idx_bm25_terms_term", table_name="bm25_terms")
    op.drop_index("idx_bm25_terms_segment", table_name="bm25_terms")
    op.drop_table("bm25_terms")
    op.drop_table("bm25_segments")
