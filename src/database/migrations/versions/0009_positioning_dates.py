"""Add editorial dates for positioning

Revision ID: 0009
Revises: 0008
Create Date: 2026-05-19
"""
from alembic import op
import sqlalchemy as sa

revision = "0009"
down_revision = "0008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for table_name in ("source_documents", "chunks", "web_cache_documents", "web_cache_chunks"):
        op.add_column(table_name, sa.Column("published_at", sa.TIMESTAMP(timezone=True), nullable=True))
        op.add_column(table_name, sa.Column("document_updated_at", sa.TIMESTAMP(timezone=True), nullable=True))


def downgrade() -> None:
    for table_name in ("web_cache_chunks", "web_cache_documents", "chunks", "source_documents"):
        op.drop_column(table_name, "document_updated_at")
        op.drop_column(table_name, "published_at")
