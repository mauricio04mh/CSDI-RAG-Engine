"""Change vector embedding dimension from 384 to 1024 for BAAI/bge-m3.

Revision ID: 0006
Revises: 0005
Create Date: 2026-05-16
"""
from __future__ import annotations

from alembic import op
from pgvector.sqlalchemy import Vector

revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Table must be empty before altering vector dimension (pgvector constraint).
    op.execute("TRUNCATE TABLE vector_documents RESTART IDENTITY")
    op.alter_column(
        "vector_documents",
        "embedding",
        type_=Vector(1024),
        existing_type=Vector(384),
        postgresql_using="NULL::vector(1024)",
    )


def downgrade() -> None:
    op.execute("TRUNCATE TABLE vector_documents RESTART IDENTITY")
    op.alter_column(
        "vector_documents",
        "embedding",
        type_=Vector(384),
        existing_type=Vector(1024),
        postgresql_using="NULL::vector(384)",
    )
