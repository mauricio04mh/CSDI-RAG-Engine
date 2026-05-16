"""vector_documents soft delete

Revision ID: 0005
Revises: 0004
Create Date: 2026-05-16
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0005"
down_revision: Union[str, None] = "0004"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "vector_documents",
        sa.Column("deleted_at", sa.TIMESTAMP(timezone=True), nullable=True),
    )
    op.create_index(
        "idx_vector_documents_deleted_at",
        "vector_documents",
        ["deleted_at"],
    )


def downgrade() -> None:
    op.drop_index("idx_vector_documents_deleted_at", table_name="vector_documents")
    op.drop_column("vector_documents", "deleted_at")
