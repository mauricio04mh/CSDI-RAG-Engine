"""Fix web_cache_vector_documents embedding dimension to 1024 (bge-m3)

Revision ID: 0008
Revises: 0007
Create Date: 2026-05-16
"""
from alembic import op

revision = "0008"
down_revision = "0007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("TRUNCATE TABLE web_cache_vector_documents RESTART IDENTITY")
    op.execute(
        "ALTER TABLE web_cache_vector_documents "
        "ALTER COLUMN embedding TYPE vector(1024) "
        "USING NULL::vector(1024)"
    )


def downgrade() -> None:
    op.execute("TRUNCATE TABLE web_cache_vector_documents RESTART IDENTITY")
    op.execute(
        "ALTER TABLE web_cache_vector_documents "
        "ALTER COLUMN embedding TYPE vector(384) "
        "USING NULL::vector(384)"
    )
