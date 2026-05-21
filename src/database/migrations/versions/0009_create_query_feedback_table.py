"""create query feedback table

Revision ID: 0009
Revises: 0008
Create Date: 2026-05-21
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0009"
down_revision: Union[str, None] = "0008"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "query_feedback",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("query", sa.Text(), nullable=False),
        sa.Column("normalized_query", sa.Text(), nullable=False),
        sa.Column("chunk_id", sa.Text(), nullable=False),
        sa.Column("source_id", sa.Text(), nullable=True),
        sa.Column("relevance", sa.Integer(), nullable=False),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("session_id", sa.Text(), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.CheckConstraint("relevance >= 0 AND relevance <= 3", name="ck_query_feedback_relevance_range"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "normalized_query",
            "chunk_id",
            "session_id",
            name="uq_query_feedback_query_chunk_session",
        ),
    )
    op.create_index("idx_query_feedback_normalized_query", "query_feedback", ["normalized_query"])
    op.create_index("idx_query_feedback_chunk_id", "query_feedback", ["chunk_id"])
    op.create_index("idx_query_feedback_source_id", "query_feedback", ["source_id"])
    op.create_index("idx_query_feedback_session_id", "query_feedback", ["session_id"])


def downgrade() -> None:
    op.drop_index("idx_query_feedback_session_id", table_name="query_feedback")
    op.drop_index("idx_query_feedback_source_id", table_name="query_feedback")
    op.drop_index("idx_query_feedback_chunk_id", table_name="query_feedback")
    op.drop_index("idx_query_feedback_normalized_query", table_name="query_feedback")
    op.drop_table("query_feedback")
