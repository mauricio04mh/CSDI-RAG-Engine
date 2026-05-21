from __future__ import annotations

import re
from datetime import datetime, timezone

from sqlalchemy import desc, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from src.query_feedback.models import QueryFeedback
from src.query_feedback.schemas import FeedbackRecord

_WHITESPACE_RE = re.compile(r"\s+")


def normalize_query(query: str) -> str:
    return _WHITESPACE_RE.sub(" ", query.strip().lower())


class FeedbackRepository:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def add_or_update_feedback(
        self,
        query: str,
        chunk_id: str,
        relevance: int,
        source_id: str | None = None,
        notes: str | None = None,
        session_id: str | None = None,
    ) -> FeedbackRecord:
        if not query.strip():
            raise ValueError("query must not be empty")
        if not chunk_id.strip():
            raise ValueError("chunk_id must not be empty")
        if relevance < 0 or relevance > 3:
            raise ValueError("relevance must be between 0 and 3")

        normalized_query = normalize_query(query)
        cleaned_chunk_id = chunk_id.strip()

        with Session(self._engine) as session, session.begin():
            row = session.execute(
                self._feedback_lookup_statement(
                    normalized_query=normalized_query,
                    chunk_id=cleaned_chunk_id,
                    session_id=session_id,
                )
            ).scalar_one_or_none()

            if row is None:
                row = QueryFeedback(
                    query=query,
                    normalized_query=normalized_query,
                    chunk_id=cleaned_chunk_id,
                    source_id=source_id,
                    relevance=relevance,
                    notes=notes,
                    session_id=session_id,
                )
                session.add(row)
                session.flush()
            else:
                row.query = query
                row.normalized_query = normalized_query
                row.chunk_id = cleaned_chunk_id
                row.source_id = source_id
                row.relevance = relevance
                row.notes = notes
                row.session_id = session_id
                row.updated_at = datetime.now(timezone.utc)
                session.flush()

            session.refresh(row)
            return self._to_feedback_record(row)

    def get_feedback_for_query(
        self,
        query: str,
        session_id: str | None = None,
    ) -> list[FeedbackRecord]:
        normalized_query = normalize_query(query)
        stmt = select(QueryFeedback).where(QueryFeedback.normalized_query == normalized_query)
        if session_id is not None:
            stmt = stmt.where(QueryFeedback.session_id == session_id)
        stmt = stmt.order_by(
            desc(QueryFeedback.updated_at),
            desc(QueryFeedback.created_at),
            desc(QueryFeedback.id),
        )
        with Session(self._engine) as session:
            rows = session.execute(stmt).scalars().all()
        return [self._to_feedback_record(row) for row in rows]

    def get_feedback_for_chunk(self, chunk_id: str) -> list[FeedbackRecord]:
        stmt = (
            select(QueryFeedback)
            .where(QueryFeedback.chunk_id == chunk_id)
            .order_by(
                desc(QueryFeedback.updated_at),
                desc(QueryFeedback.created_at),
                desc(QueryFeedback.id),
            )
        )
        with Session(self._engine) as session:
            rows = session.execute(stmt).scalars().all()
        return [self._to_feedback_record(row) for row in rows]

    def _feedback_lookup_statement(
        self,
        normalized_query: str,
        chunk_id: str,
        session_id: str | None,
    ):
        stmt = select(QueryFeedback).where(
            QueryFeedback.normalized_query == normalized_query,
            QueryFeedback.chunk_id == chunk_id,
        )
        if session_id is None:
            stmt = stmt.where(QueryFeedback.session_id.is_(None))
        else:
            stmt = stmt.where(QueryFeedback.session_id == session_id)
        return stmt

    def _to_feedback_record(self, row: QueryFeedback) -> FeedbackRecord:
        return FeedbackRecord(
            id=row.id,
            query=row.query,
            normalized_query=row.normalized_query,
            chunk_id=row.chunk_id,
            source_id=row.source_id,
            relevance=row.relevance,
            notes=row.notes,
            session_id=row.session_id,
            created_at=row.created_at,
            updated_at=row.updated_at,
        )
