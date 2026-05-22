from __future__ import annotations

import re
from datetime import datetime, timezone

from sqlalchemy import desc, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from src.query_feedback.models import QueryFeedback
from src.query_feedback.schemas import FeedbackRecord, FeedbackSummary

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
        return self.get_feedback_for_normalized_query(normalized_query, session_id=session_id)

    def get_feedback_for_normalized_query(
        self,
        normalized_query: str,
        session_id: str | None = None,
    ) -> list[FeedbackRecord]:
        if not normalized_query.strip():
            raise ValueError("normalized_query must not be empty")
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

    def list_distinct_feedback_queries(self) -> list[FeedbackRecord]:
        distinct_records: list[FeedbackRecord] = []
        seen_queries: set[str] = set()
        for record in self.list_all_feedback():
            if record.normalized_query in seen_queries:
                continue
            seen_queries.add(record.normalized_query)
            distinct_records.append(record)
        return distinct_records

    def list_all_feedback(self) -> list[FeedbackRecord]:
        stmt = select(QueryFeedback).order_by(
            desc(QueryFeedback.updated_at),
            desc(QueryFeedback.created_at),
            desc(QueryFeedback.id),
        )
        with Session(self._engine) as session:
            rows = session.execute(stmt).scalars().all()
        return [self._to_feedback_record(row) for row in rows]

    def get_summary(self) -> FeedbackSummary:
        records = self.list_all_feedback()
        if not records:
            return FeedbackSummary(
                total_feedback_items=0,
                queries_with_feedback=0,
                positive_feedback=0,
                negative_feedback=0,
                marginal_feedback=0,
                average_relevance=0.0,
            )

        total_feedback_items = len(records)
        queries_with_feedback = len({record.normalized_query for record in records})
        positive_feedback = sum(1 for record in records if record.relevance in {2, 3})
        negative_feedback = sum(1 for record in records if record.relevance == 0)
        marginal_feedback = sum(1 for record in records if record.relevance == 1)
        average_relevance = sum(record.relevance for record in records) / total_feedback_items

        return FeedbackSummary(
            total_feedback_items=total_feedback_items,
            queries_with_feedback=queries_with_feedback,
            positive_feedback=positive_feedback,
            negative_feedback=negative_feedback,
            marginal_feedback=marginal_feedback,
            average_relevance=float(average_relevance),
        )

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
