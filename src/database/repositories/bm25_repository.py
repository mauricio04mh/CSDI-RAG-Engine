from __future__ import annotations

import logging
from collections import defaultdict

from sqlalchemy import delete, select
<<<<<<< HEAD
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
=======
from sqlalchemy.orm import Session
from sqlalchemy.engine import Engine
>>>>>>> 0869b5537c8feab5210ece8b099d72c680234530

from src.database.models.bm25_models import BM25DocLength, BM25Posting, BM25Segment, BM25Term
from src.indexing.builder.segment_builder import IndexSegment

logger = logging.getLogger(__name__)


class BM25Repository:
    """All SQL operations for the BM25 inverted index domain."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def write_segment(self, segment: IndexSegment) -> None:
        """Persist an IndexSegment to the four BM25 tables in one transaction."""
<<<<<<< HEAD
        with Session(self.engine) as session, session.begin():
            db_seg = BM25Segment(
                segment_id=segment.segment_id,
                total_documents=int(segment.stats.get("total_documents", 0)),
                total_terms=int(segment.stats.get("total_terms", 0)),
                avg_doc_length=float(segment.stats.get("average_document_length", 0.0)),
            )
            session.add(db_seg)
            session.flush()

            session.execute(BM25Term.__table__.insert(), [
                {"segment_id": db_seg.id, "term": t, "doc_freq": df}
                for t, df in segment.dictionary.items()
            ])

            term_id_map = {
                row.term: row.id
                for row in session.execute(
                    select(BM25Term.id, BM25Term.term).where(BM25Term.segment_id == db_seg.id)
                ).all()
            }

            posting_rows = [
                {"term_id": term_id_map[term], "doc_id": p["doc_id"], "tf": int(p["tf"])}
                for term, postings in segment.postings.items()
                for p in postings
            ]
            if posting_rows:
                session.execute(BM25Posting.__table__.insert(), posting_rows)

            dl_rows = [
                {"segment_id": db_seg.id, "doc_id": doc_id, "doc_length": length}
                for doc_id, length in segment.doc_lengths.items()
            ]
            if dl_rows:
                session.execute(BM25DocLength.__table__.insert(), dl_rows)
=======
        with Session(self.engine) as session:
            with session.begin():
                db_segment = BM25Segment(
                    segment_id=segment.segment_id,
                    total_documents=int(segment.stats.get("total_documents", 0)),
                    total_terms=int(segment.stats.get("total_terms", 0)),
                    avg_doc_length=float(segment.stats.get("average_document_length", 0.0)),
                )
                session.add(db_segment)
                session.flush()  # populate db_segment.id

                # Bulk insert terms and retrieve their generated IDs
                term_rows = [
                    {"segment_id": db_segment.id, "term": term, "doc_freq": df}
                    for term, df in segment.dictionary.items()
                ]
                session.execute(BM25Term.__table__.insert(), term_rows)

                term_records = session.execute(
                    select(BM25Term.id, BM25Term.term).where(BM25Term.segment_id == db_segment.id)
                ).all()
                term_id_map = {row.term: row.id for row in term_records}

                # Bulk insert postings
                posting_rows = [
                    {"term_id": term_id_map[term], "doc_id": posting["doc_id"], "tf": int(posting["tf"])}
                    for term, postings in segment.postings.items()
                    for posting in postings
                ]
                if posting_rows:
                    session.execute(BM25Posting.__table__.insert(), posting_rows)

                # Bulk insert doc lengths
                doc_length_rows = [
                    {"segment_id": db_segment.id, "doc_id": doc_id, "doc_length": length}
                    for doc_id, length in segment.doc_lengths.items()
                ]
                if doc_length_rows:
                    session.execute(BM25DocLength.__table__.insert(), doc_length_rows)
>>>>>>> 0869b5537c8feab5210ece8b099d72c680234530

        logger.info("bm25_segment_persisted segment_id=%s", segment.segment_id)

    def delete_segment(self, segment_id: str) -> None:
<<<<<<< HEAD
        with Session(self.engine) as session, session.begin():
            session.execute(delete(BM25Segment).where(BM25Segment.segment_id == segment_id))
        logger.info("bm25_segment_deleted segment_id=%s", segment_id)

    def merge_and_replace(self, merged_segment: IndexSegment, source_segment_ids: list[str]) -> None:
        """Write the merged segment and delete source segments in one transaction."""
        with Session(self.engine) as session, session.begin():
            db_seg = BM25Segment(
                segment_id=merged_segment.segment_id,
                total_documents=int(merged_segment.stats.get("total_documents", 0)),
                total_terms=int(merged_segment.stats.get("total_terms", 0)),
                avg_doc_length=float(merged_segment.stats.get("average_document_length", 0.0)),
            )
            session.add(db_seg)
            session.flush()

            session.execute(BM25Term.__table__.insert(), [
                {"segment_id": db_seg.id, "term": t, "doc_freq": df}
                for t, df in merged_segment.dictionary.items()
            ])
            term_id_map = {
                row.term: row.id
                for row in session.execute(
                    select(BM25Term.id, BM25Term.term).where(BM25Term.segment_id == db_seg.id)
                ).all()
            }
            posting_rows = [
                {"term_id": term_id_map[term], "doc_id": p["doc_id"], "tf": int(p["tf"])}
                for term, postings in merged_segment.postings.items()
                for p in postings
            ]
            if posting_rows:
                session.execute(BM25Posting.__table__.insert(), posting_rows)
            dl_rows = [
                {"segment_id": db_seg.id, "doc_id": doc_id, "doc_length": length}
                for doc_id, length in merged_segment.doc_lengths.items()
            ]
            if dl_rows:
                session.execute(BM25DocLength.__table__.insert(), dl_rows)

            session.execute(delete(BM25Segment).where(BM25Segment.segment_id.in_(source_segment_ids)))

        logger.info("bm25_merge_persisted merged=%s sources=%s", merged_segment.segment_id, ",".join(source_segment_ids))
=======
        """Delete a segment and cascade to terms/postings/doc_lengths."""
        with Session(self.engine) as session:
            with session.begin():
                session.execute(
                    delete(BM25Segment).where(BM25Segment.segment_id == segment_id)
                )
        logger.info("bm25_segment_deleted segment_id=%s", segment_id)

    def merge_and_replace(self, merged_segment: IndexSegment, source_segment_ids: list[str]) -> None:
        """Write the merged segment and delete source segments atomically."""
        with Session(self.engine) as session:
            with session.begin():
                # Insert merged segment
                db_segment = BM25Segment(
                    segment_id=merged_segment.segment_id,
                    total_documents=int(merged_segment.stats.get("total_documents", 0)),
                    total_terms=int(merged_segment.stats.get("total_terms", 0)),
                    avg_doc_length=float(merged_segment.stats.get("average_document_length", 0.0)),
                )
                session.add(db_segment)
                session.flush()

                term_rows = [
                    {"segment_id": db_segment.id, "term": term, "doc_freq": df}
                    for term, df in merged_segment.dictionary.items()
                ]
                session.execute(BM25Term.__table__.insert(), term_rows)

                term_records = session.execute(
                    select(BM25Term.id, BM25Term.term).where(BM25Term.segment_id == db_segment.id)
                ).all()
                term_id_map = {row.term: row.id for row in term_records}

                posting_rows = [
                    {"term_id": term_id_map[term], "doc_id": posting["doc_id"], "tf": int(posting["tf"])}
                    for term, postings in merged_segment.postings.items()
                    for posting in postings
                ]
                if posting_rows:
                    session.execute(BM25Posting.__table__.insert(), posting_rows)

                doc_length_rows = [
                    {"segment_id": db_segment.id, "doc_id": doc_id, "doc_length": length}
                    for doc_id, length in merged_segment.doc_lengths.items()
                ]
                if doc_length_rows:
                    session.execute(BM25DocLength.__table__.insert(), doc_length_rows)

                # Delete source segments (cascades to terms/postings/doc_lengths)
                session.execute(
                    delete(BM25Segment).where(BM25Segment.segment_id.in_(source_segment_ids))
                )

        logger.info(
            "bm25_merge_persisted merged_segment_id=%s sources=%s",
            merged_segment.segment_id,
            ",".join(source_segment_ids),
        )
>>>>>>> 0869b5537c8feab5210ece8b099d72c680234530

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def list_segment_ids(self) -> list[str]:
<<<<<<< HEAD
        with Session(self.engine) as session:
            rows = session.execute(select(BM25Segment.segment_id).order_by(BM25Segment.created_at.asc())).all()
        return [r.segment_id for r in rows]

    def read_segment(self, segment_id: str) -> IndexSegment:
        with Session(self.engine) as session:
            db_seg = session.execute(select(BM25Segment).where(BM25Segment.segment_id == segment_id)).scalar_one()
=======
        """Return all segment IDs ordered by creation time (ascending)."""
        with Session(self.engine) as session:
            rows = session.execute(
                select(BM25Segment.segment_id).order_by(BM25Segment.created_at.asc())
            ).all()
        return [row.segment_id for row in rows]

    def read_segment(self, segment_id: str) -> IndexSegment:
        """Reconstruct an IndexSegment from the database."""
        with Session(self.engine) as session:
            db_seg = session.execute(
                select(BM25Segment).where(BM25Segment.segment_id == segment_id)
            ).scalar_one()
>>>>>>> 0869b5537c8feab5210ece8b099d72c680234530

            posting_rows = session.execute(
                select(BM25Term.term, BM25Posting.doc_id, BM25Posting.tf)
                .join(BM25Posting, BM25Term.id == BM25Posting.term_id)
                .where(BM25Term.segment_id == db_seg.id)
            ).all()

<<<<<<< HEAD
            dl_rows = session.execute(
=======
            doc_length_rows = session.execute(
>>>>>>> 0869b5537c8feab5210ece8b099d72c680234530
                select(BM25DocLength.doc_id, BM25DocLength.doc_length)
                .where(BM25DocLength.segment_id == db_seg.id)
            ).all()

            postings: dict[str, list[dict]] = defaultdict(list)
            for term, doc_id, tf in posting_rows:
                postings[term].append({"doc_id": doc_id, "tf": tf})

<<<<<<< HEAD
            return IndexSegment(
                segment_id=segment_id,
                dictionary={t: len(p) for t, p in postings.items()},
                postings=dict(postings),
                doc_lengths={r.doc_id: r.doc_length for r in dl_rows},
                stats={
                    "total_documents": db_seg.total_documents,
                    "total_terms": db_seg.total_terms,
                    "average_document_length": db_seg.avg_doc_length,
                },
            )

    def load_full_index(self) -> tuple[dict, dict, dict, int, float]:
        """Return (raw_postings, dictionary, doc_lengths, total_documents, avg_doc_length)."""
=======
            dictionary = {term: len(pl) for term, pl in postings.items()}
            doc_lengths = {row.doc_id: row.doc_length for row in doc_length_rows}
            stats = {
                "total_documents": db_seg.total_documents,
                "total_terms": db_seg.total_terms,
                "average_document_length": db_seg.avg_doc_length,
            }

        return IndexSegment(
            segment_id=segment_id,
            dictionary=dictionary,
            postings=dict(postings),
            doc_lengths=doc_lengths,
            stats=stats,
        )

    def load_full_index(
        self,
    ) -> tuple[dict[str, int], dict[str, list[dict]], dict[str, int], int, float]:
        """Load the entire BM25 index from all segments.

        Returns:
            (dictionary, raw_postings, doc_lengths, total_documents, avg_document_length)
        """
>>>>>>> 0869b5537c8feab5210ece8b099d72c680234530
        with Session(self.engine) as session:
            posting_rows = session.execute(
                select(BM25Term.term, BM25Posting.doc_id, BM25Posting.tf)
                .join(BM25Posting, BM25Term.id == BM25Posting.term_id)
            ).all()
<<<<<<< HEAD
            dl_rows = session.execute(select(BM25DocLength.doc_id, BM25DocLength.doc_length)).all()
=======

            doc_length_rows = session.execute(
                select(BM25DocLength.doc_id, BM25DocLength.doc_length)
            ).all()
>>>>>>> 0869b5537c8feab5210ece8b099d72c680234530

        raw_postings: dict[str, list[dict]] = defaultdict(list)
        for term, doc_id, tf in posting_rows:
            raw_postings[term].append({"doc_id": doc_id, "tf": tf})

<<<<<<< HEAD
        doc_lengths = {r.doc_id: r.doc_length for r in dl_rows}
        total_documents = len(doc_lengths)
        total_terms = sum(doc_lengths.values())
        avg_doc_length = total_terms / total_documents if total_documents else 0.0

        logger.info("bm25_full_index_loaded documents=%s terms=%s", total_documents, len(raw_postings))
        return dict(raw_postings), {t: len(p) for t, p in raw_postings.items()}, doc_lengths, total_documents, avg_doc_length
=======
        dictionary = {term: len(pl) for term, pl in raw_postings.items()}
        doc_lengths = {row.doc_id: row.doc_length for row in doc_length_rows}
        total_documents = len(doc_lengths)
        total_terms = sum(doc_lengths.values())
        avg_document_length = total_terms / total_documents if total_documents else 0.0

        logger.info(
            "bm25_full_index_loaded documents=%s terms=%s",
            total_documents,
            len(dictionary),
        )
        return dict(raw_postings), dictionary, doc_lengths, total_documents, avg_document_length
>>>>>>> 0869b5537c8feab5210ece8b099d72c680234530
