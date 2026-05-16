from __future__ import annotations

import sys
from unittest.mock import MagicMock

# Stub sentence_transformers before any module that imports it is loaded.
# This avoids a huggingface-hub version conflict in the dev environment while
# keeping all production logic intact — tests that need embeddings inject fakes.
if "sentence_transformers" not in sys.modules:
    sys.modules["sentence_transformers"] = MagicMock()

import pytest
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.orm import Session
from testcontainers.postgres import PostgresContainer

# Import model modules so their metadata is registered
from src.database.models.bm25_models import Base as BM25Base
from src.database.models.chunk_models import Base as ChunkBase
from src.database.models.source_document_models import Base as SourceDocumentBase
from src.database.models.vector_models import Base as VectorBase
from src.database.models.web_search_models import Base as WebSearchBase

_POSTGRES_IMAGE = "pgvector/pgvector:pg17"
_TRUNCATE_TABLES = """
    TRUNCATE
        chunks,
        source_documents,
        bm25_postings,
        bm25_doc_lengths,
        bm25_terms,
        bm25_segments,
        vector_documents,
        vector_index_metadata,
        web_search_hits,
        web_search_runs
    RESTART IDENTITY CASCADE
"""


@pytest.fixture(scope="session")
def pg_container():
    """One PostgreSQL container shared across the whole integration test session."""
    with PostgresContainer(image=_POSTGRES_IMAGE) as container:
        yield container


@pytest.fixture(scope="session")
def engine(pg_container: PostgresContainer) -> Engine:
    """SQLAlchemy engine connected to the test container with all tables created."""
    url = pg_container.get_connection_url()
    eng = create_engine(url, future=True)

    with eng.connect() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
        conn.commit()

    ChunkBase.metadata.create_all(eng)
    SourceDocumentBase.metadata.create_all(eng)
    BM25Base.metadata.create_all(eng)
    VectorBase.metadata.create_all(eng)
    WebSearchBase.metadata.create_all(eng)

    yield eng
    eng.dispose()


@pytest.fixture(autouse=True)
def clean_db(engine: Engine) -> None:
    """Truncate all tables before every integration test to guarantee a clean slate."""
    with Session(engine) as session:
        session.execute(text(_TRUNCATE_TABLES))
        session.commit()
