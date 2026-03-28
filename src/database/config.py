from __future__ import annotations

import os

from sqlalchemy import Engine, create_engine

_DEFAULT_URL = "postgresql://raguser:ragpassword@localhost:5432/ragengine"


def build_engine(database_url: str | None = None) -> Engine:
    """Create a connection-pooled synchronous SQLAlchemy engine."""
    url = database_url or os.getenv("DATABASE_URL", _DEFAULT_URL)
<<<<<<< HEAD
    return create_engine(url, pool_size=10, max_overflow=20, pool_pre_ping=True, future=True)
=======
    return create_engine(
        url,
        pool_size=10,
        max_overflow=20,
        pool_pre_ping=True,
        future=True,
    )
>>>>>>> 0869b5537c8feab5210ece8b099d72c680234530
