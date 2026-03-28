from __future__ import annotations

import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

from src.database.models.bm25_models import Base as BM25Base
from src.database.models.chunk_models import Base as ChunkBase
from src.database.models.vector_models import Base as VectorBase

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = [BM25Base.metadata, VectorBase.metadata, ChunkBase.metadata]

_DEFAULT_URL = "postgresql://raguser:ragpassword@localhost:5432/ragengine"


def get_url() -> str:
    return os.getenv("DATABASE_URL", _DEFAULT_URL)


def run_migrations_offline() -> None:
    context.configure(
        url=get_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    cfg = config.get_section(config.config_ini_section, {})
    cfg["sqlalchemy.url"] = get_url()
    connectable = engine_from_config(
        cfg,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
