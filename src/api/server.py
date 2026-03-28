from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI

from src.bm25.api.bm25_routes import router as bm25_router
from src.bm25.config.settings import load_settings as load_bm25_settings
from src.bm25.pipeline.bm25_retriever import BM25Retriever
from src.api.indexing_routes import router as indexing_router
from src.api.search_route import router as search_router
from src.api.vector_indexing_routes import router as vector_indexing_router
from src.database.config import build_engine
from src.indexing.builder.index_builder import IndexBuilder
from src.indexing.config.settings import load_settings as load_indexing_settings
from src.vector_indexing.config.settings import load_settings as load_vector_settings
from src.vector_indexing.pipeline.vector_index_builder import VectorIndexBuilder


def configure_logging(log_level: str) -> None:
    """Configure application logging once during startup."""
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )


def create_app() -> FastAPI:
    """Create the shared API application and wire domain entrypoints."""
    indexing_settings = load_indexing_settings()
    bm25_settings = load_bm25_settings()
    vector_settings = load_vector_settings()
    configure_logging(indexing_settings.log_level)

    database_url = os.getenv("DATABASE_URL")
    engine = build_engine(database_url)

    index_builder = IndexBuilder(settings=indexing_settings, engine=engine)
    bm25_retriever = BM25Retriever(settings=bm25_settings, engine=engine)
    vector_index_builder = VectorIndexBuilder(settings=vector_settings, engine=engine)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.db_engine = engine
        app.state.indexing_settings = indexing_settings
        app.state.bm25_settings = bm25_settings
        app.state.vector_settings = vector_settings
        app.state.index_builder = index_builder
        app.state.bm25_retriever = bm25_retriever
        app.state.vector_index_builder = vector_index_builder
        index_builder.start()
        bm25_retriever.start()
        vector_index_builder.start()
        try:
            yield
        finally:
            vector_index_builder.stop()
            index_builder.stop()
            engine.dispose()

    app = FastAPI(
        title="CSDI RAG Engine",
        version="0.1.0",
        description="Technical documentation search engine API with hybrid retrieval and RAG.",
        lifespan=lifespan,
    )
    app.include_router(search_router)
    app.include_router(bm25_router)
    app.include_router(indexing_router)
    app.include_router(vector_indexing_router)

    @app.get("/health", tags=["system"])
    async def healthcheck() -> dict[str, str]:
        return {"status": "ok"}

    return app


app = create_app()
