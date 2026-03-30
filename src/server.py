from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.bm25.api.bm25_routes import router as bm25_router
from src.config_api.api.routes import load_config_from_disk
from src.config_api.api.routes import router as config_router
from src.file_upload.api.routes import router as upload_router
from src.generation.api.routes import router as rag_router
from src.generation.config.settings import load_settings as load_generation_settings
from src.generation.llm_client import LLMClient
from src.generation.rag_pipeline import RAGPipeline
from src.metrics.api.routes import router as metrics_router
from src.reranker.cross_encoder_reranker import CrossEncoderReranker
from src.bm25.config.settings import load_settings as load_bm25_settings
from src.bm25.pipeline.bm25_retriever import BM25Retriever
from src.database.config import build_engine
from src.database.repositories.chunk_repository import ChunkRepository
from src.hybrid.api.routes import router as search_router
from src.hybrid.pipeline.hybrid_retriever import HybridRetriever
from src.indexing.api.routes import router as indexing_router
from src.indexing.builder.index_builder import IndexBuilder
from src.indexing.config.settings import load_settings as load_indexing_settings
from src.orchestrator.api.routes import router as ingestion_router
from src.orchestrator.ingestion_orchestrator import IngestionOrchestrator
from src.sources_config.source_config_repository import SourceConfigRepository
from src.vector_indexing.api.routes import router as vector_indexing_router
from src.vector_indexing.config.settings import load_settings as load_vector_settings
from src.vector_indexing.pipeline.vector_index_builder import VectorIndexBuilder
from src.vector_retrieval.api.vector_search_routes import router as vector_search_router
from src.vector_retrieval.pipeline.vector_retriever import VectorRetriever


def configure_logging(log_level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )


def create_app() -> FastAPI:
    indexing_settings = load_indexing_settings()
    bm25_settings = load_bm25_settings()
    vector_settings = load_vector_settings()
    generation_settings = load_generation_settings()
    configure_logging(indexing_settings.log_level)

    engine = build_engine(os.getenv("DATABASE_URL"))

    index_builder = IndexBuilder(settings=indexing_settings, engine=engine)
    bm25_retriever = BM25Retriever(settings=bm25_settings, engine=engine)
    vector_index_builder = VectorIndexBuilder(settings=vector_settings, engine=engine)
    vector_retriever = VectorRetriever(
        embedding_model=vector_index_builder.embedding_model,
        faiss_index=vector_index_builder.faiss_index,
        vector_store=vector_index_builder.vector_store,
        lock=vector_index_builder._lock,
    )
    hybrid_retriever = HybridRetriever(
        bm25_retriever=bm25_retriever,
        vector_retriever=vector_retriever,
        bm25_weight=float(os.getenv("HYBRID_BM25_WEIGHT", "0.3")),
        vector_weight=float(os.getenv("HYBRID_VECTOR_WEIGHT", "0.7")),
    )
    source_repo = SourceConfigRepository()
    chunk_repo = ChunkRepository(engine)
    ingestion_orchestrator = IngestionOrchestrator(
        source_repo=source_repo,
        index_builder=index_builder,
        vector_index_builder=vector_index_builder,
        chunk_repo=chunk_repo,
    )
    llm_client = LLMClient(
        base_url=generation_settings.base_url,
        api_key=generation_settings.api_key,
        model=generation_settings.model,
        max_tokens=generation_settings.max_tokens,
        temperature=generation_settings.temperature,
        timeout=generation_settings.timeout,
    )
    reranker = (
        CrossEncoderReranker(model_name=generation_settings.reranker_model)
        if generation_settings.reranker_enabled
        else None
    )
    rag_pipeline = RAGPipeline(
        retriever=hybrid_retriever,
        chunk_repo=chunk_repo,
        llm_client=llm_client,
        settings=generation_settings,
        reranker=reranker,
    )

    # Apply any persisted config overrides on top of env-var defaults
    _persisted_config = load_config_from_disk()
    if "bm25_weight" in _persisted_config and "vector_weight" in _persisted_config:
        hybrid_retriever.update_weights(
            _persisted_config["bm25_weight"],
            _persisted_config["vector_weight"],
        )
    if "model" in _persisted_config or "temperature" in _persisted_config:
        llm_client.update_settings(
            model=_persisted_config.get("model", generation_settings.model),
            temperature=_persisted_config.get("temperature", generation_settings.temperature),
        )
    if "reranker_enabled" in _persisted_config:
        persisted_reranker_on = _persisted_config["reranker_enabled"]
        if persisted_reranker_on and reranker is None:
            reranker = CrossEncoderReranker(model_name=generation_settings.reranker_model)
            rag_pipeline._reranker = reranker
        elif not persisted_reranker_on and reranker is not None:
            rag_pipeline._reranker = None

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.db_engine = engine
        app.state.indexing_settings = indexing_settings
        app.state.bm25_settings = bm25_settings
        app.state.vector_settings = vector_settings
        app.state.index_builder = index_builder
        app.state.bm25_retriever = bm25_retriever
        app.state.vector_index_builder = vector_index_builder
        app.state.vector_retriever = vector_retriever
        app.state.hybrid_retriever = hybrid_retriever
        app.state.source_repo = source_repo
        app.state.chunk_repo = chunk_repo
        app.state.ingestion_orchestrator = ingestion_orchestrator
        app.state.rag_pipeline = rag_pipeline
        app.state.llm_client = llm_client
        app.state.reranker_model = generation_settings.reranker_model
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
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost:3000"],
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.include_router(search_router)
    app.include_router(bm25_router)
    app.include_router(indexing_router)
    app.include_router(vector_indexing_router)
    app.include_router(vector_search_router)
    app.include_router(ingestion_router)
    app.include_router(rag_router)
    app.include_router(metrics_router)
    app.include_router(config_router)
    app.include_router(upload_router)

    @app.get("/health", tags=["system"])
    async def healthcheck() -> dict[str, str]:
        return {"status": "ok"}

    return app


app = create_app()
