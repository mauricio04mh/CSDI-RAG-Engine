from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
from sqlalchemy.engine import Engine

from src.bm25.config.settings import BM25Settings
from src.bm25.pipeline.bm25_retriever import BM25Retriever
from src.database.repositories.bm25_repository import BM25Repository
from src.database.repositories.chunk_repository import ChunkRepository
from src.database.repositories.vector_repository import VectorRepository
from src.database.repositories.web_cache_repository import (
    WebCacheBM25Repository,
    WebCacheChunkRepository,
    WebCacheVectorRepository,
)
from src.document_processing.chunker import DocumentChunk
from src.indexing.builder.index_builder import IndexBuilder
from src.indexing.config.settings import Settings
from src.ingestion.chunk_ingestion_service import ChunkIngestionService
from src.vector_indexing.config.settings import VectorSettings
from src.vector_indexing.pipeline.vector_index_builder import VectorIndexBuilder


def _fake_vector(dim: int = 384) -> np.ndarray:
    v = np.random.randn(dim).astype(np.float32)
    return v / np.linalg.norm(v)


def _settings(tmp_path: Path) -> tuple[Settings, BM25Settings, VectorSettings]:
    env = tmp_path / ".env"
    env.write_text("")
    index_s = Settings(
        index_buffer_size=100,
        index_max_segments_in_memory=3,
        index_flush_interval=60,
        log_level="DEBUG",
        env_path=env,
        project_root=tmp_path,
    )
    bm25_s = BM25Settings(bm25_k1=1.5, bm25_b=0.75, env_path=env, project_root=tmp_path)
    vector_s = VectorSettings(
        embedding_model="stub",
        vector_dimension=384,
        faiss_index_type="HNSW",
        hnsw_m=16,
        hnsw_ef_construction=100,
        hnsw_ef_search=50,
        vector_batch_size=100,
        log_level="DEBUG",
        env_path=env,
        project_root=tmp_path,
    )
    return index_s, bm25_s, vector_s


def test_web_cache_ingestion_does_not_touch_main_corpus(engine: Engine, tmp_path: Path) -> None:
    index_s, bm25_s, vector_s = _settings(tmp_path)
    web_bm25_repo = WebCacheBM25Repository(engine)
    web_vector_repo = WebCacheVectorRepository(engine)
    web_chunk_repo = WebCacheChunkRepository(engine)

    index_builder = IndexBuilder(settings=index_s, engine=engine, bm25_repo=web_bm25_repo)
    web_bm25_retriever = BM25Retriever(settings=bm25_s, engine=engine, bm25_repo=web_bm25_repo)

    with patch("src.vector_indexing.pipeline.vector_index_builder.EmbeddingModel") as mock_embedding_cls:
        mock_model = MagicMock()
        mock_model.encode_one.side_effect = lambda _text: _fake_vector()
        mock_embedding_cls.return_value = mock_model
        vector_builder = VectorIndexBuilder(settings=vector_s, engine=engine, vector_repo=web_vector_repo)

    index_builder.start()
    vector_builder.start()
    try:
        service = ChunkIngestionService(
            chunk_repo=web_chunk_repo,
            index_builder=index_builder,
            vector_index_builder=vector_builder,
            bm25_retriever=web_bm25_retriever,
        )
        chunks = [
            DocumentChunk(
                chunk_id=f"web:duckduckgo:example.com:abc:{i}",
                source_id="web:duckduckgo:example.com",
                url="https://example.com",
                title="Example",
                breadcrumb="web-search",
                text=f"python decorators web cache example {i}",
            )
            for i in range(3)
        ]

        result = service.ingest_chunks(chunks)
        service.finalize(reload_bm25=True)

        assert result.indexed_chunks == 3
        assert ChunkRepository(engine).count_chunks() == 0
        assert len(VectorRepository(engine).load_all_documents()[0]) == 0
        assert BM25Repository(engine).load_full_index()[3] == 0
        assert web_chunk_repo.count_chunks() == 3
        assert len(web_vector_repo.load_all_documents()[0]) == 3
        assert web_bm25_repo.load_full_index()[3] == 3
        assert web_bm25_retriever.search("python decorators", top_k=5)
    finally:
        vector_builder.stop()
        index_builder.stop()
