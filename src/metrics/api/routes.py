from __future__ import annotations

import logging

from fastapi import APIRouter, Request
from pydantic import BaseModel

from src.bm25.pipeline.bm25_retriever import BM25Retriever
from src.database.repositories.chunk_repository import ChunkRepository
from src.vector_indexing.pipeline.vector_index_builder import VectorIndexBuilder

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1", tags=["metrics"])


class MetricsResponse(BaseModel):
    total_chunks: int
    faiss_vectors: int
    bm25_documents: int
    bm25_terms: int


@router.get("/metrics", response_model=MetricsResponse)
def get_metrics(request: Request) -> MetricsResponse:
    """Return live statistics about the indexed corpus."""
    chunk_repo: ChunkRepository = request.app.state.chunk_repo
    vector_index_builder: VectorIndexBuilder = request.app.state.vector_index_builder
    bm25_retriever: BM25Retriever = request.app.state.bm25_retriever

    return MetricsResponse(
        total_chunks=chunk_repo.count_chunks(),
        faiss_vectors=len(vector_index_builder.vector_store),
        bm25_documents=bm25_retriever._index.total_documents,
        bm25_terms=len(bm25_retriever._index.dictionary),
    )
