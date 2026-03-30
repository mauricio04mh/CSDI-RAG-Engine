from __future__ import annotations

import json
import logging
import os

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field, model_validator

from src.generation.llm_client import LLMClient
from src.generation.rag_pipeline import RAGPipeline
from src.hybrid.pipeline.hybrid_retriever import HybridRetriever
from src.reranker.cross_encoder_reranker import CrossEncoderReranker

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1", tags=["config"])

_DEFAULT_CONFIG_PATH = "pipeline_config.json"


def _config_path() -> str:
    return os.getenv("PIPELINE_CONFIG_PATH", _DEFAULT_CONFIG_PATH)


def load_config_from_disk() -> dict:
    """Load persisted config from disk, returning an empty dict if not found."""
    path = _config_path()
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except Exception as exc:
        logger.warning("config_load_failed path=%s reason=%s", path, exc)
        return {}


def _persist_config(data: dict) -> None:
    path = _config_path()
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        logger.info("config_persisted path=%s", path)
    except Exception as exc:
        logger.error("config_persist_failed path=%s reason=%s", path, exc)


class PipelineConfig(BaseModel):
    bm25_weight: float = Field(..., ge=0.0, le=1.0)
    vector_weight: float = Field(..., ge=0.0, le=1.0)
    temperature: float = Field(..., ge=0.0, le=2.0)
    model: str = Field(..., min_length=1)
    reranker_enabled: bool

    @model_validator(mode="after")
    def weights_must_sum_to_one(self) -> "PipelineConfig":
        if abs(self.bm25_weight + self.vector_weight - 1.0) > 1e-6:
            raise ValueError("bm25_weight + vector_weight must equal 1.0")
        return self


@router.get("/config", response_model=PipelineConfig)
def get_config(request: Request) -> PipelineConfig:
    """Return the current live pipeline configuration."""
    hybrid_retriever: HybridRetriever = request.app.state.hybrid_retriever
    llm_client: LLMClient = request.app.state.llm_client
    rag_pipeline: RAGPipeline = request.app.state.rag_pipeline

    return PipelineConfig(
        bm25_weight=hybrid_retriever._bm25_weight,
        vector_weight=hybrid_retriever._vector_weight,
        temperature=llm_client._temperature,
        model=llm_client._model,
        reranker_enabled=rag_pipeline._reranker is not None,
    )


@router.post("/config", response_model=PipelineConfig)
def update_config(payload: PipelineConfig, request: Request) -> PipelineConfig:
    """Update pipeline configuration live and persist to disk."""
    hybrid_retriever: HybridRetriever = request.app.state.hybrid_retriever
    llm_client: LLMClient = request.app.state.llm_client
    rag_pipeline: RAGPipeline = request.app.state.rag_pipeline
    reranker_model: str = request.app.state.reranker_model

    try:
        hybrid_retriever.update_weights(
            bm25_weight=payload.bm25_weight,
            vector_weight=payload.vector_weight,
        )
        llm_client.update_settings(
            model=payload.model,
            temperature=payload.temperature,
        )

        currently_enabled = rag_pipeline._reranker is not None
        if payload.reranker_enabled and not currently_enabled:
            logger.info("reranker_enabling model=%s", reranker_model)
            rag_pipeline._reranker = CrossEncoderReranker(model_name=reranker_model)
        elif not payload.reranker_enabled and currently_enabled:
            logger.info("reranker_disabling")
            rag_pipeline._reranker = None

    except Exception as exc:
        logger.exception("config_update_failed")
        raise HTTPException(status_code=500, detail=f"Config update failed: {exc}") from exc

    _persist_config(payload.model_dump())
    return payload
