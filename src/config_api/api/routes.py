from __future__ import annotations

import json
import logging
import os
from dataclasses import replace as dc_replace

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field, model_validator

from src.generation.llm_client import LLMClient
from src.generation.rag_pipeline import RAGPipeline
from src.hybrid.pipeline.hybrid_retriever import HybridRetriever
from src.reranker.cross_encoder_reranker import CrossEncoderReranker
from src.web_search.insufficiency_detector.detector import InsufficiencyDetector

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1", tags=["config"])

_DEFAULT_CONFIG_PATH = "pipeline_config.json"


def _config_path() -> str:
    return os.getenv("PIPELINE_CONFIG_PATH", _DEFAULT_CONFIG_PATH)


def load_config_from_disk() -> dict:
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


def _provider_name(base_url: str) -> str:
    url = (base_url or "").lower()
    if "groq.com" in url:
        return "groq"
    if "localhost:11434" in url or "/api/generate" in url:
        return "ollama"
    if "api.openai.com" in url:
        return "openai"
    return "custom"


def _available_models(provider: str) -> list[str]:
    if provider == "groq":
        return [
            "llama-3.3-70b-versatile",
            "llama-3.1-70b-versatile",
            "mixtral-8x7b-32768",
        ]
    if provider == "openai":
        return [
            "gpt-4o",
            "gpt-4.1",
            "gpt-4-turbo",
            "gpt-4o-mini",
        ]
    if provider == "ollama":
        return [
            "llama3.1",
            "llama3.2",
            "mistral",
            "qwen2.5",
        ]
    return []


class InsuffConfig(BaseModel):
    confidence_threshold: float = Field(default=0.65, ge=0.0, le=1.0)
    min_results: int = Field(default=5, ge=1)
    expected_results: int = Field(default=10, ge=1)
    min_top_score: float = Field(default=0.35, ge=0.0, le=1.0)
    min_relevant_results: int = Field(default=2, ge=1)
    min_coverage_score: float = Field(default=0.20, ge=0.0, le=1.0)
    min_answerability_score: float = Field(default=0.40, ge=0.0, le=1.0)
    min_source_diversity: float = Field(default=0.30, ge=0.0, le=1.0)
    coverage_top_n: int = Field(default=5, ge=1)
    w_top: float = Field(default=0.10, ge=0.0, le=1.0)
    w_quantity: float = Field(default=0.15, ge=0.0, le=1.0)
    w_coverage: float = Field(default=0.35, ge=0.0, le=1.0)
    w_diversity: float = Field(default=0.15, ge=0.0, le=1.0)
    w_answerability: float = Field(default=0.25, ge=0.0, le=1.0)

    @model_validator(mode="after")
    def weights_must_sum_to_one(self) -> "InsuffConfig":
        total = self.w_top + self.w_quantity + self.w_coverage + self.w_diversity + self.w_answerability
        if abs(total - 1.0) > 1e-4:
            raise ValueError(f"Insufficiency weights must sum to 1.0 (got {total:.4f})")
        return self


class PipelineConfig(BaseModel):
    bm25_weight: float = Field(..., ge=0.0, le=1.0)
    vector_weight: float = Field(..., ge=0.0, le=1.0)
    temperature: float = Field(..., ge=0.0, le=2.0)
    model: str = Field(..., min_length=1)
    reranker_enabled: bool
    reranker_candidate_k: int = Field(default=30, ge=1)
    context_chunks: int = Field(default=15, ge=1, le=50)
    max_tokens: int = Field(default=1024, ge=64, le=8192)
    hyde_enabled: bool = False
    llm_base_url: str = Field(..., min_length=1)
    llm_api_key: str = Field(default="")
    provider: str = Field(default="custom")
    available_models: list[str] = Field(default_factory=list)
    insuff: InsuffConfig = Field(default_factory=InsuffConfig)

    @model_validator(mode="after")
    def weights_must_sum_to_one(self) -> "PipelineConfig":
        if abs(self.bm25_weight + self.vector_weight - 1.0) > 1e-6:
            raise ValueError("bm25_weight + vector_weight must equal 1.0")
        return self


class PipelineConfigUpdate(BaseModel):
    bm25_weight: float = Field(..., ge=0.0, le=1.0)
    vector_weight: float = Field(..., ge=0.0, le=1.0)
    temperature: float = Field(..., ge=0.0, le=2.0)
    model: str = Field(..., min_length=1)
    reranker_enabled: bool
    reranker_candidate_k: int = Field(default=30, ge=1)
    context_chunks: int = Field(default=15, ge=1, le=50)
    max_tokens: int = Field(default=1024, ge=64, le=8192)
    hyde_enabled: bool = False
    llm_base_url: str = Field(..., min_length=1)
    llm_api_key: str = Field(default="")
    insuff: InsuffConfig = Field(default_factory=InsuffConfig)

    @model_validator(mode="after")
    def weights_must_sum_to_one(self) -> "PipelineConfigUpdate":
        if abs(self.bm25_weight + self.vector_weight - 1.0) > 1e-6:
            raise ValueError("bm25_weight + vector_weight must equal 1.0")
        return self


@router.get("/config", response_model=PipelineConfig)
def get_config(request: Request) -> PipelineConfig:
    hybrid_retriever: HybridRetriever = request.app.state.hybrid_retriever
    llm_client: LLMClient = request.app.state.llm_client
    rag_pipeline: RAGPipeline = request.app.state.rag_pipeline

    base_url = llm_client._base_url
    api_key = llm_client._headers.get("Authorization", "").replace("Bearer ", "")
    provider = _provider_name(base_url)
    s = rag_pipeline._settings

    insuff_cfg = InsuffConfig()
    detector: InsufficiencyDetector | None = rag_pipeline._insufficiency_detector
    if detector is not None:
        ds = detector.settings
        insuff_cfg = InsuffConfig(
            confidence_threshold=ds.confidence_threshold,
            min_results=ds.min_results,
            expected_results=ds.expected_results,
            min_top_score=ds.min_top_score,
            min_relevant_results=ds.min_relevant_results,
            min_coverage_score=ds.min_coverage_score,
            min_answerability_score=ds.min_answerability_score,
            min_source_diversity=ds.min_source_diversity,
            coverage_top_n=ds.coverage_top_n,
            w_top=ds.w_top,
            w_quantity=ds.w_quantity,
            w_coverage=ds.w_coverage,
            w_diversity=ds.w_diversity,
            w_answerability=ds.w_answerability,
        )

    return PipelineConfig(
        bm25_weight=hybrid_retriever._bm25_weight,
        vector_weight=hybrid_retriever._vector_weight,
        temperature=llm_client._temperature,
        model=llm_client._model,
        reranker_enabled=rag_pipeline._reranker is not None,
        reranker_candidate_k=s.reranker_candidate_k,
        context_chunks=s.context_chunks,
        max_tokens=llm_client._max_tokens,
        hyde_enabled=s.hyde_enabled,
        llm_base_url=base_url,
        llm_api_key=api_key,
        provider=provider,
        available_models=_available_models(provider),
        insuff=insuff_cfg,
    )


@router.post("/config", response_model=PipelineConfig)
def update_config(payload: PipelineConfigUpdate, request: Request) -> PipelineConfig:
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
            max_tokens=payload.max_tokens,
        )
        llm_client.update_connection(
            base_url=payload.llm_base_url,
            api_key=payload.llm_api_key,
        )

        currently_enabled = rag_pipeline._reranker is not None
        if payload.reranker_enabled and not currently_enabled:
            logger.info("reranker_enabling model=%s", reranker_model)
            rag_pipeline._reranker = CrossEncoderReranker(model_name=reranker_model)
        elif not payload.reranker_enabled and currently_enabled:
            logger.info("reranker_disabling")
            rag_pipeline._reranker = None

        rag_pipeline._settings = dc_replace(
            rag_pipeline._settings,
            context_chunks=payload.context_chunks,
            reranker_candidate_k=payload.reranker_candidate_k,
            hyde_enabled=payload.hyde_enabled,
        )

        detector: InsufficiencyDetector | None = rag_pipeline._insufficiency_detector
        if detector is not None:
            ic = payload.insuff
            detector.settings = dc_replace(
                detector.settings,
                confidence_threshold=ic.confidence_threshold,
                min_results=ic.min_results,
                expected_results=ic.expected_results,
                min_top_score=ic.min_top_score,
                min_relevant_results=ic.min_relevant_results,
                min_coverage_score=ic.min_coverage_score,
                min_answerability_score=ic.min_answerability_score,
                min_source_diversity=ic.min_source_diversity,
                coverage_top_n=ic.coverage_top_n,
                w_top=ic.w_top,
                w_quantity=ic.w_quantity,
                w_coverage=ic.w_coverage,
                w_diversity=ic.w_diversity,
                w_answerability=ic.w_answerability,
            )
    except Exception as exc:
        logger.exception("config_update_failed")
        raise HTTPException(status_code=500, detail=f"Config update failed: {exc}") from exc

    _persist_config(payload.model_dump())

    provider = _provider_name(payload.llm_base_url)
    s = rag_pipeline._settings
    return PipelineConfig(
        bm25_weight=payload.bm25_weight,
        vector_weight=payload.vector_weight,
        temperature=payload.temperature,
        model=payload.model,
        reranker_enabled=payload.reranker_enabled,
        reranker_candidate_k=s.reranker_candidate_k,
        context_chunks=s.context_chunks,
        max_tokens=llm_client._max_tokens,
        hyde_enabled=s.hyde_enabled,
        llm_base_url=payload.llm_base_url,
        llm_api_key=payload.llm_api_key,
        provider=provider,
        available_models=_available_models(provider),
        insuff=payload.insuff,
    )
