from __future__ import annotations

from dataclasses import dataclass

from src.evaluation.api.routes import (
    RankingResultResponse,
    _candidate_fetch_k,
    _enrich_ids,
    _matches_source_filter,
)


def test_candidate_fetch_k_returns_top_k_without_source_filter():
    assert _candidate_fetch_k(top_k=10, source_ids=None) == 10


def test_candidate_fetch_k_expands_when_source_filter_is_present():
    assert _candidate_fetch_k(top_k=10, source_ids=["python_docs"]) > 10


def test_candidate_fetch_k_respects_upper_bound():
    assert _candidate_fetch_k(top_k=50, source_ids=["python_docs"]) == 100


def test_source_filter_excludes_unresolved_chunks_when_source_filter_is_present():
    item = RankingResultResponse(chunk_id="unknown", source_id=None)

    assert _matches_source_filter(item, ["python_docs"]) is False


def test_source_filter_keeps_unresolved_chunks_without_source_filter():
    item = RankingResultResponse(chunk_id="unknown", source_id=None)

    assert _matches_source_filter(item, None) is True


def test_enrich_ids_filters_by_source_and_preserves_order():
    chunk_repo = FakeChunkRepository({
        "doc-1": FakeChunk("doc-1", "python_docs"),
        "doc-2": FakeChunk("doc-2", "mdn_js"),
        "doc-3": FakeChunk("doc-3", "python_docs"),
    })

    results = _enrich_ids(
        chunk_ids=["doc-1", "doc-2", "doc-3"],
        chunk_repo=chunk_repo,
        judgments={},
        source_ids=["python_docs"],
    )

    assert [item.chunk_id for item in results] == ["doc-1", "doc-3"]


@dataclass(slots=True)
class FakeChunk:
    chunk_id: str
    source_id: str
    url: str = "https://example.test"
    title: str = "Example"
    breadcrumb: str = ""
    text: str = "Example text"


class FakeChunkRepository:
    def __init__(self, chunks: dict[str, FakeChunk]) -> None:
        self._chunks = chunks

    def get_chunks(self, chunk_ids: list[str]) -> dict[str, FakeChunk]:
        return {
            chunk_id: self._chunks[chunk_id]
            for chunk_id in chunk_ids
            if chunk_id in self._chunks
        }
