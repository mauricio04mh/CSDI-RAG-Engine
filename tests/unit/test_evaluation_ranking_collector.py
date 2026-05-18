from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from src.evaluation import ranking_collector
from src.evaluation.ranking_collector import collect_rankings


def test_collect_rankings_writes_rankings_for_all_strategies(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    client = _install_fake_client(monkeypatch, _successful_response)
    queries_path = _write_json(
        tmp_path,
        "queries.json",
        [{"id": "q1", "query": "decorators", "source_ids": ["python_docs"]}],
    )
    output_path = tmp_path / "generated" / "rankings.json"

    rankings = collect_rankings(queries_path, output_path, top_k=10)

    assert rankings == {
        "bm25": {"q1": ["bm25-doc"]},
        "vector": {"q1": ["vector-doc"]},
        "hybrid": {"q1": ["hybrid-chunk"]},
    }
    assert json.loads(output_path.read_text(encoding="utf-8")) == rankings
    assert {call["json"]["source_ids"][0] for call in client.calls} == {"python_docs"}


def test_collect_rankings_extracts_doc_id_for_bm25_and_vector(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    _install_fake_client(monkeypatch, _successful_response)
    queries_path = _write_json(tmp_path, "queries.json", [{"id": "q1", "query": "x"}])

    rankings = collect_rankings(queries_path, tmp_path / "rankings.json")

    assert rankings["bm25"]["q1"] == ["bm25-doc"]
    assert rankings["vector"]["q1"] == ["vector-doc"]


def test_collect_rankings_extracts_chunk_id_for_hybrid(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    _install_fake_client(monkeypatch, _successful_response)
    queries_path = _write_json(tmp_path, "queries.json", [{"id": "q1", "query": "x"}])

    rankings = collect_rankings(queries_path, tmp_path / "rankings.json")

    assert rankings["hybrid"]["q1"] == ["hybrid-chunk"]


def test_collect_rankings_omits_source_ids_when_none(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    client = _install_fake_client(monkeypatch, _successful_response)
    queries_path = _write_json(tmp_path, "queries.json", [{"id": "q1", "query": "x"}])

    collect_rankings(queries_path, tmp_path / "rankings.json")

    assert all("source_ids" not in call["json"] for call in client.calls)


def test_collect_rankings_raises_runtime_error_on_non_200_response(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    def responder(url: str) -> FakeResponse:
        return FakeResponse(status_code=500, data={"detail": "error"}, text="error")

    _install_fake_client(monkeypatch, responder)
    queries_path = _write_json(tmp_path, "queries.json", [{"id": "q1", "query": "x"}])

    with pytest.raises(RuntimeError, match="request failed.*status 500"):
        collect_rankings(queries_path, tmp_path / "rankings.json")


def test_collect_rankings_raises_runtime_error_on_malformed_response(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    def responder(url: str) -> FakeResponse:
        return FakeResponse(status_code=200, data={"items": []})

    _install_fake_client(monkeypatch, responder)
    queries_path = _write_json(tmp_path, "queries.json", [{"id": "q1", "query": "x"}])

    with pytest.raises(RuntimeError, match="results list"):
        collect_rankings(queries_path, tmp_path / "rankings.json")


class FakeResponse:
    def __init__(
        self,
        status_code: int,
        data: object,
        text: str = "",
    ) -> None:
        self.status_code = status_code
        self._data = data
        self.text = text

    def json(self) -> object:
        return self._data


class FakeClient:
    def __init__(self, responder: Any, timeout: int) -> None:
        self.responder = responder
        self.timeout = timeout
        self.calls: list[dict[str, Any]] = []

    def __enter__(self) -> FakeClient:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def post(self, url: str, json: dict[str, Any]) -> FakeResponse:
        self.calls.append({"url": url, "json": json})
        return self.responder(url)


def _install_fake_client(
    monkeypatch: pytest.MonkeyPatch,
    responder: Any,
) -> FakeClient:
    client = FakeClient(responder=responder, timeout=60)

    def fake_client_factory(timeout: int) -> FakeClient:
        client.timeout = timeout
        return client

    monkeypatch.setattr(ranking_collector.httpx, "Client", fake_client_factory)
    return client


def _successful_response(url: str) -> FakeResponse:
    if url.endswith("/api/v1/search/bm25"):
        return FakeResponse(status_code=200, data={"results": [{"doc_id": "bm25-doc"}]})
    if url.endswith("/api/v1/vector/search"):
        return FakeResponse(status_code=200, data={"results": [{"doc_id": "vector-doc"}]})
    if url.endswith("/api/v1/search"):
        return FakeResponse(
            status_code=200,
            data={"results": [{"chunk_id": "hybrid-chunk"}]},
        )

    raise AssertionError(f"Unexpected URL: {url}")


def _write_json(tmp_path: Path, filename: str, data: object) -> Path:
    path = tmp_path / filename
    path.write_text(json.dumps(data), encoding="utf-8")
    return path
