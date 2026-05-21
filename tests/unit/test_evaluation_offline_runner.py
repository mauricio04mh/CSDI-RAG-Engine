from __future__ import annotations

import json
from pathlib import Path

from src.evaluation.offline_runner import run_offline_evaluation


def test_run_offline_evaluation_loads_inputs_and_writes_report(tmp_path: Path):
    rankings_path = _write_json(
        tmp_path,
        "rankings.json",
        {
            "bm25": {"q1": ["doc-1", "doc-2"]},
            "vector": {"q1": ["doc-2", "doc-1"]},
        },
    )
    qrels_path = _write_json(tmp_path, "qrels.json", {"q1": {"doc-1": 2}})
    output_path = tmp_path / "results" / "evaluation_report.json"

    report = run_offline_evaluation(rankings_path, qrels_path, output_path, k=2)

    assert output_path.exists()
    assert report["k"] == 2
    assert "strategies" in report
    assert set(report["strategies"]) == {"bm25", "vector"}


def test_run_offline_evaluation_creates_output_directory(tmp_path: Path):
    rankings_path = _write_json(tmp_path, "rankings.json", {"bm25": {"q1": ["doc-1"]}})
    qrels_path = _write_json(tmp_path, "qrels.json", {"q1": {"doc-1": 2}})
    output_path = tmp_path / "missing" / "nested" / "report.json"

    run_offline_evaluation(rankings_path, qrels_path, output_path, k=1)

    assert output_path.parent.exists()
    assert output_path.exists()


def test_run_offline_evaluation_written_json_matches_returned_report(tmp_path: Path):
    rankings_path = _write_json(
        tmp_path,
        "rankings.json",
        {"hybrid": {"q1": ["doc-1", "doc-2"]}},
    )
    qrels_path = _write_json(tmp_path, "qrels.json", {"q1": {"doc-1": 2}})
    output_path = tmp_path / "report.json"

    report = run_offline_evaluation(rankings_path, qrels_path, output_path, k=2)
    written_report = json.loads(output_path.read_text(encoding="utf-8"))

    assert written_report == report


def _write_json(tmp_path: Path, filename: str, data: object) -> Path:
    path = tmp_path / filename
    path.write_text(json.dumps(data), encoding="utf-8")
    return path
