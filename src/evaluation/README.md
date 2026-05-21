# Evaluation Module

## Purpose

This module implements the optional Information Retrieval evaluation module for the SRI final project.

## Base Dataset

The repository includes a base evaluation dataset with:

- 5 evaluation queries
- Python Docs as source
- persisted rankings for BM25, Vector, and Hybrid
- complete qrels with `missing_pairs = 0`

## Evaluation Workflow

1. Queries are stored in `src/evaluation/datasets/queries.json`.
2. Rankings are stored in `src/evaluation/datasets/rankings.generated.json`.
3. Relevance judgments are stored in `src/evaluation/datasets/qrels.json`.
4. The audit validates that all ranked query-chunk pairs have judgments.
5. The offline runner calculates metrics and writes `src/evaluation/results/evaluation_report.json`.

## Metrics

- `Precision@K`: measures how many of the top K retrieved results are relevant.
- `Recall@K`: measures how many relevant results were found within the top K.
- `F1@K`: harmonic balance between precision and recall at K.
- `MRR`: measures how early the first relevant result appears.
- `NDCG@K`: evaluates ranking quality using graded relevance values.

## API Integration

The frontend can operate the workflow through:

- `GET /api/v1/evaluation/queries`
- `POST /api/v1/evaluation/queries`
- `POST /api/v1/evaluation/queries/{query_id}/rankings`
- `PUT /api/v1/evaluation/queries/{query_id}/judgments/{chunk_id}`
- `POST /api/v1/evaluation/run`
- `GET /api/v1/evaluation/report`
- `GET /api/v1/evaluation/summary`

## Useful Commands

Refresh rankings:

```bash
python3 -m src.evaluation.base_dataset_refresh \
  --base-url http://localhost:8888 \
  --top-k 10
```

Audit qrels:

```bash
python3 -m src.evaluation.audit_qrels \
  --rankings src/evaluation/datasets/rankings.generated.json \
  --qrels src/evaluation/datasets/qrels.json \
  --queries src/evaluation/datasets/queries.json \
  --output src/evaluation/results/qrels_audit_report.json
```

Generate final report:

```bash
python3 -m src.evaluation.offline_runner \
  --rankings src/evaluation/datasets/rankings.generated.json \
  --qrels src/evaluation/datasets/qrels.json \
  --output src/evaluation/results/evaluation_report.json \
  --k 10
```

## Final Dataset Status

The current base dataset is complete because `src/evaluation/results/qrels_audit_report.json` reports `missing_pairs = 0`.
