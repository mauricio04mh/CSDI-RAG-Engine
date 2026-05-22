# Query Feedback Module

## Purpose

The Query Feedback module adds query expansion and explicit relevance feedback capabilities to the CSDI RAG Engine.

Its purpose is to improve retrieval workflows by:

- expanding underspecified queries using pseudo-relevance feedback
- collecting explicit user judgments for retrieved chunks
- optionally reranking retrieved results using stored feedback

## Core Capabilities

### Pseudo-Relevance Feedback Query Expansion

The module can expand a query using the top chunks retrieved for the same query. Expansion is deterministic and corpus-based.

### Expanded Hybrid Search

The module can execute hybrid retrieval where:

- the BM25 branch uses the original query
- the vector branch uses the expanded query
- fusion is still performed by the existing hybrid retriever

### Explicit Relevance Feedback Persistence

The module accepts explicit feedback for retrieved chunks and stores it in PostgreSQL through SQLAlchemy.

### Feedback-Based Reranking

The module can apply exact and semantic feedback after retrieval and rerank results using adjusted scores.

### Feedback Summary and Traceability

The module exposes aggregate feedback statistics and query-based feedback lookup endpoints for debugging, inspection and future UI integration.

## Architecture Overview

The module reuses existing backend components instead of introducing a parallel retrieval stack.

- `HybridRetriever` executes hybrid retrieval
- the BM25 branch is reused through `HybridRetriever`
- vector search is reused through `HybridRetriever.vector_query`
- `ChunkRepository` resolves chunk metadata for API responses
- PostgreSQL persistence is handled through SQLAlchemy models and repositories
- when available, the existing embedding model is reused for semantic feedback matching

The module does not modify BM25, vector retrieval, hybrid fusion or chunk repository internals.

## Query Expansion Behavior

Query expansion is based on top retrieved chunks and does not rely on a manual dictionary or an LLM.

Behavior:

- the module retrieves top feedback chunks for the current query
- candidate terms are extracted from chunk `title`, `breadcrumb` and `text`
- title and breadcrumb terms receive higher weight than text-only terms
- terms already represented in the original query are filtered out
- duplicate surface forms and duplicate normalized BM25 signatures are filtered out
- only readable lowercase surface terms are returned
- the selection process is deterministic

## Expanded Search Behavior

When expansion is enabled:

- BM25 uses the original query
- vector search uses the expanded query
- `HybridRetriever` fuses both branches

When expansion is disabled:

- the module performs normal hybrid retrieval with the original query

The existing retriever internals are not modified.

## Feedback Persistence

Explicit feedback is stored in PostgreSQL.

Table:

- `query_feedback`

Feedback scale:

- `0` = not relevant
- `1` = marginal
- `2` = relevant
- `3` = highly relevant

Persistence behavior:

- feedback is updated for the same `normalized_query + chunk_id + session_id`
- `session_id` is optional and handled explicitly in repository lookup logic
- feedback is not stored in JSON

## Feedback-Based Reranking

`/search-with-feedback` applies stored feedback after retrieval.

Matching behavior:

- exact feedback is matched by normalized query
- semantic feedback can reuse feedback from very similar previous queries
- the default semantic similarity threshold is `0.92`
- exact feedback has priority over semantic feedback for the same `chunk_id`

Score formula:

```text
adjusted_score = original_score * (1 + multiplier)
```

Multipliers:

- relevance `3` -> `+0.50`
- relevance `2` -> `+0.25`
- relevance `1` -> `+0.05`
- relevance `0` -> `-0.40`

If no feedback applies to a result, `adjusted_score` remains equal to `original_score`.

## Endpoints

### `GET /api/v1/query-feedback/health`

Purpose:

- lightweight module health check

Example response:

```json
{
  "status": "ok",
  "module": "query-feedback"
}
```

### `POST /api/v1/query-feedback/expand`

Purpose:

- expand a query using pseudo-relevance feedback without executing final search

Example request:

```json
{
  "query": "How do decorators work?",
  "top_k_feedback": 5,
  "max_expansion_terms": 6,
  "source_ids": ["python_docs"]
}
```

Example response shape:

```json
{
  "original_query": "How do decorators work?",
  "expanded_query": "How do decorators work? closures wrappers descriptors",
  "expansion_terms": ["closures", "wrappers", "descriptors"],
  "method": "pseudo_relevance_feedback",
  "feedback_documents_used": 5
}
```

### `POST /api/v1/query-feedback/search`

Purpose:

- execute hybrid retrieval with optional query expansion

Example request:

```json
{
  "query": "How do decorators work?",
  "top_k": 10,
  "source_ids": ["python_docs"],
  "expansion_enabled": true,
  "top_k_feedback": 5,
  "max_expansion_terms": 6
}
```

Example response shape:

```json
{
  "original_query": "How do decorators work?",
  "expanded_query": "How do decorators work? closures wrappers",
  "expansion_terms": ["closures", "wrappers"],
  "method": "pseudo_relevance_feedback",
  "strategy": "hybrid_expanded_vector",
  "expansion_enabled": true,
  "feedback_documents_used": 5,
  "results": [
    {
      "chunk_id": "python_docs:chunk-1",
      "score": 0.91,
      "source_id": "python_docs",
      "url": "https://docs.python.org/example",
      "title": "Decorators",
      "breadcrumb": "Functions",
      "text": "A decorator wraps another callable."
    }
  ]
}
```

### `POST /api/v1/query-feedback/feedback`

Purpose:

- create or update explicit relevance feedback for a chunk

Example request:

```json
{
  "query": "How do decorators work?",
  "chunk_id": "python_docs:chunk-1",
  "relevance": 3,
  "source_id": "python_docs",
  "notes": "Directly explains wrappers.",
  "session_id": "session-a"
}
```

Example response shape:

```json
{
  "id": 1,
  "query": "How do decorators work?",
  "normalized_query": "how do decorators work?",
  "chunk_id": "python_docs:chunk-1",
  "source_id": "python_docs",
  "relevance": 3,
  "notes": "Directly explains wrappers.",
  "session_id": "session-a",
  "created_at": "2026-05-22T10:00:00+00:00",
  "updated_at": "2026-05-22T10:00:00+00:00",
  "stored": true
}
```

### `POST /api/v1/query-feedback/search-with-feedback`

Purpose:

- execute retrieval and then rerank results using exact and optional semantic feedback

Example request:

```json
{
  "query": "Explain Python decorators",
  "top_k": 10,
  "source_ids": ["python_docs"],
  "expansion_enabled": true,
  "top_k_feedback": 5,
  "max_expansion_terms": 6,
  "feedback_enabled": true,
  "semantic_feedback_enabled": true,
  "semantic_similarity_threshold": 0.92
}
```

Example response shape:

```json
{
  "original_query": "Explain Python decorators",
  "expanded_query": "Explain Python decorators closures wrappers",
  "expansion_terms": ["closures", "wrappers"],
  "method": "pseudo_relevance_feedback",
  "strategy": "hybrid_expanded_vector",
  "expansion_enabled": true,
  "feedback_enabled": true,
  "semantic_feedback_enabled": true,
  "semantic_similarity_threshold": 0.92,
  "feedback_applied": true,
  "feedback_items_used": 2,
  "matched_feedback_queries": [
    {
      "query": "How do decorators work?",
      "normalized_query": "how do decorators work?",
      "similarity": 0.94
    }
  ],
  "feedback_documents_used": 5,
  "results": [
    {
      "chunk_id": "python_docs:chunk-1",
      "original_score": 0.91,
      "adjusted_score": 1.365,
      "feedback_boost": 0.455,
      "feedback_applied": true,
      "feedback_relevance": 3,
      "feedback_source_query": "How do decorators work?",
      "feedback_query_similarity": 0.94,
      "feedback_match_type": "semantic",
      "source_id": "python_docs",
      "url": "https://docs.python.org/example",
      "title": "Decorators",
      "breadcrumb": "Functions",
      "text": "A decorator wraps another callable."
    }
  ]
}
```

### `GET /api/v1/query-feedback/feedback/summary`

Purpose:

- return aggregate feedback statistics

Example response:

```json
{
  "total_feedback_items": 12,
  "queries_with_feedback": 4,
  "positive_feedback": 7,
  "negative_feedback": 2,
  "marginal_feedback": 3,
  "average_relevance": 1.92
}
```

### `GET /api/v1/query-feedback/feedback?query=...`

Purpose:

- return feedback records associated with a normalized query

Example response shape:

```json
{
  "query": "How do decorators work?",
  "normalized_query": "how do decorators work?",
  "items": [
    {
      "id": 1,
      "query": "How do decorators work?",
      "normalized_query": "how do decorators work?",
      "chunk_id": "python_docs:chunk-1",
      "source_id": "python_docs",
      "relevance": 3,
      "notes": "Directly explains wrappers.",
      "session_id": "session-a",
      "created_at": "2026-05-22T10:00:00+00:00",
      "updated_at": "2026-05-22T10:05:00+00:00"
    }
  ]
}
```

## Data Model

The `query_feedback` table stores explicit feedback records with the following columns:

- `id`
- `query`
- `normalized_query`
- `chunk_id`
- `source_id`
- `relevance`
- `notes`
- `session_id`
- `created_at`
- `updated_at`

## Testing

Main test files:

- `tests/unit/test_query_feedback_expansion.py`
- `tests/unit/test_query_feedback_repository.py`
- `tests/unit/test_query_feedback_reranker.py`
- `tests/unit/test_query_feedback_service.py`
- `tests/unit/test_query_feedback_api_routes.py`

Command:

```bash
pytest tests/unit/test_query_feedback_expansion.py \
       tests/unit/test_query_feedback_repository.py \
       tests/unit/test_query_feedback_reranker.py \
       tests/unit/test_query_feedback_service.py \
       tests/unit/test_query_feedback_api_routes.py
```

## Out of Scope / Current Limitations

- no frontend integration is included in this backend module
- no evaluation module integration is implemented yet
- no recent feedback endpoint is provided
- query embeddings are not persisted; semantic matching computes them at runtime
- feedback is applied only through `/search-with-feedback`; `/search` remains unchanged
