# Query Feedback Module

This module contains the base structure for the Query Feedback functionality in the CSDI RAG Engine.

This module supports query feedback workflows for the CSDI RAG Engine.

It includes:

- query expansion through pseudo-relevance feedback
- explicit relevance feedback handling
- feedback-aware search and reranking flows

## Phase 1

Phase 1 registers the base API skeleton and a module health route.

## Phase 2

Phase 2 adds pseudo-relevance feedback query expansion.

- The module uses the top chunks retrieved by the existing hybrid retriever.
- Expansion terms are extracted deterministically from chunk titles, breadcrumbs, and text.
- The implementation does not use a manual dictionary or an LLM.

Endpoint:

- `POST /api/v1/query-feedback/expand`

## Phase 3

Phase 3 adds expanded hybrid search.

- `/expand` only returns the expanded query and selected expansion terms.
- `/search` executes retrieval and returns enriched chunk results.
- When `expansion_enabled=true`, BM25 uses the original query and Vector Search uses the expanded query through `HybridRetriever.vector_query`.
- Retriever internals are not modified.
- No user feedback is persisted yet.

Endpoint:

- `POST /api/v1/query-feedback/search`

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
