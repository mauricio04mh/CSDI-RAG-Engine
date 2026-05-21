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
