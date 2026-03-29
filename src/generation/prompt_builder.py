from __future__ import annotations

from src.document_processing.chunker import DocumentChunk

_SYSTEM_PROMPT = (
    "You are a helpful programming assistant. "
    "Answer the user's question based ONLY on the provided documentation context. "
    "If the context does not contain enough information to answer, say so clearly. "
    "Do not make up information. Be concise and precise. "
    "When showing code examples, use markdown code blocks."
)


def build_messages(query: str, chunks: list[DocumentChunk]) -> list[dict]:
    """Build the chat messages list for the LLM from a query and retrieved chunks."""
    context_parts: list[str] = []
    for i, chunk in enumerate(chunks, 1):
        context_parts.append(
            f"[{i}] {chunk.title}\nSource: {chunk.url}\n\n{chunk.text}"
        )

    context = "\n\n---\n\n".join(context_parts)

    user_content = f"Context from documentation:\n\n{context}\n\n---\n\nQuestion: {query}"

    return [
        {"role": "system", "content": _SYSTEM_PROMPT},
        {"role": "user", "content": user_content},
    ]
