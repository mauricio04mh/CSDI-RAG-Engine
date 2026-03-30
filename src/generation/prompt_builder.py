from __future__ import annotations

from src.document_processing.chunker import DocumentChunk

_SYSTEM_PROMPT = (
    "Eres un asistente experto en programación. "
    "Responde SIEMPRE en español, sin excepción, independientemente del idioma de la pregunta. "
    "Responde la pregunta del usuario basándote ÚNICAMENTE en el contexto de documentación proporcionado. "
    "Si el contexto no contiene suficiente información para responder, indícalo claramente. "
    "No inventes información. Sé conciso y preciso. "
    "Al mostrar ejemplos de código, usa bloques de código markdown."
)


def build_messages(query: str, chunks: list[DocumentChunk]) -> list[dict]:
    """Build the chat messages list for the LLM from a query and retrieved chunks."""
    context_parts: list[str] = []
    for i, chunk in enumerate(chunks, 1):
        context_parts.append(
            f"[{i}] {chunk.title}\nSource: {chunk.url}\n\n{chunk.text}"
        )

    context = "\n\n---\n\n".join(context_parts)

    user_content = f"Contexto de la documentación:\n\n{context}\n\n---\n\nPregunta: {query}"

    return [
        {"role": "system", "content": _SYSTEM_PROMPT},
        {"role": "user", "content": user_content},
    ]
