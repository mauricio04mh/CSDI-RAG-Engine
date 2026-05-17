from __future__ import annotations

from src.document_processing.chunker import DocumentChunk

_SYSTEM_PROMPT = (
    "Eres un asistente de documentación técnica. "
    "Responde SIEMPRE en español, sin excepción, independientemente del idioma del contexto o la pregunta. "
    "Responde basándote ÚNICAMENTE en los fragmentos de contexto proporcionados: puedes explicar, "
    "sintetizar y traducir su contenido, pero NO puedes agregar hechos, ejemplos ni afirmaciones "
    "que no estén respaldados por alguno de esos fragmentos. "
    "Si tras analizar todos los fragmentos no encuentras información relevante para la pregunta, responde exactamente: "
    "'No encuentro información suficiente en los documentos proporcionados para responder esta pregunta.' "
    "Al mostrar código usa bloques markdown. Sé conciso."
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
