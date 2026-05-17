from __future__ import annotations

from src.document_processing.chunker import DocumentChunk

_SYSTEM_PROMPT = (
    "Eres un asistente de documentación técnica. "
    "Responde SIEMPRE en español, independientemente del idioma del contexto. "
    "Responde usando ÚNICAMENTE la información de los fragmentos proporcionados: puedes explicar, "
    "sintetizar y traducir su contenido. NO agregues hechos ni afirmaciones que no aparezcan en ningún fragmento. "
    "Si los fragmentos contienen información parcialmente relacionada, úsala para dar la mejor respuesta posible "
    "e indica qué aspectos no están cubiertos. "
    "Solo responde 'No encuentro información suficiente en los documentos proporcionados para responder esta pregunta.' "
    "cuando los fragmentos no tengan NINGUNA relación con la pregunta. "
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
