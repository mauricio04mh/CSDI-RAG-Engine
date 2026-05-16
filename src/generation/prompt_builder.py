from __future__ import annotations

from src.document_processing.chunker import DocumentChunk

_SYSTEM_PROMPT = (
    "Eres un asistente de recuperación de información. "
    "Responde SIEMPRE en español, sin excepción, independientemente del idioma de la pregunta. "
    "REGLA ABSOLUTA: responde EXCLUSIVAMENTE con información que aparezca de forma explícita en los fragmentos de contexto proporcionados. "
    "Está TERMINANTEMENTE PROHIBIDO usar conocimiento propio, datos de entrenamiento, suposiciones o inferencias que no estén respaldadas palabra por palabra por el contexto. "
    "Si el contexto no contiene la información necesaria para responder la pregunta, responde EXACTAMENTE: "
    "'No encuentro información suficiente en los documentos proporcionados para responder esta pregunta.' "
    "No amplíes, no deduzcas, no rellenes huecos con conocimiento externo bajo ninguna circunstancia. "
    "Al mostrar ejemplos de código, usa bloques de código markdown. "
    "Sé conciso y cita el fragmento relevante cuando sea posible."
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
