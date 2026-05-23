from __future__ import annotations

from src.document_processing.chunker import DocumentChunk

_BASE = (
    "Eres un asistente de documentación técnica. "
    "Responde SIEMPRE en español, independientemente del idioma del contexto. "
    "Responde usando ÚNICAMENTE la información de los fragmentos proporcionados: puedes explicar, "
    "sintetizar y traducir su contenido. NO agregues hechos ni afirmaciones que no aparezcan en ningún fragmento. "
    "Si los fragmentos contienen información parcialmente relacionada, úsala para dar la mejor respuesta posible "
    "e indica qué aspectos no están cubiertos. "
    "Al mostrar código usa bloques markdown. Sé conciso."
)

# Scenario 1: results only from internal corpus (original behaviour).
_SYSTEM_CORPUS_ONLY = (
    _BASE
    + " Solo responde 'No encuentro información suficiente en los documentos proporcionados para responder esta pregunta.' "
    "cuando los fragmentos no tengan NINGUNA relación con la pregunta."
)

# Scenario 2: no corpus results, everything comes from web search.
_SYSTEM_WEB_ONLY = (
    _BASE
    + " No se encontró información en el corpus de documentación técnica interno. "
    "Los fragmentos provienen de búsqueda web. "
    "Inicia tu respuesta indicando explícitamente que no tienes información sobre el tema en tu corpus de conocimiento "
    "pero que encontraste lo siguiente en la web. "
    "Ejemplo de apertura: 'No tengo información sobre esto en mi corpus de conocimiento, "
    "pero esto es lo más relevante que encontré en la web:'. "
    "Solo indica que no encontraste nada si los fragmentos web tampoco son relevantes."
)

# Scenario 3: corpus results + additional web results.
_SYSTEM_MIXED = (
    _BASE
    + " Los fragmentos están divididos en dos secciones: "
    "'CORPUS DE DOCUMENTACIÓN TÉCNICA' con información interna y "
    "'RESULTADOS DE BÚSQUEDA WEB' con resultados adicionales de la web. "
    "Estructura tu respuesta en dos partes separadas: "
    "primero bajo el encabezado '**Información del corpus:**' lo encontrado en la documentación interna, "
    "luego bajo '**Información adicional de la web:**' lo encontrado en la búsqueda web. "
    "Si alguna sección no aporta información relevante para la pregunta, puedes omitirla."
)


def _build_context(corpus_chunks: list[DocumentChunk], web_chunks: list[DocumentChunk]) -> str:
    parts: list[str] = []

    if corpus_chunks:
        corpus_items = [
            f"[C{i}] {c.title}\nFuente: {c.url}\n\n{c.text}"
            for i, c in enumerate(corpus_chunks, 1)
        ]
        parts.append(
            "=== CORPUS DE DOCUMENTACIÓN TÉCNICA ===\n\n"
            + "\n\n---\n\n".join(corpus_items)
        )

    if web_chunks:
        web_items = [
            f"[W{i}] {c.title}\nFuente: {c.url}\n\n{c.text}"
            for i, c in enumerate(web_chunks, 1)
        ]
        parts.append(
            "=== RESULTADOS DE BÚSQUEDA WEB ===\n\n"
            + "\n\n---\n\n".join(web_items)
        )

    return "\n\n".join(parts)


def build_messages(
    query: str,
    corpus_chunks: list[DocumentChunk],
    web_chunks: list[DocumentChunk] | None = None,
) -> list[dict]:
    """Build the chat messages list for the LLM.

    Selects the system prompt and structures the context based on whether
    results come from the internal corpus, the web, or both.
    """
    web_chunks = web_chunks or []

    has_corpus = bool(corpus_chunks)
    has_web = bool(web_chunks)

    if has_corpus and has_web:
        system_prompt = _SYSTEM_MIXED
    elif has_web:
        system_prompt = _SYSTEM_WEB_ONLY
    else:
        system_prompt = _SYSTEM_CORPUS_ONLY

    context = _build_context(corpus_chunks, web_chunks)
    user_content = f"Contexto de la documentación:\n\n{context}\n\n---\n\nPregunta: {query}"

    return [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_content},
    ]
