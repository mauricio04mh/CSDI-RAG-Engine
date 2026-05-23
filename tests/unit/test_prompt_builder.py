from __future__ import annotations

from datetime import datetime

from src.document_processing.chunker import DocumentChunk
from src.generation.prompt_builder import build_messages


def _corpus_chunk(chunk_id: str = "c1", title: str = "Título", text: str = "texto corpus") -> DocumentChunk:
    return DocumentChunk(
        chunk_id=chunk_id,
        source_id="docs",
        url="https://docs.example.com/page",
        title=title,
        breadcrumb="Sección > Página",
        text=text,
    )


def _web_chunk(chunk_id: str = "w1", title: str = "Web título", text: str = "texto web") -> DocumentChunk:
    return DocumentChunk(
        chunk_id=chunk_id,
        source_id="web:duckduckgo:example.com",
        url="https://web.example.com/article",
        title=title,
        breadcrumb="web-search",
        text=text,
    )


# --- Scenario 1: corpus only ---

def test_corpus_only_uses_corpus_system_prompt():
    messages = build_messages("¿qué es Python?", [_corpus_chunk()])
    system = messages[0]["content"]
    assert "corpus de documentación técnica interno" not in system
    assert "No encuentro información suficiente" in system


def test_corpus_only_labels_chunks_with_C_prefix():
    messages = build_messages("pregunta", [_corpus_chunk("c1"), _corpus_chunk("c2")])
    user = messages[1]["content"]
    assert "[C1]" in user
    assert "[C2]" in user
    assert "[W" not in user


def test_corpus_only_includes_corpus_section_header():
    messages = build_messages("pregunta", [_corpus_chunk()])
    user = messages[1]["content"]
    assert "CORPUS DE DOCUMENTACIÓN TÉCNICA" in user


# --- Scenario 2: web only ---

def test_web_only_uses_web_system_prompt():
    messages = build_messages("pregunta", [], [_web_chunk()])
    system = messages[0]["content"]
    assert "No se encontró información en el corpus de documentación técnica interno" in system
    assert "corpus de conocimiento" in system


def test_web_only_labels_chunks_with_W_prefix():
    messages = build_messages("pregunta", [], [_web_chunk("w1"), _web_chunk("w2")])
    user = messages[1]["content"]
    assert "[W1]" in user
    assert "[W2]" in user
    assert "[C" not in user


def test_web_only_includes_web_section_header():
    messages = build_messages("pregunta", [], [_web_chunk()])
    user = messages[1]["content"]
    assert "RESULTADOS DE BÚSQUEDA WEB" in user


def test_web_only_no_corpus_section_header():
    messages = build_messages("pregunta", [], [_web_chunk()])
    user = messages[1]["content"]
    assert "CORPUS DE DOCUMENTACIÓN TÉCNICA" not in user


# --- Scenario 3: corpus + web ---

def test_mixed_uses_mixed_system_prompt():
    messages = build_messages("pregunta", [_corpus_chunk()], [_web_chunk()])
    system = messages[0]["content"]
    assert "Información del corpus" in system
    assert "Información adicional de la web" in system


def test_mixed_includes_both_section_headers():
    messages = build_messages("pregunta", [_corpus_chunk()], [_web_chunk()])
    user = messages[1]["content"]
    assert "CORPUS DE DOCUMENTACIÓN TÉCNICA" in user
    assert "RESULTADOS DE BÚSQUEDA WEB" in user


def test_mixed_labels_corpus_and_web_chunks():
    messages = build_messages("pregunta", [_corpus_chunk("c1")], [_web_chunk("w1")])
    user = messages[1]["content"]
    assert "[C1]" in user
    assert "[W1]" in user


def test_mixed_preserves_query_in_user_message():
    messages = build_messages("¿cómo funciona async?", [_corpus_chunk()], [_web_chunk()])
    user = messages[1]["content"]
    assert "¿cómo funciona async?" in user


# --- Default argument: web_chunks=None is equivalent to [] ---

def test_web_chunks_none_treated_as_corpus_only():
    messages_explicit = build_messages("q", [_corpus_chunk()], [])
    messages_default = build_messages("q", [_corpus_chunk()])
    assert messages_explicit[0]["content"] == messages_default[0]["content"]


# --- Messages structure ---

def test_always_returns_two_messages():
    for corpus, web in [
        ([_corpus_chunk()], []),
        ([], [_web_chunk()]),
        ([_corpus_chunk()], [_web_chunk()]),
    ]:
        messages = build_messages("q", corpus, web)
        assert len(messages) == 2
        assert messages[0]["role"] == "system"
        assert messages[1]["role"] == "user"
