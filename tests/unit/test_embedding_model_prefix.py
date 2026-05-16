from __future__ import annotations

from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from src.vector_indexing.encoder.embedding_model import EmbeddingModel


def _make_model(dim: int = 4) -> tuple[EmbeddingModel, MagicMock]:
    model = EmbeddingModel(model_name="fake-model", expected_dimension=dim)
    mock_st = MagicMock()
    mock_st.encode.return_value = np.ones((1, dim), dtype=np.float32)
    model._model = mock_st
    return model, mock_st


def test_encode_query_prepends_prefix():
    model, mock_st = _make_model()
    prefix = "Represent this sentence: "
    model.encode_query("hello world", prefix=prefix)
    call_args = mock_st.encode.call_args[0][0]
    assert call_args == ["Represent this sentence: hello world"]


def test_encode_query_empty_prefix_unchanged():
    model, mock_st = _make_model()
    model.encode_query("hello world", prefix="")
    call_args = mock_st.encode.call_args[0][0]
    assert call_args == ["hello world"]


def test_encode_query_no_prefix_arg_unchanged():
    model, mock_st = _make_model()
    model.encode_query("hello world")
    call_args = mock_st.encode.call_args[0][0]
    assert call_args == ["hello world"]


def test_encode_one_unaffected_by_prefix():
    """encode_one (used for documents) must never apply the query prefix."""
    model, mock_st = _make_model()
    model.encode_one("my document text")
    call_args = mock_st.encode.call_args[0][0]
    assert call_args == ["my document text"]
