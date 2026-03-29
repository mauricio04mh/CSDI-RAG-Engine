from __future__ import annotations

import re

import snowballstemmer

# Words that carry no discriminative value for BM25 matching
_STOP_WORDS: frozenset[str] = frozenset({
    "a", "an", "the", "and", "or", "but", "in", "on", "at", "to", "for",
    "of", "with", "by", "from", "is", "are", "was", "were", "be", "been",
    "being", "have", "has", "had", "do", "does", "did", "will", "would",
    "could", "should", "may", "might", "shall", "can", "not", "no", "nor",
    "so", "yet", "both", "either", "neither", "as", "if", "then", "than",
    "that", "this", "these", "those", "it", "its", "i", "we", "you", "he",
    "she", "they", "what", "which", "who", "how", "when", "where", "why",
})

_TOKEN_RE = re.compile(r"[a-z0-9]+")
_stemmer = snowballstemmer.stemmer("english")


def tokenize(text: str) -> list[str]:
    """Tokenize, lowercase, remove stop words, and stem.

    Consistent tokenization for both indexing and querying so that
    'decorators', 'decorated', 'decorator' all match the same stem.
    """
    tokens = _TOKEN_RE.findall(text.lower())
    return [
        _stemmer.stemWord(tok)
        for tok in tokens
        if tok not in _STOP_WORDS and len(tok) > 1
    ]
