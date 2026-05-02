from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping


@dataclass(slots=True)
class WebSearchHit:
    title: str
    url: str
    snippet: str
    score: float | None = None
    provider: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class WebSearchDocument:
    url: str
    title: str
    text: str
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class WebSearchRunResult:
    query: str
    hits: list[WebSearchHit]
    documents: list[WebSearchDocument]
    indexed_count: int
