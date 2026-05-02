from __future__ import annotations

from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from src.database.models.web_search_models import WebSearchHitRow, WebSearchRun
from src.web_search.schemas import WebSearchHit


class WebSearchRepository:
    """Persistence operations for web-search runs and hits."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def save_run(
        self,
        *,
        query: str,
        provider: str,
        hits: list[WebSearchHit],
        documents_count: int,
        indexed_count: int,
    ) -> int:
        with Session(self.engine) as session, session.begin():
            run = WebSearchRun(
                query=query,
                provider=provider,
                hits_count=len(hits),
                documents_count=documents_count,
                indexed_count=indexed_count,
            )
            session.add(run)
            session.flush()

            rows = [
                WebSearchHitRow(
                    run_id=run.id,
                    rank=rank,
                    title=hit.title,
                    url=hit.url,
                    snippet=hit.snippet,
                    score=hit.score,
                    provider=hit.provider,
                    extra_metadata=dict(hit.metadata),
                )
                for rank, hit in enumerate(hits, start=1)
            ]
            if rows:
                session.add_all(rows)

            return int(run.id)
