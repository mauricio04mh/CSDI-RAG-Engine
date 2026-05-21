from __future__ import annotations

from fastapi import APIRouter

router = APIRouter(prefix="/api/v1/query-feedback", tags=["query-feedback"])


@router.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "module": "query-feedback"}
