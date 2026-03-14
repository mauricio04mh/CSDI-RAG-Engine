from fastapi import APIRouter
from pydantic import BaseModel, Field


router = APIRouter(prefix="/api/v1", tags=["search"])


class SearchRequest(BaseModel):
    query: str = Field(..., min_length=1, description="Programming question or search query.")
    top_k: int = Field(default=5, ge=1, le=20, description="Maximum number of results.")


@router.post("/search")
async def search(request: SearchRequest) -> dict:
    return {
        "query": request.query,
        "top_k": request.top_k,
        "results": [],
        "answer": "Search pipeline not implemented yet.",
    }
