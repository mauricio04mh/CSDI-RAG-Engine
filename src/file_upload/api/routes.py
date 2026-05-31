from __future__ import annotations

import io
import logging
from pathlib import Path

from fastapi import APIRouter, Form, HTTPException, Request, UploadFile
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel

from src.document_processing.chunker import Chunker
from src.ingestion.chunk_ingestion_service import ChunkIngestionService

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1", tags=["upload"])

_SUPPORTED_SUFFIXES = {".txt", ".md", ".pdf", ".docx"}


class UploadResponse(BaseModel):
    source_id: str
    filename: str
    chunks_produced: int
    chunks_indexed: int


def _extract_text(filename: str, data: bytes) -> str:
    suffix = Path(filename).suffix.lower()
    if suffix in (".txt", ".md"):
        return data.decode("utf-8", errors="replace")
    if suffix == ".pdf":
        try:
            import pypdf
            reader = pypdf.PdfReader(io.BytesIO(data))
            return "\n".join(page.extract_text() or "" for page in reader.pages)
        except Exception as exc:
            raise ValueError(f"PDF extraction failed: {exc}") from exc
    if suffix == ".docx":
        try:
            import docx
            doc = docx.Document(io.BytesIO(data))
            return "\n".join(para.text for para in doc.paragraphs)
        except Exception as exc:
            raise ValueError(f"DOCX extraction failed: {exc}") from exc
    raise ValueError(
        f"Unsupported file type '{suffix}'. Supported: {sorted(_SUPPORTED_SUFFIXES)}"
    )


@router.post("/upload", response_model=UploadResponse)
async def upload_file(
    request: Request,
    file: UploadFile,
    source_id: str = Form(default=None),
) -> UploadResponse:
    """Upload a TXT, MD, PDF, or DOCX file; chunk and index its content."""
    filename = file.filename or "upload"
    suffix = Path(filename).suffix.lower()

    if suffix not in _SUPPORTED_SUFFIXES:
        raise HTTPException(
            status_code=415,
            detail=f"Unsupported file type '{suffix}'. Supported: {sorted(_SUPPORTED_SUFFIXES)}",
        )

    if source_id is None:
        source_id = Path(filename).stem

    data = await file.read()
    if not data:
        raise HTTPException(status_code=400, detail="Uploaded file is empty.")

    chunk_ingestion: ChunkIngestionService = request.app.state.chunk_ingestion_service
    user_source_repo = request.app.state.user_source_repo

    # All CPU/IO-heavy work runs in a thread so the asyncio event loop stays
    # free to serve polls and other requests while the upload is processed.
    def _process() -> UploadResponse:
        try:
            text = _extract_text(filename, data)
        except ValueError as exc:
            raise exc

        if not text.strip():
            raise ValueError("No extractable text found in file.")

        chunks = Chunker().chunk(
            source_id=source_id,
            url=f"upload://{filename}",
            title=Path(filename).stem,
            breadcrumb="",
            content=text,
        )

        if not chunks:
            raise ValueError("File produced no chunks after processing.")

        ingestion_result = chunk_ingestion.ingest_chunks(chunks)
        finalize_result = chunk_ingestion.finalize(reload_bm25=True)
        if finalize_result.vector_flushed:
            logger.debug("upload_vector_buffer_flushed count=%s", finalize_result.vector_flushed)

        user_source_repo.register(
            source_id=source_id,
            name=Path(filename).stem,
            base_url=f"upload://{filename}",
            source_kind="upload_file",
        )

        logger.info(
            "upload_complete source_id=%s filename=%s produced=%s indexed=%s",
            source_id,
            filename,
            len(chunks),
            ingestion_result.indexed_chunks,
        )
        return UploadResponse(
            source_id=source_id,
            filename=filename,
            chunks_produced=len(chunks),
            chunks_indexed=ingestion_result.indexed_chunks,
        )

    try:
        return await run_in_threadpool(_process)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
