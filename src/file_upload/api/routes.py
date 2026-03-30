from __future__ import annotations

import io
import logging
from pathlib import Path

from fastapi import APIRouter, Form, HTTPException, Request, UploadFile
from pydantic import BaseModel

from src.bm25.text.tokenizer import tokenize
from src.database.repositories.chunk_repository import ChunkRepository
from src.document_processing.chunker import Chunker
from src.indexing.builder.index_builder import IndexBuilder
from src.vector_indexing.pipeline.vector_index_builder import VectorIndexBuilder

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

    try:
        text = _extract_text(filename, data)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    if not text.strip():
        raise HTTPException(status_code=422, detail="No extractable text found in file.")

    chunk_repo: ChunkRepository = request.app.state.chunk_repo
    index_builder: IndexBuilder = request.app.state.index_builder
    vector_index_builder: VectorIndexBuilder = request.app.state.vector_index_builder

    chunker = Chunker()
    chunks = chunker.chunk(
        source_id=source_id,
        url=f"upload://{filename}",
        title=Path(filename).stem,
        breadcrumb="",
        content=text,
    )
    chunks_produced = len(chunks)

    if not chunks:
        raise HTTPException(status_code=422, detail="File produced no chunks after processing.")

    existing_ids = chunk_repo.get_existing_chunk_ids([c.chunk_id for c in chunks])
    new_chunks = [c for c in chunks if c.chunk_id not in existing_ids]

    chunks_indexed = 0
    if new_chunks:
        chunk_repo.save_chunks(new_chunks)
        for chunk in new_chunks:
            tokens = tokenize(chunk.text)
            try:
                index_builder.add_document(doc_id=chunk.chunk_id, tokens=tokens)
                vector_index_builder.add_document(doc_id=chunk.chunk_id, text=chunk.text)
                chunks_indexed += 1
            except ValueError as exc:
                logger.warning(
                    "upload_chunk_index_conflict chunk_id=%s reason=%s", chunk.chunk_id, exc
                )
            except Exception:
                logger.exception("upload_chunk_index_failed chunk_id=%s", chunk.chunk_id)

        flushed = vector_index_builder.flush()
        if flushed:
            logger.debug("upload_vector_buffer_flushed count=%s", flushed)

    logger.info(
        "upload_complete source_id=%s filename=%s produced=%s indexed=%s",
        source_id,
        filename,
        chunks_produced,
        chunks_indexed,
    )
    return UploadResponse(
        source_id=source_id,
        filename=filename,
        chunks_produced=chunks_produced,
        chunks_indexed=chunks_indexed,
    )
