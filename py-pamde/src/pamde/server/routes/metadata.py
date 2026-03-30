"""
REST routes for metadata read/write.

GET  /api/status        — server mode + active file name
GET  /api/file          — file-level info + tags
GET  /api/columns       — list of ColumnInfo (UI table rows)
POST /api/upload        — upload a .parquet file (reads footer only, not row data)
POST /api/file/tags     — upsert a file-level tag  { key, value }
POST /api/columns/tags  — upsert a column-level tag { column_path, key, value }
DELETE /api/columns/tags — remove a column-level tag { column_path, key }
"""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel

from pamde.editor import ParquetEditor

router = APIRouter()

_UPLOAD_DIR = Path.cwd() / "pamde_uploads"
_UPLOAD_DIR.mkdir(exist_ok=True)


def _editor(request: Request) -> ParquetEditor:
    if not request.app.state.parquet_path:
        raise HTTPException(status_code=400, detail="No file loaded. Upload a file first.")
    return ParquetEditor.open(request.app.state.parquet_path)


@router.get("/status")
def get_status(request: Request) -> dict:
    path = request.app.state.parquet_path
    return {
        "mode": request.app.state.mode,
        "file": Path(path).name if path else None,
    }


@router.get("/file")
def get_file(request: Request) -> dict:
    editor = _editor(request)
    return {
        "path": str(request.app.state.parquet_path),
        "file": Path(request.app.state.parquet_path).name,
        "tags": editor.file_tags(),
    }


@router.get("/columns")
def get_columns(request: Request) -> list[dict]:
    import dataclasses

    editor = _editor(request)
    return [dataclasses.asdict(c) for c in editor.columns()]


@router.post("/upload")
async def upload_file(request: Request, file: UploadFile) -> dict:
    if not file.filename or not file.filename.endswith(".parquet"):
        raise HTTPException(status_code=400, detail="Only .parquet files are accepted.")

    dest = _UPLOAD_DIR / file.filename
    with dest.open("wb") as f:
        shutil.copyfileobj(file.file, f)

    # Validate that it's a readable parquet file (reads footer only, not row data).
    try:
        ParquetEditor.open(str(dest))
    except Exception as e:
        dest.unlink(missing_ok=True)
        raise HTTPException(status_code=422, detail=f"Not a valid Parquet file: {e}") from e

    request.app.state.parquet_path = str(dest)
    return {"file": file.filename}


@router.get("/download")
def download_file(request: Request) -> FileResponse:
    path = request.app.state.parquet_path
    if not path:
        raise HTTPException(status_code=400, detail="No file loaded.")
    filename = Path(path).name
    return FileResponse(path, media_type="application/octet-stream", filename=filename)


class FileTagRequest(BaseModel):
    key: str
    value: Optional[str] = None


class ColumnTagRequest(BaseModel):
    column_path: str
    key: str
    value: Optional[str] = None


class ColumnTagUpdate(BaseModel):
    column_path: str
    key: str
    value: Optional[str] = None


class ColumnTagsBatchRequest(BaseModel):
    updates: list[ColumnTagUpdate]


@router.post("/file/tags")
def set_file_tag(request: Request, body: FileTagRequest) -> dict:
    editor = _editor(request)
    editor.set_file_tag(body.key, body.value)
    return {"ok": True}


@router.post("/columns/tags")
def set_column_tag(request: Request, body: ColumnTagRequest) -> dict:
    editor = _editor(request)
    editor.set_column_tag(body.column_path, body.key, body.value)
    return {"ok": True}


@router.delete("/columns/tags")
def remove_column_tag(request: Request, body: ColumnTagRequest) -> dict:
    editor = _editor(request)
    editor.set_column_tag(body.column_path, body.key, value=None)
    return {"ok": True}


@router.post("/columns/tags/batch")
def set_column_tags_batch(request: Request, body: ColumnTagsBatchRequest) -> dict:
    editor = _editor(request)
    updates = [(u.column_path, u.key, u.value) for u in body.updates]
    editor.set_column_tags_batch(updates)
    return {"ok": True}
