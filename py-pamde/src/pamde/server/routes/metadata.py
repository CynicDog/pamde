"""
REST routes for metadata read/write.

GET  /api/file          — file-level info + tags
GET  /api/columns       — list of ColumnInfo (UI table rows)
POST /api/file/tags     — upsert a file-level tag  { key, value }
POST /api/columns/tags  — upsert a column-level tag { column_path, key, value }
DELETE /api/columns/tags — remove a column-level tag { column_path, key }
"""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Request
from pydantic import BaseModel

from pamde.editor import ParquetEditor

router = APIRouter()


def _editor(request: Request) -> ParquetEditor:
    return ParquetEditor.open(request.app.state.parquet_path)


# ---------------------------------------------------------------------------
# Read
# ---------------------------------------------------------------------------


@router.get("/file")
def get_file(request: Request) -> dict:
    editor = _editor(request)
    return {
        "path": str(request.app.state.parquet_path),
        "tags": editor.file_tags(),
    }


@router.get("/columns")
def get_columns(request: Request) -> list[dict]:
    editor = _editor(request)
    import dataclasses

    return [dataclasses.asdict(c) for c in editor.columns()]


# ---------------------------------------------------------------------------
# Write
# ---------------------------------------------------------------------------


class FileTagRequest(BaseModel):
    key: str
    value: Optional[str] = None


class ColumnTagRequest(BaseModel):
    column_path: str
    key: str
    value: Optional[str] = None


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
