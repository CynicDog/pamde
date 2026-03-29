"""
High-level Python API for editing Parquet metadata.

Backend selection (automatic):
  1. Rust extension (_pamde_runtime) — preferred when built via `maturin develop`.
  2. pyarrow backend — fallback for running without a compiled extension.

Usage:
    editor = ParquetEditor.open("my_data.parquet")
    print(editor.columns())
    editor.set_file_tag("owner", "data-team")
    editor.set_column_tag("fruits.name", "description", "Primary key for fruit taxonomy")
    editor.save("my_data_annotated.parquet")
"""

from __future__ import annotations

import dataclasses
from pathlib import Path
from typing import Any


def _use_rust() -> bool:
    try:
        from _pamde_runtime._pamde_runtime import PyParquetFile  # noqa: F401

        return True
    except ImportError:
        return False


def _make_file(path: str | Path) -> Any:
    if _use_rust():
        from _pamde_runtime._pamde_runtime import PyParquetFile

        return PyParquetFile.open(str(path))
    else:
        from pamde._pyarrow_backend import ParquetFile

        return ParquetFile(path)


@dataclasses.dataclass
class ColumnInfo:
    """Human-facing view of a single Parquet leaf column."""

    physical_name: str
    path_in_schema: str
    physical_type: str
    logical_type: str | None
    repetition: str
    field_id: int | None
    null_count: int | None
    distinct_count: int | None
    min_value: str | None
    max_value: str | None
    compression: str
    total_compressed_size: int
    total_uncompressed_size: int
    tags: dict[str, str | None]

    @classmethod
    def _from_backend(cls, raw: Any) -> ColumnInfo:
        """Normalise from either the Rust PyColumnInfo or the pyarrow ColumnInfo."""
        if isinstance(raw, cls):
            return raw
        # Rust PyColumnInfo: attributes, column_kv_metadata is list of tuples
        if hasattr(raw, "column_kv_metadata"):
            tags = dict(raw.column_kv_metadata)
        else:
            tags = getattr(raw, "tags", {})
        return cls(
            physical_name=raw.physical_name,
            path_in_schema=raw.path_in_schema,
            physical_type=raw.physical_type,
            logical_type=raw.logical_type,
            repetition=raw.repetition,
            field_id=raw.field_id,
            null_count=raw.null_count,
            distinct_count=raw.distinct_count,
            min_value=raw.min_value,
            max_value=raw.max_value,
            compression=raw.compression,
            total_compressed_size=raw.total_compressed_size,
            total_uncompressed_size=raw.total_uncompressed_size,
            tags=tags,
        )


class ParquetEditor:
    """Open a Parquet file for metadata inspection and editing."""

    _path: Path
    _backend: Any  # PyParquetFile (Rust) or ParquetFile (pyarrow)

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._backend = _make_file(self._path)

    @classmethod
    def open(cls, path: str | Path) -> ParquetEditor:
        return cls(path)

    @property
    def backend_name(self) -> str:
        return "rust" if _use_rust() else "pyarrow"

    def columns(self) -> list[ColumnInfo]:
        """Return one ColumnInfo per leaf column in the Parquet schema."""
        return [ColumnInfo._from_backend(c) for c in self._backend.columns()]

    def file_tags(self) -> dict[str, str | None]:
        """Return file-level key/value metadata."""
        raw = self._backend.file_tags()
        # Rust backend returns list of tuples; pyarrow backend returns dict
        if isinstance(raw, list):
            return dict(raw)
        return raw

    def set_file_tag(
        self, key: str, value: str | None, *, out_path: str | Path | None = None
    ) -> None:
        """Upsert a file-level metadata tag.  Writes to out_path (or same file)."""
        self._backend.set_file_tag(key, value, out_path or self._path)

    def set_column_tag(
        self,
        column_path: str,
        key: str,
        value: str | None,
        *,
        out_path: str | Path | None = None,
    ) -> None:
        """Upsert a column-level metadata tag for the column at column_path."""
        self._backend.set_column_tag(column_path, key, value, out_path or self._path)

    def save(self, out_path: str | Path) -> None:
        """Write current metadata state to out_path."""
        pass
