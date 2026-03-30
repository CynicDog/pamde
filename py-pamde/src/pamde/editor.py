"""
High-level Python API for editing Parquet metadata.

Usage:
    editor = ParquetEditor.open("my_data.parquet")
    print(editor.columns())
    editor.set_file_tag("owner", "data-team")
    editor.set_column_tag("name", "description", "Primary key")
    editor.save("my_data_annotated.parquet")
"""

from __future__ import annotations

import dataclasses
from pathlib import Path
from typing import Any

from _pamde_runtime._pamde_runtime import PyParquetFile


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
    def _from_rust(cls, raw: Any) -> ColumnInfo:
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
            tags=dict(raw.column_kv_metadata),
        )


class ParquetEditor:
    """Open a Parquet file for metadata inspection and editing."""

    _path: Path
    _backend: Any

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._backend = PyParquetFile(str(self._path))

    @classmethod
    def open(cls, path: str | Path) -> ParquetEditor:
        return cls(path)

    def columns(self) -> list[ColumnInfo]:
        return [ColumnInfo._from_rust(c) for c in self._backend.columns()]

    def file_tags(self) -> dict[str, str | None]:
        return self._backend.file_tags()

    def set_file_tag(
        self, key: str, value: str | None, *, out_path: str | Path | None = None
    ) -> None:
        dest = str(out_path or self._path)
        self._backend.set_file_tag(key, value, dest)

    def set_column_tag(
        self,
        column_path: str,
        key: str,
        value: str | None,
        *,
        out_path: str | Path | None = None,
    ) -> None:
        dest = str(out_path or self._path)
        self._backend.set_column_tag(column_path, key, value, dest)

    def set_column_tags_batch(
        self,
        updates: list[tuple[str, str, str | None]],
        *,
        out_path: str | Path | None = None,
    ) -> None:
        """Apply multiple column tag mutations with a single footer write."""
        dest = str(out_path or self._path)
        self._backend.set_column_tags_batch(updates, dest)

    def save(self, out_path: str | Path) -> None:
        pass
