"""
Pure-Python backend for pamde using pyarrow.

Used when the Rust extension (_pamde_runtime) is not available.
Provides the same interface as the Rust backend so editor.py is backend-agnostic.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq


# ---------------------------------------------------------------------------
# ColumnInfo
# ---------------------------------------------------------------------------

@dataclass
class ColumnInfo:
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
    tags: dict[str, str | None] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# ParquetFile
# ---------------------------------------------------------------------------

class ParquetFile:
    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._meta = pq.read_metadata(str(self._path))

    # ------------------------------------------------------------------
    # Inspection
    # ------------------------------------------------------------------

    def columns(self) -> list[ColumnInfo]:
        schema = self._meta.schema
        num_cols = len(schema)

        # Aggregate stats + sizes across all row groups per column index.
        agg: dict[int, dict[str, Any]] = {i: {} for i in range(num_cols)}
        for rg_idx in range(self._meta.num_row_groups):
            rg = self._meta.row_group(rg_idx)
            for col_idx in range(rg.num_columns):
                chunk = rg.column(col_idx)
                a = agg[col_idx]

                if "compression" not in a:
                    a["compression"] = str(chunk.compression)

                a["total_compressed_size"] = (
                    a.get("total_compressed_size", 0) + chunk.total_compressed_size
                )
                a["total_uncompressed_size"] = (
                    a.get("total_uncompressed_size", 0) + chunk.total_uncompressed_size
                )

                if chunk.is_stats_set and chunk.statistics is not None:
                    s = chunk.statistics
                    nc = s.null_count
                    if nc is not None:
                        a["null_count"] = a.get("null_count", 0) + nc
                    dc = s.distinct_count
                    if dc is not None:
                        # Not additive; keep max across row groups as approximation.
                        a["distinct_count"] = max(a.get("distinct_count", 0), dc)
                    if s.has_min_max:
                        new_min = _to_str(s.min)
                        new_max = _to_str(s.max)
                        cur_min = a.get("min_value")
                        cur_max = a.get("max_value")
                        a["min_value"] = new_min if cur_min is None else min(cur_min, new_min)
                        a["max_value"] = new_max if cur_max is None else max(cur_max, new_max)

        # Read arrow schema once for field metadata + field_ids.
        arrow_schema = pq.read_schema(str(self._path))

        result: list[ColumnInfo] = []
        for col_idx in range(num_cols):
            col_s = schema.column(col_idx)
            a = agg[col_idx]
            repetition = _repetition(col_s)
            tags, field_id = _arrow_field_info(arrow_schema, col_s.name)

            result.append(
                ColumnInfo(
                    physical_name=col_s.name,
                    path_in_schema=col_s.path,
                    physical_type=str(col_s.physical_type),
                    logical_type=_logical_type_str(col_s),
                    repetition=repetition,
                    field_id=field_id,
                    null_count=a.get("null_count"),
                    distinct_count=a.get("distinct_count"),
                    min_value=a.get("min_value"),
                    max_value=a.get("max_value"),
                    compression=a.get("compression", "UNCOMPRESSED"),
                    total_compressed_size=a.get("total_compressed_size", 0),
                    total_uncompressed_size=a.get("total_uncompressed_size", 0),
                    tags=tags,
                )
            )
        return result

    def file_tags(self) -> dict[str, str | None]:
        raw = self._meta.metadata  # bytes→bytes dict or None
        if not raw:
            return {}
        return {
            k.decode("utf-8", errors="replace"): v.decode("utf-8", errors="replace")
            for k, v in raw.items()
        }

    # ------------------------------------------------------------------
    # Mutation
    # ------------------------------------------------------------------

    def set_file_tag(
        self, key: str, value: str | None, out_path: str | Path | None = None
    ) -> None:
        dest = Path(out_path or self._path)
        import pyarrow as pa

        table = pq.read_table(str(self._path))
        existing: dict[bytes, bytes] = dict(table.schema.metadata or {})
        kb = key.encode()
        if value is None:
            existing.pop(kb, None)
        else:
            existing[kb] = value.encode()
        table = table.replace_schema_metadata(existing)
        pq.write_table(table, str(dest))
        if dest == self._path:
            self._meta = pq.read_metadata(str(self._path))

    def set_column_tag(
        self,
        column_path: str,
        key: str,
        value: str | None,
        out_path: str | Path | None = None,
    ) -> None:
        dest = Path(out_path or self._path)
        import pyarrow as pa

        table = pq.read_table(str(self._path))
        top_name = column_path.split(".")[0]
        fields = []
        for f in table.schema:
            if f.name == top_name:
                meta: dict[bytes, bytes] = dict(f.metadata or {})
                kb = key.encode()
                if value is None:
                    meta.pop(kb, None)
                else:
                    meta[kb] = value.encode()
                fields.append(f.with_metadata(meta))
            else:
                fields.append(f)
        new_schema = pa.schema(fields, metadata=table.schema.metadata)
        table = table.cast(new_schema)
        pq.write_table(table, str(dest))
        if dest == self._path:
            self._meta = pq.read_metadata(str(self._path))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _to_str(v: Any) -> str:
    if isinstance(v, bytes):
        try:
            return v.decode("utf-8")
        except Exception:
            return v.hex()
    return str(v)


def _repetition(col_s: Any) -> str:
    """
    Infer repetition from definition/repetition levels.
    max_definition_level == 0 → REQUIRED
    max_repetition_level > 0  → REPEATED
    otherwise                 → OPTIONAL
    """
    if getattr(col_s, "max_definition_level", 1) == 0:
        return "REQUIRED"
    if getattr(col_s, "max_repetition_level", 0) > 0:
        return "REPEATED"
    return "OPTIONAL"


def _logical_type_str(col_s: Any) -> str | None:
    lt = col_s.logical_type
    if lt is None:
        return None
    s = str(lt)
    # pyarrow returns e.g. "String" or "Null" — return as-is
    return s if s not in ("None", "") else None


def _arrow_field_info(
    arrow_schema: Any, col_name: str
) -> tuple[dict[str, str | None], int | None]:
    """Return (user tags dict, field_id | None) from the Arrow schema field."""
    try:
        f = arrow_schema.field(col_name)
    except (KeyError, Exception):
        return {}, None

    field_id: int | None = None
    tags: dict[str, str | None] = {}

    if f.metadata:
        for kb, vb in f.metadata.items():
            k = kb.decode("utf-8", errors="replace")
            v = vb.decode("utf-8", errors="replace") if vb is not None else None
            # Iceberg/Parquet internal keys — surface field_id, hide the rest
            if kb == b"PARQUET:field_id":
                try:
                    field_id = int(v)  # type: ignore[arg-type]
                except Exception:
                    pass
                continue
            if k.startswith("PARQUET:"):
                continue
            tags[k] = v

    return tags, field_id
