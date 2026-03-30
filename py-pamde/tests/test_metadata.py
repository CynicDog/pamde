"""
Integration tests for pamde metadata operations using the Rust backend.
Run after `maturin develop` in py-pamde/runtime/pamde-runtime/.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from pamde.editor import ColumnInfo, ParquetEditor

FRUITS = Path(__file__).parent / "fruits.parquet"


@pytest.fixture
def fruits_copy(tmp_path: Path) -> Path:
    """A writable copy of fruits.parquet in a temp directory."""
    dst = tmp_path / "fruits.parquet"
    shutil.copy(FRUITS, dst)
    return dst


class TestOpen:
    def test_open_returns_editor(self) -> None:
        editor = ParquetEditor.open(FRUITS)
        assert editor is not None

    def test_open_nonexistent_raises(self) -> None:
        with pytest.raises(Exception):
            ParquetEditor.open("/nonexistent/path/no.parquet")

    def test_open_invalid_file_raises(self, tmp_path: Path) -> None:
        bad = tmp_path / "bad.parquet"
        bad.write_bytes(b"not a parquet file at all")
        with pytest.raises(Exception):
            ParquetEditor.open(bad)


class TestColumns:
    def test_column_count(self) -> None:
        editor = ParquetEditor.open(FRUITS)
        assert len(editor.columns()) == 4

    def test_column_names(self) -> None:
        cols = ParquetEditor.open(FRUITS).columns()
        names = [c.physical_name for c in cols]
        assert names == ["name", "color", "acidity", "weight"]

    def test_column_paths(self) -> None:
        cols = ParquetEditor.open(FRUITS).columns()
        paths = [c.path_in_schema for c in cols]
        assert paths == ["name", "color", "acidity", "weight"]

    def test_physical_types(self) -> None:
        cols = {c.physical_name: c for c in ParquetEditor.open(FRUITS).columns()}
        assert cols["name"].physical_type == "BYTE_ARRAY"
        assert cols["color"].physical_type == "BYTE_ARRAY"
        assert cols["acidity"].physical_type == "DOUBLE"
        assert cols["weight"].physical_type == "INT64"

    def test_repetition_optional(self) -> None:
        cols = ParquetEditor.open(FRUITS).columns()
        for c in cols:
            assert c.repetition == "OPTIONAL"

    def test_null_count_zero(self) -> None:
        cols = ParquetEditor.open(FRUITS).columns()
        for c in cols:
            assert c.null_count == 0

    def test_statistics_present(self) -> None:
        cols = {c.physical_name: c for c in ParquetEditor.open(FRUITS).columns()}
        assert cols["name"].min_value == "apple"
        assert cols["name"].max_value == "durian"
        assert cols["weight"].min_value is not None
        assert cols["weight"].max_value is not None

    def test_numeric_statistics(self) -> None:
        cols = {c.physical_name: c for c in ParquetEditor.open(FRUITS).columns()}
        assert int(cols["weight"].min_value) > 0  # type: ignore[arg-type]
        assert int(cols["weight"].max_value) > 0  # type: ignore[arg-type]

    def test_compression_present(self) -> None:
        cols = ParquetEditor.open(FRUITS).columns()
        for c in cols:
            assert isinstance(c.compression, str)
            assert len(c.compression) > 0

    def test_size_fields_positive(self) -> None:
        cols = ParquetEditor.open(FRUITS).columns()
        for c in cols:
            assert c.total_compressed_size > 0
            assert c.total_uncompressed_size > 0

    def test_tags_empty_by_default(self) -> None:
        cols = ParquetEditor.open(FRUITS).columns()
        for c in cols:
            assert c.tags == {}

    def test_returns_column_info_dataclass(self) -> None:
        cols = ParquetEditor.open(FRUITS).columns()
        assert all(isinstance(c, ColumnInfo) for c in cols)

    def test_field_id_none_when_not_set(self) -> None:
        cols = ParquetEditor.open(FRUITS).columns()
        for c in cols:
            assert c.field_id is None or isinstance(c.field_id, int)


class TestFileTags:
    def test_file_tags_empty_by_default(self) -> None:
        tags = ParquetEditor.open(FRUITS).file_tags()
        user_tags = {k: v for k, v in tags.items() if not k.startswith("ARROW")}
        assert user_tags == {}

    def test_set_file_tag_persists(self, fruits_copy: Path) -> None:
        editor = ParquetEditor.open(fruits_copy)
        editor.set_file_tag("owner", "data-team")
        tags = ParquetEditor.open(fruits_copy).file_tags()
        assert tags["owner"] == "data-team"

    def test_set_file_tag_to_different_path(self, tmp_path: Path) -> None:
        out = tmp_path / "out.parquet"
        editor = ParquetEditor.open(FRUITS)
        editor.set_file_tag("env", "prod", out_path=out)
        tags = ParquetEditor.open(out).file_tags()
        assert tags["env"] == "prod"
        assert not {k: v for k, v in ParquetEditor.open(FRUITS).file_tags().items() if k == "env"}

    def test_set_multiple_file_tags(self, fruits_copy: Path) -> None:
        editor = ParquetEditor.open(fruits_copy)
        editor.set_file_tag("owner", "alice")
        editor.set_file_tag("team", "analytics")
        tags = ParquetEditor.open(fruits_copy).file_tags()
        assert tags["owner"] == "alice"
        assert tags["team"] == "analytics"

    def test_update_existing_file_tag(self, fruits_copy: Path) -> None:
        editor = ParquetEditor.open(fruits_copy)
        editor.set_file_tag("owner", "alice")
        editor.set_file_tag("owner", "bob")
        tags = ParquetEditor.open(fruits_copy).file_tags()
        assert tags["owner"] == "bob"

    def test_remove_file_tag(self, fruits_copy: Path) -> None:
        editor = ParquetEditor.open(fruits_copy)
        editor.set_file_tag("owner", "alice")
        editor.set_file_tag("owner", None)
        tags = {
            k: v
            for k, v in ParquetEditor.open(fruits_copy).file_tags().items()
            if not k.startswith("ARROW")
        }
        assert "owner" not in tags

    def test_remove_nonexistent_tag_is_noop(self, fruits_copy: Path) -> None:
        editor = ParquetEditor.open(fruits_copy)
        editor.set_file_tag("nonexistent", None)

    def test_file_tag_special_characters(self, fruits_copy: Path) -> None:
        editor = ParquetEditor.open(fruits_copy)
        editor.set_file_tag("note", "hello world / test & stuff")
        tags = ParquetEditor.open(fruits_copy).file_tags()
        assert tags["note"] == "hello world / test & stuff"

    def test_file_tag_unicode(self, fruits_copy: Path) -> None:
        editor = ParquetEditor.open(fruits_copy)
        editor.set_file_tag("label", "数据集")
        tags = ParquetEditor.open(fruits_copy).file_tags()
        assert tags["label"] == "数据集"

    def test_file_returns_dict(self) -> None:
        tags = ParquetEditor.open(FRUITS).file_tags()
        assert isinstance(tags, dict)


class TestColumnTags:
    def test_set_column_tag_persists(self, fruits_copy: Path) -> None:
        editor = ParquetEditor.open(fruits_copy)
        editor.set_column_tag("weight", "unit", "grams")
        col = next(
            c for c in ParquetEditor.open(fruits_copy).columns() if c.path_in_schema == "weight"
        )
        assert col.tags.get("unit") == "grams"

    def test_set_column_tag_different_path(self, tmp_path: Path) -> None:
        out = tmp_path / "out.parquet"
        editor = ParquetEditor.open(FRUITS)
        editor.set_column_tag("name", "description", "fruit name", out_path=out)
        col = next(c for c in ParquetEditor.open(out).columns() if c.path_in_schema == "name")
        assert col.tags.get("description") == "fruit name"

    def test_set_multiple_column_tags(self, fruits_copy: Path) -> None:
        editor = ParquetEditor.open(fruits_copy)
        editor.set_column_tag("acidity", "unit", "pH")
        editor.set_column_tag("acidity", "range", "0-14")
        col = next(
            c for c in ParquetEditor.open(fruits_copy).columns() if c.path_in_schema == "acidity"
        )
        assert col.tags.get("unit") == "pH"
        assert col.tags.get("range") == "0-14"

    def test_update_existing_column_tag(self, fruits_copy: Path) -> None:
        editor = ParquetEditor.open(fruits_copy)
        editor.set_column_tag("color", "note", "old")
        editor.set_column_tag("color", "note", "new")
        col = next(
            c for c in ParquetEditor.open(fruits_copy).columns() if c.path_in_schema == "color"
        )
        assert col.tags.get("note") == "new"

    def test_remove_column_tag(self, fruits_copy: Path) -> None:
        editor = ParquetEditor.open(fruits_copy)
        editor.set_column_tag("weight", "unit", "grams")
        editor.set_column_tag("weight", "unit", None)
        col = next(
            c for c in ParquetEditor.open(fruits_copy).columns() if c.path_in_schema == "weight"
        )
        assert "unit" not in col.tags

    def test_column_tags_isolated_per_column(self, fruits_copy: Path) -> None:
        editor = ParquetEditor.open(fruits_copy)
        editor.set_column_tag("weight", "unit", "grams")
        cols = {c.path_in_schema: c for c in ParquetEditor.open(fruits_copy).columns()}
        assert "unit" not in cols["name"].tags
        assert "unit" not in cols["acidity"].tags
        assert cols["weight"].tags.get("unit") == "grams"

    def test_file_tags_unaffected_by_column_tag(self, fruits_copy: Path) -> None:
        editor = ParquetEditor.open(fruits_copy)
        editor.set_file_tag("project", "fruit-analysis")
        editor.set_column_tag("name", "key", "primary")
        tags = {
            k: v
            for k, v in ParquetEditor.open(fruits_copy).file_tags().items()
            if not k.startswith("ARROW")
        }
        assert tags.get("project") == "fruit-analysis"

    def test_column_tags_unaffected_by_file_tag(self, fruits_copy: Path) -> None:
        editor = ParquetEditor.open(fruits_copy)
        editor.set_column_tag("weight", "unit", "grams")
        editor.set_file_tag("extra", "value")
        col = next(
            c for c in ParquetEditor.open(fruits_copy).columns() if c.path_in_schema == "weight"
        )
        assert col.tags.get("unit") == "grams"


class TestFileIntegrity:
    def test_data_row_count_preserved(self, fruits_copy: Path) -> None:
        try:
            import pyarrow.parquet as pq
        except ImportError:
            pytest.skip("pyarrow not installed for integrity check")
        before = pq.read_metadata(str(fruits_copy)).num_rows
        editor = ParquetEditor.open(fruits_copy)
        editor.set_file_tag("test", "value")
        after = pq.read_metadata(str(fruits_copy)).num_rows
        assert before == after

    def test_file_remains_readable_after_edit(self, fruits_copy: Path) -> None:
        editor = ParquetEditor.open(fruits_copy)
        editor.set_file_tag("x", "1")
        editor.set_column_tag("name", "y", "2")
        reopened = ParquetEditor.open(fruits_copy)
        cols = reopened.columns()
        assert len(cols) == 4

    def test_column_stats_preserved_after_edit(self, fruits_copy: Path) -> None:
        before = {
            c.path_in_schema: (c.min_value, c.max_value)
            for c in ParquetEditor.open(fruits_copy).columns()
        }
        editor = ParquetEditor.open(fruits_copy)
        editor.set_file_tag("tag", "val")
        after = {
            c.path_in_schema: (c.min_value, c.max_value)
            for c in ParquetEditor.open(fruits_copy).columns()
        }
        assert before == after

    def test_inplace_edit_roundtrip(self, fruits_copy: Path) -> None:
        original_size = fruits_copy.stat().st_size
        editor = ParquetEditor.open(fruits_copy)
        editor.set_file_tag("k", "v")
        assert fruits_copy.exists()
        after_size = fruits_copy.stat().st_size
        assert abs(after_size - original_size) < 1024

    def test_original_unmodified_when_writing_to_new_path(self, tmp_path: Path) -> None:
        out = tmp_path / "out.parquet"
        editor = ParquetEditor.open(FRUITS)
        editor.set_file_tag("new_key", "new_val", out_path=out)
        orig_tags = {
            k: v
            for k, v in ParquetEditor.open(FRUITS).file_tags().items()
            if not k.startswith("ARROW")
        }
        assert "new_key" not in orig_tags
