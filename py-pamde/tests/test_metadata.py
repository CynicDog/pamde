"""
Integration tests for pamde metadata operations.
These tests require pamde-runtime to be built (maturin develop).
"""

import pytest
from pathlib import Path

# from pamde import ParquetEditor

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.mark.skip(reason="pamde-runtime not yet built")
def test_open_file() -> None:
    # editor = ParquetEditor.open(FIXTURES / "fruits.parquet")
    # assert len(editor.columns()) == 4
    pass


@pytest.mark.skip(reason="pamde-runtime not yet built")
def test_set_file_tag(tmp_path: Path) -> None:
    # editor = ParquetEditor.open(FIXTURES / "fruits.parquet")
    # out = tmp_path / "fruits_tagged.parquet"
    # editor.set_file_tag("owner", "data-team", out_path=out)
    # assert ParquetEditor.open(out).file_tags()["owner"] == "data-team"
    pass


@pytest.mark.skip(reason="pamde-runtime not yet built")
def test_set_column_tag(tmp_path: Path) -> None:
    pass
