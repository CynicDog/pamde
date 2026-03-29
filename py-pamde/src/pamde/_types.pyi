"""
Type stubs for types exposed by the Rust extension (_pamde_runtime).
IDE-facing: these are not imported at runtime.
"""

class PyColumnInfo:
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
    column_kv_metadata: list[tuple[str, str | None]]

class PyParquetFile:
    @staticmethod
    def open(path: str) -> "PyParquetFile": ...
    def columns(self) -> list[PyColumnInfo]: ...
    def file_tags(self) -> dict[str, str | None]: ...
    def set_file_tag(
        self, key: str, value: str | None, out_path: str
    ) -> None: ...
    def set_column_tag(
        self, column_path: str, key: str, value: str | None, out_path: str
    ) -> None: ...
