/// Schema introspection helpers.
///
/// Parquet stores schema as a flattened depth-first list of SchemaElement nodes.
/// This module provides:
/// - A typed view of each leaf column: physical type, logical type, repetition,
///   field_id, converted_type (deprecated but still present in many files).
/// - Statistics per column per row-group: null_count, distinct_count, min/max values.
/// - Encoding and compression info per column chunk.
/// - Size statistics (unencoded byte size, repetition/definition histograms).
///
/// Column info is what drives the rows of the UI table.

/// All information about a single Parquet leaf column, aggregated across row groups.
pub struct ColumnInfo {
    /// Physical column name (SchemaElement.name)
    pub physical_name: String,
    /// Dot-separated path in the schema tree (e.g. "address.street")
    pub path_in_schema: String,
    /// Physical type (BOOLEAN, INT32, INT64, FLOAT, DOUBLE, BYTE_ARRAY, …)
    pub physical_type: String,
    /// Logical / converted type annotation if present
    pub logical_type: Option<String>,
    /// Repetition: REQUIRED | OPTIONAL | REPEATED
    pub repetition: String,
    /// field_id (used by Iceberg and other table formats)
    pub field_id: Option<i32>,
    /// Aggregated null count across all row groups (None if unknown)
    pub null_count: Option<i64>,
    /// Aggregated distinct count across all row groups (None if unknown)
    pub distinct_count: Option<i64>,
    /// Min value as a human-readable string (best-effort)
    pub min_value: Option<String>,
    /// Max value as a human-readable string (best-effort)
    pub max_value: Option<String>,
    /// Compression codec
    pub compression: String,
    /// Total compressed size in bytes across all row groups
    pub total_compressed_size: i64,
    /// Total uncompressed size in bytes across all row groups
    pub total_uncompressed_size: i64,
    /// Per-column key/value metadata from ColumnMetaData (if any)
    pub column_kv_metadata: Vec<(String, Option<String>)>,
}

impl ColumnInfo {
    /// Build ColumnInfo list from a parsed ParquetFileMeta.
    pub fn from_file_meta(
        _meta: &crate::metadata::ParquetFileMeta,
    ) -> Result<Vec<Self>, crate::Error> {
        todo!("iterate schema elements + row groups to build ColumnInfo list")
    }
}
