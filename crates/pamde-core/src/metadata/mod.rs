/// Low-level operations on Parquet FileMetaData.
///
/// Responsibilities:
/// - Open a Parquet file and parse its footer (FileMetaData thrift struct).
/// - Expose all fields from the Parquet spec: schema, row_groups, key_value_metadata,
///   created_by, version, column_orders.
/// - Read and mutate file-level key_value_metadata (arbitrary user tags).
/// - Read and mutate column-level key_value_metadata stored in ColumnMetaData.
/// - Write back a modified footer to produce a new file (rewrite footer in-place or
///   write to a new path).
///
/// We use the `parquet` crate (from apache/arrow-rs) to parse the footer.
/// The thrift-encoded FileMetaData is accessed via `parquet::file::metadata::ParquetMetaData`.

use std::path::Path;

/// Represents the full metadata of a Parquet file as understood by pamde.
pub struct ParquetFileMeta {
    pub path: std::path::PathBuf,
    // TODO: wrap parquet::file::metadata::ParquetMetaData
}

impl ParquetFileMeta {
    /// Open a Parquet file and parse its footer.
    pub fn open<P: AsRef<Path>>(_path: P) -> Result<Self, crate::Error> {
        todo!("read parquet footer via parquet::file::footer::parse_metadata")
    }

    /// Return all file-level key/value metadata pairs.
    pub fn file_kv_metadata(&self) -> Vec<(String, Option<String>)> {
        todo!()
    }

    /// Upsert a file-level key/value metadata entry and write the modified footer.
    pub fn set_file_kv<P: AsRef<Path>>(
        &mut self,
        _key: &str,
        _value: Option<&str>,
        _out_path: P,
    ) -> Result<(), crate::Error> {
        todo!()
    }
}

pub use crate::Error;
