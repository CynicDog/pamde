/// PyO3 bindings — exposes pamde-core types to Python.
///
/// Naming convention: Rust types get a `Py` prefix when exposed to Python,
/// mirroring the pattern in the code structure example.
///
/// This module is only compiled when the `extension-module` feature is enabled
/// (i.e. when building the pamde-runtime maturin package).

use pyo3::prelude::*;

#[pyclass(get_all)]
pub struct PyColumnInfo {
    pub physical_name: String,
    pub path_in_schema: String,
    pub physical_type: String,
    pub logical_type: Option<String>,
    pub repetition: String,
    pub field_id: Option<i32>,
    pub null_count: Option<i64>,
    pub distinct_count: Option<i64>,
    pub min_value: Option<String>,
    pub max_value: Option<String>,
    pub compression: String,
    pub total_compressed_size: i64,
    pub total_uncompressed_size: i64,
    pub column_kv_metadata: Vec<(String, Option<String>)>,
}

/// Entry-point type exposed to Python.
/// Wraps the Rust ParquetFileMeta and provides column/metadata access.
#[pyclass]
pub struct PyParquetFile {
    // inner: crate::metadata::ParquetFileMeta,
}

#[pymethods]
impl PyParquetFile {
    #[new]
    pub fn open(_path: &str) -> PyResult<Self> {
        todo!("open parquet file and return PyParquetFile")
    }

    /// Return list of PyColumnInfo, one per leaf column.
    pub fn columns(&self) -> PyResult<Vec<PyColumnInfo>> {
        todo!()
    }

    /// Return file-level key/value metadata as a dict {key: value | None}.
    /// Matches the pyarrow backend interface so editor.py handles both uniformly.
    pub fn file_tags(&self) -> PyResult<std::collections::HashMap<String, Option<String>>> {
        todo!()
    }

    /// Upsert a file-level metadata key.  Pass value=None to remove the key.
    pub fn set_file_tag(
        &mut self,
        _key: &str,
        _value: Option<&str>,
        _out_path: &str,
    ) -> PyResult<()> {
        todo!()
    }

    /// Upsert a column-level metadata key for a column identified by path_in_schema.
    pub fn set_column_tag(
        &mut self,
        _column_path: &str,
        _key: &str,
        _value: Option<&str>,
        _out_path: &str,
    ) -> PyResult<()> {
        todo!()
    }
}

#[pymodule]
pub fn _pamde_runtime(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyParquetFile>()?;
    m.add_class::<PyColumnInfo>()?;
    Ok(())
}
