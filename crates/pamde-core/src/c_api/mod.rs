use std::collections::HashMap;

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

#[pyclass]
pub struct PyParquetFile {
    inner: crate::metadata::ParquetFileMeta,
}

#[pymethods]
impl PyParquetFile {
    #[new]
    pub fn open(path: &str) -> PyResult<Self> {
        let inner = crate::metadata::ParquetFileMeta::open(path)?;
        Ok(Self { inner })
    }

    pub fn columns(&self) -> PyResult<Vec<PyColumnInfo>> {
        let cols = crate::schema::ColumnInfo::from_file_meta(&self.inner)?;
        Ok(cols
            .into_iter()
            .map(|c| PyColumnInfo {
                physical_name: c.physical_name,
                path_in_schema: c.path_in_schema,
                physical_type: c.physical_type,
                logical_type: c.logical_type,
                repetition: c.repetition,
                field_id: c.field_id,
                null_count: c.null_count,
                distinct_count: c.distinct_count,
                min_value: c.min_value,
                max_value: c.max_value,
                compression: c.compression,
                total_compressed_size: c.total_compressed_size,
                total_uncompressed_size: c.total_uncompressed_size,
                column_kv_metadata: c.column_kv_metadata,
            })
            .collect())
    }

    pub fn file_tags(&self) -> PyResult<HashMap<String, Option<String>>> {
        Ok(self.inner.file_kv_metadata().into_iter().collect())
    }

    pub fn set_file_tag(
        &mut self,
        key: &str,
        value: Option<&str>,
        out_path: &str,
    ) -> PyResult<()> {
        self.inner.set_file_kv(key, value, out_path)?;
        Ok(())
    }

    pub fn set_column_tag(
        &mut self,
        column_path: &str,
        key: &str,
        value: Option<&str>,
        out_path: &str,
    ) -> PyResult<()> {
        self.inner.set_column_kv(column_path, key, value, out_path)?;
        Ok(())
    }

    /// Apply multiple column tag mutations with a single footer write.
    /// updates: list of (column_path, key, value) tuples; value=None removes the tag.
    pub fn set_column_tags_batch(
        &mut self,
        updates: Vec<(String, String, Option<String>)>,
        out_path: &str,
    ) -> PyResult<()> {
        self.inner.set_column_kvs(&updates, out_path)?;
        Ok(())
    }
}

#[pymodule]
pub fn _pamde_runtime(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyParquetFile>()?;
    m.add_class::<PyColumnInfo>()?;
    Ok(())
}
