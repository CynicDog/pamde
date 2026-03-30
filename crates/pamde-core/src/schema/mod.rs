use parquet::basic::Type as PhysicalType;

pub struct ColumnInfo {
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

fn bytes_to_display(bytes: &[u8], physical_type: PhysicalType) -> String {
    match physical_type {
        PhysicalType::INT32 if bytes.len() >= 4 => {
            i32::from_le_bytes(bytes[..4].try_into().unwrap()).to_string()
        }
        PhysicalType::INT64 if bytes.len() >= 8 => {
            i64::from_le_bytes(bytes[..8].try_into().unwrap()).to_string()
        }
        PhysicalType::FLOAT if bytes.len() >= 4 => {
            f32::from_le_bytes(bytes[..4].try_into().unwrap()).to_string()
        }
        PhysicalType::DOUBLE if bytes.len() >= 8 => {
            f64::from_le_bytes(bytes[..8].try_into().unwrap()).to_string()
        }
        PhysicalType::BOOLEAN if !bytes.is_empty() => {
            if bytes[0] != 0 { "true" } else { "false" }.to_string()
        }
        PhysicalType::BYTE_ARRAY | PhysicalType::FIXED_LEN_BYTE_ARRAY => {
            std::str::from_utf8(bytes)
                .map(|s| s.to_string())
                .unwrap_or_else(|_| {
                    bytes.iter().fold("0x".to_string(), |mut s, b| {
                        s.push_str(&format!("{b:02x}"));
                        s
                    })
                })
        }
        _ => bytes.iter().fold("0x".to_string(), |mut s, b| {
            s.push_str(&format!("{b:02x}"));
            s
        }),
    }
}

impl ColumnInfo {
    pub fn from_file_meta(
        meta: &crate::metadata::ParquetFileMeta,
    ) -> Result<Vec<Self>, crate::Error> {
        let parquet_meta = &meta.meta;
        let schema_descr = parquet_meta.file_metadata().schema_descr();
        let num_columns = schema_descr.num_columns();
        let row_groups = parquet_meta.row_groups();
        let format_rgs = &meta.format_meta.row_groups;

        let mut result = Vec::with_capacity(num_columns);

        for col_idx in 0..num_columns {
            let col_descr = schema_descr.column(col_idx);

            let physical_name = col_descr.name().to_string();
            let path_in_schema = col_descr.path().parts().join(".");
            let physical_type = col_descr.physical_type();
            let physical_type_str = format!("{physical_type:?}");

            let logical_type = col_descr.logical_type().map(|lt| format!("{lt:?}"));

            let schema_type = col_descr.self_type_ptr();
            let basic_info = schema_type.get_basic_info();
            let repetition = format!("{:?}", basic_info.repetition());

            let field_id: Option<i32> = if basic_info.has_id() {
                Some(basic_info.id())
            } else {
                None
            };

            let mut total_null_count: Option<i64> = Some(0);
            let mut total_distinct_count: Option<i64> = None;
            let mut min_val: Option<String> = None;
            let mut max_val: Option<String> = None;
            let mut total_compressed: i64 = 0;
            let mut total_uncompressed: i64 = 0;
            let mut compression_str = "UNCOMPRESSED".to_string();

            for rg in row_groups {
                if col_idx >= rg.num_columns() {
                    continue;
                }
                let col_chunk = rg.column(col_idx);

                total_compressed += col_chunk.compressed_size();
                total_uncompressed += col_chunk.uncompressed_size();
                compression_str = format!("{}", col_chunk.compression());

                match col_chunk.statistics() {
                    Some(stats) => {
                        match stats.null_count_opt() {
                            Some(nc) => {
                                if let Some(ref mut acc) = total_null_count {
                                    *acc += nc as i64;
                                }
                            }
                            None => total_null_count = None,
                        }

                        if let Some(dc) = stats.distinct_count_opt() {
                            let dc = dc as i64;
                            total_distinct_count =
                                Some(total_distinct_count.map_or(dc, |prev| prev.max(dc)));
                        }

                        if min_val.is_none() {
                            if let Some(b) = stats.min_bytes_opt() {
                                min_val = Some(bytes_to_display(b, physical_type));
                            }
                        }
                        if max_val.is_none() {
                            if let Some(b) = stats.max_bytes_opt() {
                                max_val = Some(bytes_to_display(b, physical_type));
                            }
                        }
                    }
                    None => total_null_count = None,
                }
            }

            // Column KV metadata: take the first non-empty match across all row groups.
            let mut column_kv_metadata: Vec<(String, Option<String>)> = Vec::new();

            'outer: for fmt_rg in format_rgs {
                for fmt_col in &fmt_rg.columns {
                    if let Some(ref col_meta) = fmt_col.meta_data {
                        if col_meta.path_in_schema.join(".") == path_in_schema {
                            if let Some(ref kvs) = col_meta.key_value_metadata {
                                if !kvs.is_empty() {
                                    column_kv_metadata = kvs
                                        .iter()
                                        .map(|kv| (kv.key.clone(), kv.value.clone()))
                                        .collect();
                                    break 'outer;
                                }
                            }
                        }
                    }
                }
            }

            result.push(ColumnInfo {
                physical_name,
                path_in_schema,
                physical_type: physical_type_str,
                logical_type,
                repetition,
                field_id,
                null_count: total_null_count,
                distinct_count: total_distinct_count,
                min_value: min_val,
                max_value: max_val,
                compression: compression_str,
                total_compressed_size: total_compressed,
                total_uncompressed_size: total_uncompressed,
                column_kv_metadata,
            });
        }

        Ok(result)
    }
}
