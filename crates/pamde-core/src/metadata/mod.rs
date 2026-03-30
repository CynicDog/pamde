use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;

use parquet::file::metadata::ParquetMetaData;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::thrift::TSerializable;
use thrift::protocol::{TCompactInputProtocol, TCompactOutputProtocol};

const PARQUET_MAGIC: &[u8; 4] = b"PAR1";

pub struct ParquetFileMeta {
    pub path: std::path::PathBuf,
    pub(crate) meta: Arc<ParquetMetaData>,
    pub(crate) format_meta: parquet::format::FileMetaData,
}

fn read_footer_bytes(path: &Path) -> Result<Vec<u8>, crate::Error> {
    let mut file = File::open(path)?;
    let file_size = file.seek(SeekFrom::End(0))? as usize;

    if file_size < 12 {
        return Err(crate::Error::InvalidMetadata(
            "file too small to be a valid Parquet file".into(),
        ));
    }

    let mut tail = [0u8; 8];
    file.seek(SeekFrom::Start((file_size - 8) as u64))?;
    file.read_exact(&mut tail)?;

    if &tail[4..] != PARQUET_MAGIC {
        return Err(crate::Error::InvalidMetadata(
            "missing PAR1 magic bytes at end of file".into(),
        ));
    }

    let footer_len = u32::from_le_bytes(tail[..4].try_into().unwrap()) as usize;
    if footer_len + 8 > file_size {
        return Err(crate::Error::InvalidMetadata(
            "footer length exceeds file size".into(),
        ));
    }

    let footer_start = file_size - 8 - footer_len;
    let mut footer_bytes = vec![0u8; footer_len];
    file.seek(SeekFrom::Start(footer_start as u64))?;
    file.read_exact(&mut footer_bytes)?;

    Ok(footer_bytes)
}

fn decode_format_meta(
    footer_bytes: &[u8],
) -> Result<parquet::format::FileMetaData, crate::Error> {
    let mut cursor = std::io::Cursor::new(footer_bytes);
    let mut prot = TCompactInputProtocol::new(&mut cursor);
    parquet::format::FileMetaData::read_from_in_protocol(&mut prot)
        .map_err(|e| crate::Error::Parquet(format!("thrift decode error: {e}")))
}

fn encode_format_meta(
    meta: &parquet::format::FileMetaData,
) -> Result<Vec<u8>, crate::Error> {
    let mut buf = Vec::new();
    let mut prot = TCompactOutputProtocol::new(&mut buf);
    meta.write_to_out_protocol(&mut prot)
        .map_err(|e| crate::Error::Parquet(format!("thrift encode error: {e}")))?;
    Ok(buf)
}

fn write_modified_file(
    in_path: &Path,
    out_path: &Path,
    new_footer_bytes: &[u8],
) -> Result<(), crate::Error> {
    let mut in_file = File::open(in_path)?;
    let file_size = in_file.seek(SeekFrom::End(0))? as usize;

    let mut tail = [0u8; 8];
    in_file.seek(SeekFrom::Start((file_size - 8) as u64))?;
    in_file.read_exact(&mut tail)?;
    let old_footer_len = u32::from_le_bytes(tail[..4].try_into().unwrap()) as usize;
    let data_end = file_size - 8 - old_footer_len;

    let in_place = in_path == out_path;
    let write_target = if in_place {
        out_path.with_extension("pamde_tmp")
    } else {
        out_path.to_path_buf()
    };

    {
        let mut out_file = File::create(&write_target)?;

        in_file.seek(SeekFrom::Start(0))?;
        let mut reader = BufReader::new((&mut in_file).take(data_end as u64));
        std::io::copy(&mut reader, &mut out_file)?;

        out_file.write_all(new_footer_bytes)?;

        let new_footer_len = new_footer_bytes.len() as u32;
        out_file.write_all(&new_footer_len.to_le_bytes())?;
        out_file.write_all(PARQUET_MAGIC)?;
        out_file.flush()?;
    }

    if in_place {
        std::fs::rename(&write_target, out_path)?;
    }

    Ok(())
}

fn open_reader(path: &Path) -> Result<Arc<ParquetMetaData>, crate::Error> {
    let file = File::open(path)?;
    let reader =
        SerializedFileReader::new(file).map_err(|e| crate::Error::Parquet(e.to_string()))?;
    Ok(Arc::new(reader.metadata().clone()))
}

impl ParquetFileMeta {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, crate::Error> {
        let path = path.as_ref().to_path_buf();
        let meta = open_reader(&path)?;
        let footer_bytes = read_footer_bytes(&path)?;
        let format_meta = decode_format_meta(&footer_bytes)?;
        Ok(Self { path, meta, format_meta })
    }

    pub fn file_kv_metadata(&self) -> Vec<(String, Option<String>)> {
        self.format_meta
            .key_value_metadata
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .map(|kv| (kv.key.clone(), kv.value.clone()))
            .collect()
    }

    pub fn set_file_kv<P: AsRef<Path>>(
        &mut self,
        key: &str,
        value: Option<&str>,
        out_path: P,
    ) -> Result<(), crate::Error> {
        let out_path = out_path.as_ref();
        let mut format_meta = self.format_meta.clone();

        let kvs = format_meta.key_value_metadata.get_or_insert_with(Vec::new);
        if let Some(v) = value {
            if let Some(existing) = kvs.iter_mut().find(|kv| kv.key == key) {
                existing.value = Some(v.to_string());
            } else {
                kvs.push(parquet::format::KeyValue {
                    key: key.to_string(),
                    value: Some(v.to_string()),
                });
            }
        } else {
            kvs.retain(|kv| kv.key != key);
        }

        let new_footer = encode_format_meta(&format_meta)?;
        write_modified_file(&self.path, out_path, &new_footer)?;

        self.format_meta = format_meta;
        self.meta = open_reader(out_path)?;
        self.path = out_path.to_path_buf();

        Ok(())
    }

    pub fn set_column_kv<P: AsRef<Path>>(
        &mut self,
        column_path: &str,
        key: &str,
        value: Option<&str>,
        out_path: P,
    ) -> Result<(), crate::Error> {
        self.set_column_kvs(&[(column_path.to_string(), key.to_string(), value.map(|v| v.to_string()))], out_path)
    }

    /// Apply multiple column tag mutations in a single footer write.
    pub fn set_column_kvs<P: AsRef<Path>>(
        &mut self,
        updates: &[(String, String, Option<String>)],
        out_path: P,
    ) -> Result<(), crate::Error> {
        let out_path = out_path.as_ref();
        let mut format_meta = self.format_meta.clone();

        for (column_path, key, value) in updates {
            let target_parts: Vec<String> =
                column_path.split('.').map(|s| s.to_string()).collect();

            for rg in &mut format_meta.row_groups {
                for col in &mut rg.columns {
                    if let Some(ref mut col_meta) = col.meta_data {
                        if col_meta.path_in_schema == target_parts {
                            let kvs =
                                col_meta.key_value_metadata.get_or_insert_with(Vec::new);
                            if let Some(v) = value {
                                if let Some(existing) =
                                    kvs.iter_mut().find(|kv| kv.key == *key)
                                {
                                    existing.value = Some(v.clone());
                                } else {
                                    kvs.push(parquet::format::KeyValue {
                                        key: key.clone(),
                                        value: Some(v.clone()),
                                    });
                                }
                            } else {
                                kvs.retain(|kv| kv.key != *key);
                            }
                        }
                    }
                }
            }
        }

        let new_footer = encode_format_meta(&format_meta)?;
        write_modified_file(&self.path, out_path, &new_footer)?;

        self.format_meta = format_meta;
        self.meta = open_reader(out_path)?;
        self.path = out_path.to_path_buf();

        Ok(())
    }
}
