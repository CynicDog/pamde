pub mod metadata;
pub mod schema;

#[cfg(feature = "extension-module")]
pub mod c_api;

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    Parquet(String),
    InvalidMetadata(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Io(e) => write!(f, "IO error: {e}"),
            Error::Parquet(msg) => write!(f, "Parquet error: {msg}"),
            Error::InvalidMetadata(msg) => write!(f, "Invalid metadata: {msg}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

impl From<parquet::errors::ParquetError> for Error {
    fn from(e: parquet::errors::ParquetError) -> Self {
        Error::Parquet(e.to_string())
    }
}

#[cfg(feature = "extension-module")]
impl From<Error> for pyo3::PyErr {
    fn from(e: Error) -> Self {
        pyo3::exceptions::PyRuntimeError::new_err(e.to_string())
    }
}
