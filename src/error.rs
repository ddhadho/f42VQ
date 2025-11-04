use thiserror::Error;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("Corrupted data: {0}")]
    Corruption(String),
    
    #[error("Corrupted data: {0}")]
    CorruptedData(String), 
    
    #[error("Invalid format: {0}")]
    InvalidFormat(String),
    
    #[error("Key not found")]
    KeyNotFound,
}

pub type Result<T> = std::result::Result<T, StorageError>;