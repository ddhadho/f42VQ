pub mod error;
pub mod wal;
// pub mod memtable;  // Uncomment later
// pub mod sstable;   // Uncomment later

// Re-export commonly used types
pub use error::{Result, StorageError};

// Core types that everything uses
pub type Key = Vec<u8>;
pub type Value = Vec<u8>;
pub type Timestamp = u64;  // Unix timestamp in seconds

/// Entry in the storage system
#[derive(Debug, Clone, PartialEq)]
pub struct Entry {
    pub key: Key,
    pub value: Value,
    pub timestamp: Timestamp,
}

impl Entry {
    pub fn new(key: Key, value: Value, timestamp: Timestamp) -> Self {
        Entry { key, value, timestamp }
    }
}

/// Operation types for WAL
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum OpType {
    Put = 1,
    Delete = 2,
}

impl OpType {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(OpType::Put),
            2 => Some(OpType::Delete),
            _ => None,
        }
    }
}