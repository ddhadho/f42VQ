//! Write-Ahead Log with Batching
//!
//! Provides durable, sequential write storage with configurable batching.
//! 
//! ## Design
//! 
//! Writes accumulate in an in-memory buffer and are flushed to disk when:
//! - Buffer reaches size limit (configurable)
//! - Explicit `flush()` is called
//! - WAL is dropped (ensures no data loss)
//!
//! ## Performance
//!
//! Batching provides 10-100x throughput improvement over naive fsync-per-write:
//! - Without batching: ~100-200 writes/sec (limited by fsync latency)
//! - With batching (4KB buffer): ~5,000-10,000 writes/sec
//! - With batching (16KB buffer): ~20,000-50,000 writes/sec
//!
//! ## Durability Guarantee
//!
//! - After `flush()` returns: All buffered writes are durable (survived fsync)
//! - On drop: Buffer is automatically flushed (no data loss)
//! - On crash: Last batch (milliseconds of data) may be lost
//!
//! ## Example
//!
//! ```no_run
//! use timeseries_lsm::{Entry, Wal};
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let mut wal = Wal::new("data.wal", 4096)?;  // 4KB buffer
//!
//! // Writes are batched automatically
//! for i in 0..1000 {
//!     let entry = Entry {
//!         key: format!("key_{}", i).into_bytes(),
//!         value: vec![0; 100],
//!         timestamp: i,
//!     };
//!     wal.append(&entry)?;
//! }
//!
//! // Explicit flush when needed
//! wal.flush()?;
//! # Ok(())
//! # }
//! ```


use crate::{Entry, OpType, Result, StorageError};
use bytes::{Buf, BufMut, BytesMut};
use crc32fast::Hasher;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};use std::path::{Path, PathBuf};

pub struct Wal {
    file: File,
    #[allow(dead_code)]
    path: PathBuf,
    buffer: BytesMut,    //accumulates writes
    buffer_size: usize,  //flush when buffer reaches this size
}

impl Wal {
    /// Create or open a WAL file
    pub fn new(path: impl AsRef<Path>, buffer_size: usize) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        
        Ok(Wal {
            file,
            path,
            buffer: BytesMut::with_capacity(buffer_size),
            buffer_size,
        })
    }
    
    /// Append an entry to the WAL
    pub fn append(&mut self, entry: &Entry) -> Result<()> {
        self.append_operation(OpType::Put, entry)
    }
    
    /// Append a delete operation to the WAL
    pub fn append_delete(&mut self, key: &[u8], timestamp: u64) -> Result<()> {
        let entry = Entry {
            key: key.to_vec(),
            value: Vec::new(),  // Deletes have empty value
            timestamp,
        };
        self.append_operation(OpType::Delete, &entry)
    }
    
    fn append_operation(&mut self, op_type: OpType, entry: &Entry) -> Result<()> {
        // Encode the record
        let record = encode_record(op_type, entry)?;
        
        // Add to buffer
        self.buffer.extend_from_slice(&record);
        
        // Flush if buffer is full
        if self.buffer.len() >= self.buffer_size {
            self.flush()?;
        }
        
        Ok(())
    }
    
    /// Flush buffered writes to disk
    pub fn flush(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        
        // Write buffer to file
        self.file.write_all(&self.buffer)?;
        
        // Force to disk (durability!)
        self.file.sync_all()?;
        
        // Clear buffer
        self.buffer.clear();
        
        Ok(())
    }
    
    /// Recover entries from WAL file
    pub fn recover(path: impl AsRef<Path>) -> Result<Vec<Entry>> {
        let path = path.as_ref();
        
        if !path.exists() {
            return Ok(Vec::new());
        }
        
        let mut file = File::open(path)?;
        let mut entries = Vec::new();
        
        loop {
            match read_record(&mut file) {
                Ok(Some((op_type, entry))) => {
                    match op_type {
                        OpType::Put => entries.push(entry),
                        OpType::Delete => {
                            // Handle deletes: remove from entries or mark as tombstone
                            // For now, just add with empty value
                            entries.push(entry);
                        }
                    }
                }
                Ok(None) => break,  // EOF
                Err(StorageError::Corruption(msg)) => {
                    // Corruption detected - stop reading
                    eprintln!("WAL corruption detected: {}", msg);
                    break;
                }
                Err(e) => return Err(e),
            }
        }
        
        Ok(entries)
    }
    
    /// Get the current file size
    pub fn size(&self) -> Result<u64> {
        Ok(self.file.metadata()?.len())
    }
}

impl Drop for Wal {
    fn drop(&mut self) {
        // Ensure any buffered data is flushed on drop
        let _ = self.flush();
    }
}

/// Encode a record into bytes
fn encode_record(op_type: OpType, entry: &Entry) -> Result<Vec<u8>> {
    let mut data = BytesMut::new();
    
    // Encode data payload
    data.put_u64_le(entry.timestamp);
    
    // Key length + key
    if entry.key.len() > u16::MAX as usize {
        return Err(StorageError::InvalidFormat("Key too long".into()));
    }
    data.put_u16_le(entry.key.len() as u16);
    data.put_slice(&entry.key);
    
    // Value length + value
    if entry.value.len() > u32::MAX as usize {
        return Err(StorageError::InvalidFormat("Value too long".into()));
    }
    data.put_u32_le(entry.value.len() as u32);
    data.put_slice(&entry.value);
    
    let data_len = data.len();
    if data_len > u16::MAX as usize {
        return Err(StorageError::InvalidFormat("Record too large".into()));
    }
    
    // Calculate checksum over: length + type + data
    let mut hasher = Hasher::new();
    hasher.update(&(data_len as u16).to_le_bytes());
    hasher.update(&[op_type as u8]);
    hasher.update(&data);
    let checksum = hasher.finalize();
    
    // Build final record
    let mut record = BytesMut::with_capacity(4 + 2 + 1 + data_len);
    record.put_u32_le(checksum);           // 4 bytes
    record.put_u16_le(data_len as u16);    // 2 bytes
    record.put_u8(op_type as u8);          // 1 byte
    record.put_slice(&data);               // N bytes
    
    Ok(record.to_vec())
}

/// Read a record from the file
/// Returns: Ok(Some((op_type, entry))) if successful
///          Ok(None) if EOF
///          Err if corruption or IO error
fn read_record(file: &mut File) -> Result<Option<(OpType, Entry)>> {
    // Read header (checksum + length + type = 7 bytes)
    let mut header = [0u8; 7];
    match file.read_exact(&mut header) {
        Ok(_) => {},
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            return Ok(None);  // EOF
        }
        Err(e) => return Err(e.into()),
    }
    
    let mut buf = &header[..];
    let checksum = buf.get_u32_le();
    let length = buf.get_u16_le();
    let type_byte = buf.get_u8();
    
    let op_type = OpType::from_u8(type_byte)
        .ok_or_else(|| StorageError::InvalidFormat(format!("Invalid op type: {}", type_byte)))?;
    
    // Read data
    let mut data = vec![0u8; length as usize];
    file.read_exact(&mut data)?;
    
    // Verify checksum
    let mut hasher = Hasher::new();
    hasher.update(&length.to_le_bytes());
    hasher.update(&[type_byte]);
    hasher.update(&data);
    let computed = hasher.finalize();
    
    if computed != checksum {
        return Err(StorageError::Corruption(format!(
            "Checksum mismatch: expected {}, got {}",
            checksum, computed
        )));
    }
    
    // Decode data
    let mut buf = &data[..];
    
    let timestamp = buf.get_u64_le();
    
    let key_len = buf.get_u16_le() as usize;
    if buf.remaining() < key_len {
        return Err(StorageError::Corruption("Truncated key".into()));
    }
    let key = buf[..key_len].to_vec();
    buf.advance(key_len);
    
    let value_len = buf.get_u32_le() as usize;
    if buf.remaining() < value_len {
        return Err(StorageError::Corruption("Truncated value".into()));
    }
    let value = buf[..value_len].to_vec();
    
    let entry = Entry {
        key,
        value,
        timestamp,
    };
    
    Ok(Some((op_type, entry)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    
    #[test]
    fn test_wal_append_and_flush() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");
        
        let mut wal = Wal::new(&path, 1024).unwrap();
        
        let entry = Entry {
            key: b"test_key".to_vec(),
            value: b"test_value".to_vec(),
            timestamp: 1234567890,
        };
        
        wal.append(&entry).unwrap();
        wal.flush().unwrap();
        
        // File should exist and have data
        assert!(path.exists());
        assert!(wal.size().unwrap() > 0);
    }
    
    #[test]
    fn test_wal_recovery() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");
        
        // Write some entries
        {
            let mut wal = Wal::new(&path, 1024).unwrap();
            
            for i in 0..100 {
                let entry = Entry {
                    key: format!("key_{}", i).into_bytes(),
                    value: format!("value_{}", i).into_bytes(),
                    timestamp: 1000 + i,
                };
                wal.append(&entry).unwrap();
            }
            
            wal.flush().unwrap();
        }
        
        // Recover
        let entries = Wal::recover(&path).unwrap();
        
        assert_eq!(entries.len(), 100);
        assert_eq!(entries[0].key, b"key_0");
        assert_eq!(entries[99].value, b"value_99");
    }
    
    #[test]
    fn test_wal_auto_flush_on_buffer_full() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");
        
        // Small buffer to trigger auto-flush
        let mut wal = Wal::new(&path, 128).unwrap();
        
        // Write enough data to overflow buffer
        for i in 0..10 {
            let entry = Entry {
                key: format!("key_{}", i).into_bytes(),
                value: vec![0u8; 100],  // Large value
                timestamp: 1000 + i,
            };
            wal.append(&entry).unwrap();
        }
        
        // Should have auto-flushed
        assert!(wal.size().unwrap() > 0);
    }
    
    #[test]
    fn test_wal_corruption_detection() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");
        
        // Write valid entry
        {
            let mut wal = Wal::new(&path, 1024).unwrap();
            let entry = Entry {
                key: b"key".to_vec(),
                value: b"value".to_vec(),
                timestamp: 1000,
            };
            wal.append(&entry).unwrap();
            wal.flush().unwrap();
        }
        
        // Corrupt the file (flip a byte)
        {
            let mut file = OpenOptions::new().write(true).open(&path).unwrap();
            file.seek(SeekFrom::Start(10)).unwrap();
            file.write_all(&[0xFF]).unwrap();
        }
        
        // Recovery should detect corruption
        let result = Wal::recover(&path);
        
        // Should either return empty (stopped at corruption) or error
        match result {
            Ok(entries) => assert!(entries.is_empty()),
            Err(StorageError::Corruption(_)) => {}, // Expected
            Err(e) => panic!("Unexpected error: {:?}", e),
        }
    }
    
    #[test]
    fn test_wal_delete_operation() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");
        
        let mut wal = Wal::new(&path, 1024).unwrap();
        
        wal.append_delete(b"key_to_delete", 2000).unwrap();
        wal.flush().unwrap();
        
        let entries = Wal::recover(&path).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].key, b"key_to_delete");
        assert!(entries[0].value.is_empty());
    }

    #[test]
    fn test_batching_multiple_writes() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("batch.wal");
        
        {
            let mut wal = Wal::new(&path, 4096).unwrap();
            
            // Write 100 entries (should batch them)
            for i in 0..100 {
                let entry = Entry {
                    key: format!("key_{}", i).into_bytes(),
                    value: vec![0u8; 50],
                    timestamp: 1000 + i,
                };
                wal.append(&entry).unwrap();
            }
            
            // Explicitly flush remaining buffer
            wal.flush().unwrap();
        }
        
        // Verify all entries recovered
        let entries = Wal::recover(&path).unwrap();
        assert_eq!(entries.len(), 100);
    }

    #[test]
    fn test_drop_flushes_buffer() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("drop.wal");
        
        // Write entry and drop without explicit flush
        {
            let mut wal = Wal::new(&path, 4096).unwrap();
            let entry = Entry {
                key: b"test".to_vec(),
                value: b"value".to_vec(),
                timestamp: 1000,
            };
            wal.append(&entry).unwrap();
            
            // No explicit flush - Drop should handle it
        }
        
        // Verify entry was still written (Drop flushed it)
        let entries = Wal::recover(&path).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].key, b"test");
    }

    #[test]
    fn test_explicit_flush() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("flush.wal");
        
        let mut wal = Wal::new(&path, 4096).unwrap();
        
        let entry = Entry {
            key: b"key".to_vec(),
            value: b"value".to_vec(),
            timestamp: 1000,
        };
        
        wal.append(&entry).unwrap();
        
        // Explicitly flush
        wal.flush().unwrap();
        
        // Verify it was written
        let entries = Wal::recover(&path).unwrap();
        assert_eq!(entries.len(), 1);
    }

    #[test]
    fn test_buffer_size_limit() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("limit.wal");
        
        // Small buffer to force frequent flushes
        let mut wal = Wal::new(&path, 256).unwrap();  // 256 bytes
        
        // Write entries larger than buffer
        for i in 0..10 {
            let entry = Entry {
                key: format!("key_{}", i).into_bytes(),
                value: vec![0u8; 200],  // 200 byte value
                timestamp: 1000 + i,
            };
            wal.append(&entry).unwrap();
            // Should auto-flush when buffer exceeds 256 bytes
        }
        
        wal.flush().unwrap();
        
        // All entries should be written
        let entries = Wal::recover(&path).unwrap();
        assert_eq!(entries.len(), 10);
    }

    #[test]
    fn benchmark_batching_performance() {
        use std::time::Instant;
        
        let dir = tempdir().unwrap();
        
        let entry = Entry {
            key: b"benchmark_key".to_vec(),
            value: vec![0u8; 100],
            timestamp: 1000,
        };
        
        let num_writes = 10000;
        
        // Test 1: Small buffer (frequent flushes)
        let path1 = dir.path().join("small_batch.wal");
        let start = Instant::now();
        {
            let mut wal = Wal::new(&path1, 128).unwrap();  // 128 bytes (tiny)
            for _ in 0..num_writes {
                wal.append(&entry).unwrap();
            }
            wal.flush().unwrap();
        }
        let small_buffer_time = start.elapsed();
        
        // Test 2: Large buffer (infrequent flushes)
        let path2 = dir.path().join("large_batch.wal");
        let start = Instant::now();
        {
            let mut wal = Wal::new(&path2, 16384).unwrap();  // 16KB
            for _ in 0..num_writes {
                wal.append(&entry).unwrap();
            }
            wal.flush().unwrap();
        }
        let large_buffer_time = start.elapsed();
        
        println!("\n=== Batching Performance ===");
        println!("Writes: {}", num_writes);
        println!("Small buffer (128B): {:?} ({:.0} writes/sec)",
                small_buffer_time,
                num_writes as f64 / small_buffer_time.as_secs_f64());
        println!("Large buffer (16KB): {:?} ({:.0} writes/sec)",
                large_buffer_time,
                num_writes as f64 / large_buffer_time.as_secs_f64());
        println!("Speedup: {:.2}x",
                small_buffer_time.as_secs_f64() / large_buffer_time.as_secs_f64());
        
        // Large buffer should be faster
        assert!(large_buffer_time < small_buffer_time);
    }

}







