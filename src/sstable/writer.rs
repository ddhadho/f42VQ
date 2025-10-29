//! /// SSTable Writer
/// 
/// Writes sorted key-value-timestamp tuples to disk in an immutable format.
/// 
/// # Format
/// - Data is written in blocks (default 4KB)
/// - Each block is independently compressed with Snappy
/// - Keys within blocks use prefix compression
/// - Index allows binary search over blocks
/// - Bloom filter enables fast "key not found" checks
/// 
/// # Usage
/// ```ignore
/// let mut writer = SsTableWriter::new(path)?;
/// // Keys MUST be added in sorted order
/// writer.add(b"key1", b"value1", 1000)?;
/// writer.add(b"key2", b"value2", 2000)?;
/// writer.finish()?;  // Flushes remaining data and writes metadata
/// ```

use crate::{Result, Timestamp};
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use super::block::BlockBuilder;
use super::bloom::BloomFilterBuilder;
use super::format::{Header, Footer, IndexEntry};

pub struct SsTableWriter {
    file: File,
    path: PathBuf,
    block_builder: BlockBuilder,
    index_entries: Vec<IndexEntry>,
    bloom_filter: BloomFilterBuilder,
    block_size: usize,
    offset: u64,
    header: Header,
}

impl SsTableWriter {
    pub fn new(path: PathBuf, block_size: usize) -> Result<Self> {
        let file = File::create(&path)?;
        
        Ok(SsTableWriter {
            file,
            path,
            block_builder: BlockBuilder::new(),
            index_entries: Vec::new(),
            bloom_filter: BloomFilterBuilder::new(10000, 0.01),
            block_size,
            offset: 0,
            header: Header::new(),
        })
    }
    
    /// Add a key-value pair with timestamp
    /// Keys MUST be added in sorted order!
    pub fn add(&mut self, key: &[u8], value: &[u8], timestamp: Timestamp) -> Result<()> {
        // Add to bloom filter
        self.bloom_filter.add(key);
        
        // Update min/max timestamps
        if timestamp < self.header.min_timestamp {
            self.header.min_timestamp = timestamp;
        }
        if timestamp > self.header.max_timestamp {
            self.header.max_timestamp = timestamp;
        }
        
        // Add to current block
        self.block_builder.add(key, value, timestamp);
        
        // Flush block if it's full
        if self.block_builder.size() >= self.block_size {
            self.flush_block()?;
        }
        
        Ok(())
    }
    
    fn flush_block(&mut self) -> Result<()> {
        if self.block_builder.is_empty() {
            return Ok(());
        }
        
        // Get first key for index
        let first_key = self.block_builder.first_key()
            .ok_or_else(|| crate::StorageError::InvalidFormat(
                "Block has no first key".into()
            ))?
            .to_vec();
        
        // Build and compress block
        let compressed = self.block_builder.finish()?;
        
        // Write block data
        self.file.write_all(&compressed)?;
        
        // Add index entry
        self.index_entries.push(IndexEntry {
            first_key,
            offset: self.offset,
            size: compressed.len() as u32,
        });
        
        self.offset += compressed.len() as u64;
        self.header.num_blocks += 1;
        
        // Reset block builder for next block
        self.block_builder.reset();
        
        Ok(())
    }
    
    /// Finish writing and close the file
    pub fn finish(&mut self) -> Result<()> {
        // Flush any remaining data in current block
        self.flush_block()?;
        
        // Write bloom filter
        let bloom_offset = self.offset;
        let bloom_data = self.bloom_filter.finish();
        self.file.write_all(&bloom_data)?;
        let bloom_size = bloom_data.len() as u32;
        self.offset += bloom_data.len() as u64;
        
        // Write index
        let index_offset = self.offset;
        let index_data = self.encode_index()?;
        self.file.write_all(&index_data)?;
        let index_size = index_data.len() as u32;
        self.offset += index_data.len() as u64;
        
        // Write footer
        let footer = Footer {
            index_offset,
            bloom_offset,
            index_size,
            bloom_size,
            checksum: 0, // TODO: Calculate checksum
        };
        self.file.write_all(&footer.encode())?;
        
        // Go back to beginning and write header
        use std::io::Seek;
        self.file.seek(std::io::SeekFrom::Start(0))?;
        self.file.write_all(&self.header.encode())?;
        
        // Ensure everything is written
        self.file.sync_all()?;
        
        Ok(())
    }
    
    fn encode_index(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        
        for entry in &self.index_entries {
            buf.extend_from_slice(&entry.encode());
        }
        
        Ok(buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    
    #[test]
    fn test_sstable_writer_basic() -> Result<()> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");
        
        let mut writer = SsTableWriter::new(path.clone(), 4096)?;
        
        // Write some entries (must be sorted!)
        for i in 0..100 {
            let key = format!("key_{:04}", i);
            let value = format!("value_{:04}", i);
            writer.add(key.as_bytes(), value.as_bytes(), 1000 + i)?;
        }
        
        writer.finish()?;
        
        // Verify file exists and has data
        assert!(path.exists());
        let metadata = std::fs::metadata(&path)?;
        assert!(metadata.len() > 0);
        
        Ok(())
    }
}