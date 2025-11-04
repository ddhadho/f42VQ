//! SSTable reader implementation
//! 
//! Key design decisions:
//! - Loads index + bloom filter into memory for fast lookups
//! - Reads data blocks on-demand from disk
//! - Handles corruption gracefully (returns Error, doesn't panic)
//! - Uses prefix decompression with validation

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use bytes::{Buf, Bytes};
use crate::error::{Result, StorageError};
use crate::sstable::format::*;
use crate::sstable::bloom::BloomFilter;

/// SSTable reader
/// 
/// Memory layout:
/// - Header: ~64 bytes
/// - Index: ~40 bytes per block (e.g., 2.5MB for 1GB file with 16KB blocks)
/// - Bloom filter: ~1-2KB
/// - Data blocks: read on-demand
pub struct SsTableReader {
    file: File,
    path: PathBuf,
    index: Vec<IndexEntry>,
    bloom_filter: BloomFilter,
    header: Header,
}

impl SsTableReader {
    /// Open an SSTable file for reading
    /// 
    /// Loads metadata (header, index, bloom filter) into memory
    /// but reads data blocks on-demand during get/scan operations
    pub fn open(path: PathBuf) -> Result<Self> {
        let mut file = File::open(&path)?;
        
        // 1. Read and validate header
        let header = Self::read_header(&mut file)?;
        
        // 2. Read footer (contains pointers to index and bloom filter)
        let footer = Self::read_footer(&mut file)?;
        
        // 3. Load bloom filter into memory
        let bloom_filter = Self::read_bloom_filter(&mut file, &footer)?;
        
        // 4. Load index into memory (critical for fast lookups)
        let index = Self::read_index(&mut file, &footer)?;
        
        Ok(Self {
            file,
            path,
            index,
            bloom_filter,
            header,
        })
    }
    
    /// Get a value by key
    /// 
    /// Algorithm:
    /// 1. Check bloom filter (fast negative test)
    /// 2. Binary search index to find candidate block
    /// 3. Read and decompress block
    /// 4. Search within block using prefix decompression
    pub fn get(&mut self, key: &[u8]) -> Result<Option<(Vec<u8>, u64)>> {
        // Fast path: bloom filter says key doesn't exist
        // This saves expensive disk I/O for missing keys
        if !self.bloom_filter.contains(key) {
            return Ok(None);
        }
        
        // Find which block might contain this key
        let block_idx = match self.find_block_for_key(key) {
            Some(idx) => idx,
            None => return Ok(None),  // Key is before first block
        };
        
        // Read and decompress the block
        let entries = self.read_and_decompress_block(block_idx)?;
        
        // Binary search within the decompressed block
        // (entries are sorted by key)
        match entries.binary_search_by(|entry| entry.key.as_slice().cmp(key)) {
            Ok(idx) => Ok(Some((entries[idx].value.clone(), entries[idx].timestamp))),
            Err(_) => Ok(None),
        }
    }
    
    /// Scan a range of keys [start, end] inclusive
    /// 
    /// Returns all entries where start <= key <= end
    pub fn scan(&mut self, start: &[u8], end: &[u8]) 
        -> Result<Vec<(Vec<u8>, Vec<u8>, u64)>> 
    {
        let mut results = Vec::new();
        
        // Find first block that might contain start key
        let start_block = self.find_block_for_key(start)
            .unwrap_or(0);  // If before first block, start from beginning
        
        // Read consecutive blocks until we pass end key
        for block_idx in start_block..self.index.len() {
            let entries = self.read_and_decompress_block(block_idx)?;
            
            for entry in entries {
                // Skip keys before start
                if entry.key.as_slice() < start {
                    continue;
                }
                
                // Stop when we pass end
                if entry.key.as_slice() > end {
                    return Ok(results);
                }
                
                // This key is in range
                results.push((entry.key, entry.value, entry.timestamp));
            }
        }
        
        Ok(results)
    }
    
    // === Helper Methods ===
    
    /// Read header from file
    fn read_header(file: &mut File) -> Result<Header> {
        file.seek(SeekFrom::Start(0))?;
        
        let mut buf = vec![0u8; HEADER_SIZE];
        file.read_exact(&mut buf)?;
        
        Header::decode(&buf)
    }
    
    /// Read footer from file (at the end)
    fn read_footer(file: &mut File) -> Result<Footer> {
        file.seek(SeekFrom::End(-(FOOTER_SIZE as i64)))?;
        
        let mut buf = vec![0u8; FOOTER_SIZE];
        file.read_exact(&mut buf)?;
        
        Footer::decode(&buf)
    }
    
    /// Read bloom filter from file
    fn read_bloom_filter(file: &mut File, footer: &Footer) -> Result<BloomFilter> {
        if footer.bloom_size == 0 {
            // No bloom filter in file (placeholder writer)
            return Ok(BloomFilter::new(Vec::new()));
        }
        
        file.seek(SeekFrom::Start(footer.bloom_offset))?;
        
        let mut buf = vec![0u8; footer.bloom_size as usize];
        file.read_exact(&mut buf)?;
        
        BloomFilter::decode(&buf)
    }
    
    /// Read index from file
    fn read_index(file: &mut File, footer: &Footer) -> Result<Vec<IndexEntry>> {
        file.seek(SeekFrom::Start(footer.index_offset))?;
        
        let mut buf = vec![0u8; footer.index_size as usize];
        file.read_exact(&mut buf)?;
        
        let mut entries = Vec::new();
        let mut cursor = &buf[..];
        
        while cursor.remaining() > 0 {
            let entry = IndexEntry::decode(&mut cursor)?;
            entries.push(entry);
        }
        
        Ok(entries)
    }
    
    /// Find which block might contain the given key
    /// 
    /// Returns the index of the last block where first_key <= key
    /// 
    /// Example:
    ///   Block 0: first_key = "a"
    ///   Block 1: first_key = "m"
    ///   Block 2: first_key = "z"
    ///   
    ///   find_block_for_key("p") -> Some(1)  (block 1: "m" <= "p" < "z")
    ///   find_block_for_key("m") -> Some(1)  (exact match)
    ///   find_block_for_key("0") -> None     (before first block)
    fn find_block_for_key(&self, key: &[u8]) -> Option<usize> {
        if self.index.is_empty() {
            return None;
        }
        
        // Key is before the first block
        if key < self.index[0].first_key.as_slice() {
            return None;
        }
        
        // Use partition_point to find the insertion point
        // This returns the first index where first_key > key
        let idx = self.index.partition_point(|entry| {
            entry.first_key.as_slice() <= key
        });
        
        // We want the block BEFORE the insertion point
        // (the last block where first_key <= key)
        if idx > 0 {
            Some(idx - 1)
        } else {
            None
        }
    }
    
    /// Read a block from disk and decompress it
    /// 
    /// This is the hot path for reads - optimize carefully!
    fn read_and_decompress_block(&mut self, block_idx: usize) -> Result<Vec<BlockEntry>> {
        if block_idx >= self.index.len() {
            return Err(StorageError::InvalidFormat(
                format!("Block index {} out of range", block_idx)
            ));
        }
        
        let entry = &self.index[block_idx];
        
        // Read compressed block from disk
        self.file.seek(SeekFrom::Start(entry.offset))?;
        let mut compressed = vec![0u8; entry.size as usize];
        self.file.read_exact(&mut compressed)?;
        
        // Decompress with Snappy
        let decompressed = snap::raw::Decoder::new()
            .decompress_vec(&compressed)
            .map_err(|e| StorageError::CorruptedData(
                format!("Snappy decompression failed: {}", e)
            ))?;
        
        // Decode entries with prefix decompression
        Self::decode_block(&decompressed)
    }
    
    /// Decode a decompressed block into entries
    /// 
    /// Handles prefix compression: each entry stores shared prefix length
    /// with previous key, then only the differing suffix
    fn decode_block(data: &[u8]) -> Result<Vec<BlockEntry>> {
        let mut entries = Vec::new();
        let mut cursor = &data[..];
        let mut previous_key = Vec::new();
        
        while cursor.remaining() > 0 {
            // Read prefix compression metadata
            let shared_len = Self::decode_varint(&mut cursor)?;
            let unshared_len = Self::decode_varint(&mut cursor)?;
            let value_len = Self::decode_varint(&mut cursor)?;
            
            // Validate shared_len (detect corruption)
            if shared_len > previous_key.len() {
                return Err(StorageError::CorruptedData(
                    format!("Invalid shared_len {} > previous key len {}", 
                            shared_len, previous_key.len())
                ));
            }
            
            // Reconstruct full key: reuse prefix + append suffix
            let mut key = Vec::with_capacity(shared_len + unshared_len);
            key.extend_from_slice(&previous_key[..shared_len]);
            
            if cursor.remaining() < unshared_len {
                return Err(StorageError::CorruptedData(
                    "Truncated key delta".into()
                ));
            }
            key.extend_from_slice(&cursor[..unshared_len]);
            cursor.advance(unshared_len);
            
            // Read value
            if cursor.remaining() < value_len {
                return Err(StorageError::CorruptedData(
                    "Truncated value".into()
                ));
            }
            let value = cursor[..value_len].to_vec();
            cursor.advance(value_len);
            
            // Read timestamp
            if cursor.remaining() < 8 {
                return Err(StorageError::CorruptedData(
                    "Truncated timestamp".into()
                ));
            }
            let timestamp = cursor.get_u64_le();
            
            // Validate sort order (keys must be sorted)
            if !previous_key.is_empty() && key <= previous_key {
                return Err(StorageError::CorruptedData(
                    "Keys not in sorted order".into()
                ));
            }
            
            previous_key = key.clone();
            entries.push(BlockEntry { key, value, timestamp });
        }
        
        Ok(entries)
    }
    
    /// Decode a variable-length integer (varint)
    /// 
    /// Uses LEB128 encoding: 7 bits of data per byte, MSB = continuation bit
    fn decode_varint(buf: &mut &[u8]) -> Result<usize> {
        let mut result = 0;
        let mut shift = 0;
        
        loop {
            if buf.is_empty() {
                return Err(StorageError::CorruptedData(
                    "Truncated varint".into()
                ));
            }
            
            let byte = buf[0];
            buf.advance(1);
            
            result |= ((byte & 0x7F) as usize) << shift;
            
            if byte & 0x80 == 0 {
                break;
            }
            
            shift += 7;
            if shift >= 64 {
                return Err(StorageError::CorruptedData(
                    "Varint too large".into()
                ));
            }
        }
        
        Ok(result)
    }
    
    /// Get diagnostic information about this SSTable
    pub fn info(&self) -> SsTableInfo {
        SsTableInfo {
            path: self.path.clone(),
            num_blocks: self.header.num_blocks,
            min_timestamp: self.header.min_timestamp,
            max_timestamp: self.header.max_timestamp,
            index_entries: self.index.len(),
        }
    }
}

/// Entry decoded from a block
#[derive(Debug, Clone)]
struct BlockEntry {
    key: Vec<u8>,
    value: Vec<u8>,
    timestamp: u64,
}

/// Diagnostic information about an SSTable
#[derive(Debug)]
pub struct SsTableInfo {
    pub path: PathBuf,
    pub num_blocks: u32,
    pub min_timestamp: u64,
    pub max_timestamp: u64,
    pub index_entries: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sstable::writer::SsTableWriter;
    use tempfile::TempDir;
    
    #[test]
    fn test_reader_basic() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let path = temp_dir.path().join("test.sst");
        
        // Write some data
        let mut writer = SsTableWriter::new(path.clone(), DEFAULT_BLOCK_SIZE)?;
        writer.add(b"key1", b"value1", 100)?;
        writer.add(b"key2", b"value2", 200)?;
        writer.add(b"key3", b"value3", 300)?;
        writer.finish()?;
        
        // Read it back
        let mut reader = SsTableReader::open(path)?;
        
        assert_eq!(reader.get(b"key1")?, Some((b"value1".to_vec(), 100)));
        assert_eq!(reader.get(b"key2")?, Some((b"value2".to_vec(), 200)));
        assert_eq!(reader.get(b"key3")?, Some((b"value3".to_vec(), 300)));
        assert_eq!(reader.get(b"key4")?, None);
        
        Ok(())
    }
    
    #[test]
    fn test_reader_scan() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let path = temp_dir.path().join("test.sst");
        
        // Write test data
        let mut writer = SsTableWriter::new(path.clone(), DEFAULT_BLOCK_SIZE)?;
        for i in 0..10 {
            let key = format!("key{:02}", i);
            let value = format!("value{}", i);
            writer.add(key.as_bytes(), value.as_bytes(), i as u64)?;
        }
        writer.finish()?;
        
        // Scan a range
        let mut reader = SsTableReader::open(path)?;
        let results = reader.scan(b"key03", b"key07")?;
        
        assert_eq!(results.len(), 5);
        assert_eq!(results[0].0, b"key03");
        assert_eq!(results[4].0, b"key07");
        
        Ok(())
    }
}