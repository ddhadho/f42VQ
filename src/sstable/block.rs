//! Block builder and reader for SSTable data blocks

use bytes::{BufMut, BytesMut};
use crate::Result;

/// Builds a data block with prefix compression
pub struct BlockBuilder {
    buffer: BytesMut,
    last_key: Vec<u8>,
    first_key: Option<Vec<u8>>,
    entries_count: usize,
}

impl BlockBuilder {
    pub fn new() -> Self {
        BlockBuilder {
            buffer: BytesMut::new(),
            last_key: Vec::new(),
            first_key: None,
            entries_count: 0,
        }
    }
    
    /// Add an entry to the block (key must be sorted!)
    pub fn add(&mut self, key: &[u8], value: &[u8], timestamp: u64) {
        // Save first key
        if self.first_key.is_none() {
            self.first_key = Some(key.to_vec());
        }
        
        // Calculate shared prefix length with previous key
        let shared = common_prefix_len(&self.last_key, key);
        let unshared = key.len() - shared;
        
        // Encode entry with prefix compression
        encode_varint(&mut self.buffer, shared);
        encode_varint(&mut self.buffer, unshared);
        encode_varint(&mut self.buffer, value.len());
        
        // Write unshared part of key
        self.buffer.put_slice(&key[shared..]);
        
        // Write value
        self.buffer.put_slice(value);
        
        // Write timestamp
        self.buffer.put_u64_le(timestamp);
        
        // Update last key
        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        
        self.entries_count += 1;
    }
    
    /// Get current size in bytes
    pub fn size(&self) -> usize {
        self.buffer.len()
    }
    
    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.entries_count == 0
    }
    
    /// Get the first key in this block
    pub fn first_key(&self) -> Option<&[u8]> {
        self.first_key.as_ref().map(|k| k.as_slice())
    }
    
    /// Finish building and return compressed data
    pub fn finish(&mut self) -> Result<Vec<u8>> {
        if self.is_empty() {
            return Ok(Vec::new());
        }
        
        // Compress with Snappy
        let compressed = snap::raw::Encoder::new()
            .compress_vec(&self.buffer)
            .map_err(|e| crate::StorageError::InvalidFormat(
                format!("Compression failed: {}", e)
            ))?;
        
        Ok(compressed)
    }
    
    /// Reset for reuse
    pub fn reset(&mut self) {
        self.buffer.clear();
        self.last_key.clear();
        self.first_key = None;
        self.entries_count = 0;
    }
}

/// Calculate common prefix length
fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    let min_len = a.len().min(b.len());
    for i in 0..min_len {
        if a[i] != b[i] {
            return i;
        }
    }
    min_len
}

/// Encode unsigned integer as varint (variable-length encoding)
fn encode_varint(buf: &mut BytesMut, mut value: usize) {
    while value >= 0x80 {
        buf.put_u8((value as u8) | 0x80);
        value >>= 7;
    }
    buf.put_u8(value as u8);
}

/// Decode varint from bytes
pub fn decode_varint(buf: &mut &[u8]) -> Result<usize> {
    let mut value: usize = 0;
    let mut shift: usize = 0;
    
    loop {
        if buf.is_empty() {
            return Err(crate::StorageError::InvalidFormat(
                "Varint truncated".into()
            ));
        }
        
        let byte = buf[0];
        buf.advance(1);
        
        value |= ((byte & 0x7F) as usize) << shift;
        
        if byte & 0x80 == 0 {
            break;
        }
        
        shift += 7;
        
        if shift > 63 {
            return Err(crate::StorageError::InvalidFormat(
                "Varint too large".into()
            ));
        }
    }
    
    Ok(value)
}

// Need this for decode_varint
use bytes::Buf;