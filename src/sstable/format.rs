//! SSTable file format constants and structures

use crate::Result;
use bytes::{Buf, BufMut, BytesMut};

/// Magic number to identify SSTable files (ASCII: "SSTB")
pub const MAGIC_NUMBER: u32 = 0x53535442;

/// Current format version
pub const VERSION: u32 = 1;

/// Size of header in bytes
pub const HEADER_SIZE: usize = 64;

/// Size of footer in bytes
pub const FOOTER_SIZE: usize = 64;

/// Default block size (16KB)
pub const DEFAULT_BLOCK_SIZE: usize = 16 * 1024;

/// File header
#[derive(Debug, Clone)]
pub struct Header {
    pub magic: u32,
    pub version: u32,
    pub num_blocks: u32,
    pub min_timestamp: u64,
    pub max_timestamp: u64,
}

impl Header {
    pub fn new() -> Self {
        Header {
            magic: MAGIC_NUMBER,
            version: VERSION,
            num_blocks: 0,
            min_timestamp: u64::MAX,
            max_timestamp: 0,
        }
    }
    
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = BytesMut::with_capacity(HEADER_SIZE);
        
        buf.put_u32_le(self.magic);
        buf.put_u32_le(self.version);
        buf.put_u32_le(self.num_blocks);
        buf.put_u64_le(self.min_timestamp);
        buf.put_u64_le(self.max_timestamp);
        
        // Pad to HEADER_SIZE
        while buf.len() < HEADER_SIZE {
            buf.put_u8(0);
        }
        
        buf.to_vec()
    }
    
    pub fn decode(data: &[u8]) -> Result<Self> {
        if data.len() < HEADER_SIZE {
            return Err(crate::StorageError::InvalidFormat(
                "Header too short".into()
            ));
        }
        
        let mut buf = &data[..];
        
        let magic = buf.get_u32_le();
        if magic != MAGIC_NUMBER {
            return Err(crate::StorageError::InvalidFormat(
                format!("Invalid magic number: {:x}", magic)
            ));
        }
        
        let version = buf.get_u32_le();
        let num_blocks = buf.get_u32_le();
        let min_timestamp = buf.get_u64_le();
        let max_timestamp = buf.get_u64_le();
        
        Ok(Header {
            magic,
            version,
            num_blocks,
            min_timestamp,
            max_timestamp,
        })
    }
}

/// File footer
#[derive(Debug, Clone)]
pub struct Footer {
    pub index_offset: u64,      // Where index block starts
    pub bloom_offset: u64,      // Where bloom filter starts
    pub index_size: u32,        // Size of index block
    pub bloom_size: u32,        // Size of bloom filter
    pub checksum: u32,          // Checksum of footer
}

impl Footer {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = BytesMut::with_capacity(FOOTER_SIZE);
        
        buf.put_u64_le(self.index_offset);
        buf.put_u64_le(self.bloom_offset);
        buf.put_u32_le(self.index_size);
        buf.put_u32_le(self.bloom_size);
        buf.put_u32_le(self.checksum);
        
        // Pad to FOOTER_SIZE
        while buf.len() < FOOTER_SIZE {
            buf.put_u8(0);
        }
        
        buf.to_vec()
    }
    
    pub fn decode(data: &[u8]) -> Result<Self> {
        if data.len() < FOOTER_SIZE {
            return Err(crate::StorageError::InvalidFormat(
                "Footer too short".into()
            ));
        }
        
        let mut buf = &data[..];
        
        let index_offset = buf.get_u64_le();
        let bloom_offset = buf.get_u64_le();
        let index_size = buf.get_u32_le();
        let bloom_size = buf.get_u32_le();
        let checksum = buf.get_u32_le();
        
        Ok(Footer {
            index_offset,
            bloom_offset,
            index_size,
            bloom_size,
            checksum,
        })
    }
}

/// Index entry (points to a data block)
#[derive(Debug, Clone)]
pub struct IndexEntry {
    pub first_key: Vec<u8>,     // First key in block
    pub offset: u64,            // File offset of block
    pub size: u32,              // Compressed size of block
}

impl IndexEntry {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = BytesMut::new();
        
        // Key length + key
        buf.put_u16_le(self.first_key.len() as u16);
        buf.put_slice(&self.first_key);
        
        // Offset and size
        buf.put_u64_le(self.offset);
        buf.put_u32_le(self.size);
        
        buf.to_vec()
    }
    
    pub fn decode(data: &mut &[u8]) -> Result<Self> {
        if data.len() < 2 {
            return Err(crate::StorageError::InvalidFormat(
                "Index entry too short".into()
            ));
        }
        
        let key_len = data.get_u16_le() as usize;
        
        if data.remaining() < key_len + 12 {
            return Err(crate::StorageError::InvalidFormat(
                "Index entry truncated".into()
            ));
        }
        
        let first_key = data[..key_len].to_vec();
        data.advance(key_len);
        
        let offset = data.get_u64_le();
        let size = data.get_u32_le();
        
        Ok(IndexEntry {
            first_key,
            offset,
            size,
        })
    }
}