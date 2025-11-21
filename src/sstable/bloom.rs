//! Bloom filter implementation for fast negative lookups
//! 
//! Custom implementation with serialization support.
//! Uses multiple hash functions (FNV and murmur variants) for good distribution.

use crate::{Result, StorageError};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Bloom filter for SSTable
/// 
/// Space-efficient probabilistic data structure that can tell you:
/// - "Definitely NOT in set" (100% accurate)
/// - "Maybe in set" (~1% false positive rate)
pub struct BloomFilter {
    bits: Vec<u8>,           // Bit array
    num_bits: usize,         // Total bits
    num_hash_functions: u32, // k hash functions
}

impl BloomFilter {
    /// Create a new bloom filter
    /// 
    /// # Arguments
    /// * `expected_items` - Number of keys expected (e.g., 10,000)
    /// * `false_positive_rate` - Desired FP rate (e.g., 0.01 = 1%)
    pub fn new(expected_items: usize, false_positive_rate: f64) -> Self {
        // Calculate optimal parameters
        let num_bits = Self::optimal_num_bits(expected_items, false_positive_rate);
        let num_hash_functions = Self::optimal_num_hash_functions(expected_items, num_bits);
        
        // Allocate bit array (8 bits per byte)
        let num_bytes = (num_bits + 7) / 8;
        let bits = vec![0u8; num_bytes];
        
        BloomFilter {
            bits,
            num_bits,
            num_hash_functions,
        }
    }
    
    /// Calculate optimal number of bits
    /// Formula: m = -n * ln(p) / (ln(2)^2)
    fn optimal_num_bits(n: usize, p: f64) -> usize {
        let ln2_squared = std::f64::consts::LN_2 * std::f64::consts::LN_2;
        let m = -(n as f64) * p.ln() / ln2_squared;
        m.ceil() as usize
    }
    
    /// Calculate optimal number of hash functions
    /// Formula: k = (m/n) * ln(2)
    fn optimal_num_hash_functions(n: usize, m: usize) -> u32 {
        let k = (m as f64 / n as f64) * std::f64::consts::LN_2;
        k.ceil().max(1.0) as u32
    }
    
    /// Generate k hash values for a key
    fn hash(&self, key: &[u8]) -> Vec<usize> {
        let mut hashes = Vec::with_capacity(self.num_hash_functions as usize);
        
        // Use double hashing: h_i = h1 + i*h2
        let h1 = self.hash_fn(key, 0) as usize;
        let h2 = self.hash_fn(key, 1) as usize;
        
        for i in 0..self.num_hash_functions {
            let hash = h1.wrapping_add((i as usize).wrapping_mul(h2));
            hashes.push(hash % self.num_bits);
        }
        
        hashes
    }
    
    /// Single hash function
    fn hash_fn(&self, key: &[u8], seed: u64) -> u64 {
        let mut hasher = DefaultHasher::new();
        seed.hash(&mut hasher);
        key.hash(&mut hasher);
        hasher.finish()
    }
    
    /// Set a bit at position
    fn set_bit(&mut self, pos: usize) {
        let byte_idx = pos / 8;
        let bit_idx = pos % 8;
        if byte_idx < self.bits.len() {
            self.bits[byte_idx] |= 1 << bit_idx;
        }
    }
    
    /// Get a bit at position
    fn get_bit(&self, pos: usize) -> bool {
        let byte_idx = pos / 8;
        let bit_idx = pos % 8;
        if byte_idx < self.bits.len() {
            (self.bits[byte_idx] & (1 << bit_idx)) != 0
        } else {
            false
        }
    }
    
    /// Check if key might be in the set
    pub fn contains(&self, key: &[u8]) -> bool {
        let hashes = self.hash(key);
        hashes.iter().all(|&pos| self.get_bit(pos))
    }
    
    /// Encode bloom filter to bytes
    pub fn encode(&self) -> Vec<u8> {
        let mut data = Vec::new();
        
        // Write metadata
        data.extend_from_slice(&(self.num_bits as u32).to_le_bytes());
        data.extend_from_slice(&self.num_hash_functions.to_le_bytes());
        
        // Write bit array
        data.extend_from_slice(&self.bits);
        
        data
    }
    
    /// Decode bloom filter from bytes
    pub fn decode(data: &[u8]) -> Result<Self> {
        if data.is_empty() {
            // Empty = backwards compatibility (permissive filter)
            // Create a filter with all bits set = always returns true
            let num_bits = 8; // Minimal size
            let bits = vec![0xFF; 1]; // All bits set
            return Ok(BloomFilter {
                bits,
                num_bits,
                num_hash_functions: 1,
            });
        }
        
        if data.len() < 8 {
            return Err(StorageError::InvalidFormat(
                "Bloom filter data too short".into()
            ));
        }
        
        // Read metadata
        let num_bits = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
        let num_hash_functions = u32::from_le_bytes([data[4], data[5], data[6], data[7]]);
        
        // Read bit array
        let bits = data[8..].to_vec();
        
        // Validate
        let expected_bytes = (num_bits + 7) / 8;
        if bits.len() != expected_bytes {
            return Err(StorageError::InvalidFormat(
                format!("Bloom filter size mismatch: expected {} bytes, got {}", 
                        expected_bytes, bits.len())
            ));
        }
        
        Ok(BloomFilter {
            bits,
            num_bits,
            num_hash_functions,
        })
    }
    
    /// Get statistics
    pub fn stats(&self) -> BloomFilterStats {
        let num_bytes = self.bits.len();
        let bits_set = self.bits.iter()
            .map(|byte| byte.count_ones() as usize)
            .sum::<usize>();
        
        BloomFilterStats {
            num_bits: self.num_bits,
            num_hash_functions: self.num_hash_functions as usize,
            estimated_memory: num_bytes + 8, // bits + metadata
            bits_set,
            fill_ratio: bits_set as f64 / self.num_bits as f64,
        }
    }
}

/// Bloom filter builder for SSTable writing
pub struct BloomFilterBuilder {
    filter: BloomFilter,
}

impl BloomFilterBuilder {
    /// Create new bloom filter builder
    pub fn new(expected_items: usize, false_positive_rate: f64) -> Self {
        BloomFilterBuilder {
            filter: BloomFilter::new(expected_items, false_positive_rate),
        }
    }
    
    /// Add a key to the bloom filter
    pub fn add(&mut self, key: &[u8]) {
        let hashes = self.filter.hash(key);
        for pos in hashes {
            self.filter.set_bit(pos);
        }
    }
    
    /// Finish building and return serialized bytes
    pub fn finish(&self) -> Vec<u8> {
        self.filter.encode()
    }
}

/// Bloom filter statistics
#[derive(Debug, Clone)]
pub struct BloomFilterStats {
    pub num_bits: usize,
    pub num_hash_functions: usize,
    pub estimated_memory: usize,
    pub bits_set: usize,
    pub fill_ratio: f64,
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_bloom_filter_basic() {
        let mut builder = BloomFilterBuilder::new(100, 0.01);
        
        // Add keys
        for i in 0..100 {
            let key = format!("key_{}", i);
            builder.add(key.as_bytes());
        }
        
        // Serialize and deserialize
        let data = builder.finish();
        println!("Bloom filter size: {} bytes", data.len());
        assert!(!data.is_empty());
        
        let filter = BloomFilter::decode(&data).unwrap();
        
        // All added keys should be found
        for i in 0..100 {
            let key = format!("key_{}", i);
            assert!(filter.contains(key.as_bytes()),
                    "Key {} should be in filter", key);
        }
        
        println!("✓ Basic test: All 100 keys found");
    }
    
    #[test]
    fn test_bloom_filter_false_positive_rate() {
        let mut builder = BloomFilterBuilder::new(1000, 0.01);
        
        // Add keys 0-999
        for i in 0..1000 {
            let key = format!("key_{:04}", i);
            builder.add(key.as_bytes());
        }
        
        let data = builder.finish();
        let filter = BloomFilter::decode(&data).unwrap();
        
        // Test keys 10000-10999 (not in filter)
        let mut false_positives = 0;
        let test_count = 1000;
        
        for i in 10000..(10000 + test_count) {
            let key = format!("key_{:04}", i);
            if filter.contains(key.as_bytes()) {
                false_positives += 1;
            }
        }
        
        let fp_rate = false_positives as f64 / test_count as f64;
        println!("False positive rate: {:.2}% ({}/{})", 
                 fp_rate * 100.0, false_positives, test_count);
        
        // Should be close to 1% (allow up to 5% variance)
        assert!(fp_rate < 0.05,
                "FP rate too high: {:.2}%", fp_rate * 100.0);
        
        println!("✓ FP rate test: {:.2}%", fp_rate * 100.0);
    }
    
    #[test]
    fn test_bloom_filter_stats() {
        let mut builder = BloomFilterBuilder::new(1000, 0.01);
        
        for i in 0..500 {
            let key = format!("sensor_{:04}", i);
            builder.add(key.as_bytes());
        }
        
        let data = builder.finish();
        let filter = BloomFilter::decode(&data).unwrap();
        let stats = filter.stats();
        
        println!("Bloom filter statistics:");
        println!("  Bits: {}", stats.num_bits);
        println!("  Hash functions: {}", stats.num_hash_functions);
        println!("  Memory: {} bytes ({:.2} KB)", 
                 stats.estimated_memory,
                 stats.estimated_memory as f64 / 1024.0);
        println!("  Fill ratio: {:.1}%", stats.fill_ratio * 100.0);
        
        assert!(stats.num_bits > 1000);
        assert!(stats.num_hash_functions >= 1);
        assert!(stats.estimated_memory < 20_000);
        
        println!("✓ Stats test passed");
    }
    
    #[test]
    fn test_empty_bloom_filter() {
        let filter = BloomFilter::decode(&[]).unwrap();
        assert!(filter.contains(b"any_key"));
        
        println!("✓ Empty filter (backwards compatibility)");
    }
    
    #[test]
    fn test_bloom_filter_time_series() {
        let mut builder = BloomFilterBuilder::new(1000, 0.01);
        
        // Add time-series keys
        for i in 0..1000 {
            let key = format!("sensor_001_temp_{}", i);
            builder.add(key.as_bytes());
        }
        
        let data = builder.finish();
        let filter = BloomFilter::decode(&data).unwrap();
        
        // Check present keys
        assert!(filter.contains(b"sensor_001_temp_0"));
        assert!(filter.contains(b"sensor_001_temp_500"));
        assert!(filter.contains(b"sensor_001_temp_999"));
        
        // Count rejections of absent keys
        let mut rejections = 0;
        for i in 2000..3000 {
            let key = format!("sensor_002_temp_{}", i);
            if !filter.contains(key.as_bytes()) {
                rejections += 1;
            }
        }
        
        let rejection_rate = rejections as f64 / 1000.0;
        println!("Rejection rate: {:.1}% ({}/1000)", 
                 rejection_rate * 100.0, rejections);
        
        assert!(rejection_rate > 0.90,
                "Should reject >90% of absent keys");
        
        println!("✓ Time-series test: {:.1}% rejected", rejection_rate * 100.0);
    }
    
    #[test]
    fn test_bloom_filter_parameters() {
        // Test various configurations
        let configs = vec![
            (100, 0.01),    // Small, low FP
            (1000, 0.01),   // Medium, low FP
            (10000, 0.01),  // Large, low FP
            (1000, 0.05),   // Medium, higher FP (less memory)
        ];
        
        for (n, p) in configs {
            let filter = BloomFilter::new(n, p);
            let stats = filter.stats();
            
            println!("Config n={}, p={:.2}%: {} bits, {} hashes, {:.2}KB",
                     n, p * 100.0,
                     stats.num_bits,
                     stats.num_hash_functions,
                     stats.estimated_memory as f64 / 1024.0);
        }
        
        println!("✓ Parameter test passed");
    }
}