use crate::{Result, Timestamp};
use std::collections::BTreeMap;

/// In-memory sorted table with timestamp support
/// 
/// Stores (key -> (value, timestamp)) pairs for time-series data.
/// When MemTable reaches max_size, it should be flushed to disk as an SSTable.
pub struct MemTable {
    data: BTreeMap<Vec<u8>, (Vec<u8>, Timestamp)>,
    size_bytes: usize,
    max_size: usize,
}

impl MemTable {
    pub fn new(max_size: usize) -> Self {
        MemTable {
            data: BTreeMap::new(),
            size_bytes: 0,
            max_size,
        }
    }
    
    /// Insert a key-value pair with timestamp
    /// Returns true if MemTable is now full and should be flushed
    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>, timestamp: Timestamp) -> Result<bool> {
        let key_size = key.len();
        let value_size = value.len();
        let entry_overhead = 40 + 8;  // BTreeMap overhead + timestamp size
        
        // Check if we're updating an existing key
        let old_value_size = if let Some((old_value, _)) = self.data.get(&key) {
            old_value.len()
        } else {
            0
        };
        
        // Insert the entr
        self.data.insert(key, (value, timestamp));
        
        // Update size tracking
        if old_value_size > 0 {
            // Updating existing key - only value size changed
            self.size_bytes = self.size_bytes - old_value_size + value_size;
        } else {
            // New key - add full entry size
            let new_size = key_size + value_size + entry_overhead;
            self.size_bytes += new_size;
        }
        
        // Return true if memtable is full
        Ok(self.size_bytes >= self.max_size)
    }
    
    /// Get the value for a key (ignores timestamp)
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.data.get(key).map(|(v, _)| v.clone())
    }
    
    /// Get the value and timestamp for a key
    pub fn get_with_timestamp(&self, key: &[u8]) -> Option<(Vec<u8>, Timestamp)> {
        self.data.get(key).map(|(v, t)| (v.clone(), *t))
    }
    
    /// Scan a range of keys
    pub fn scan(&self, start: &[u8], end: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.data
            .range(start.to_vec()..end.to_vec())
            .map(|(k, (v, _))| (k.clone(), v.clone()))
            .collect()
    }
    
    /// Scan a range with timestamps (useful for time-series queries)
    pub fn scan_with_timestamps(&self, start: &[u8], end: &[u8]) -> Vec<(Vec<u8>, Vec<u8>, Timestamp)> {
        self.data
            .range(start.to_vec()..end.to_vec())
            .map(|(k, (v, t))| (k.clone(), v.clone(), *t))
            .collect()
    }
    
    /// Get current size in bytes
    pub fn size_bytes(&self) -> usize {
        self.size_bytes
    }
    
    /// Get number of entries
    pub fn len(&self) -> usize {
        self.data.len()
    }
    
    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
    
    /// Get all entries (for flushing to SSTable)
    pub fn entries(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.data.iter()
            .map(|(k, (v, _))| (k.clone(), v.clone()))
            .collect()
    }
    
    /// Get all entries with timestamps (for flushing to SSTable)
    pub fn entries_with_timestamps(&self) -> Vec<(Vec<u8>, Vec<u8>, Timestamp)> {
        self.data.iter()
            .map(|(k, (v, t))| (k.clone(), v.clone(), *t))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_put_and_get() {
        let mut memtable = MemTable::new(1024);
        
        memtable.put(b"key1".to_vec(), b"value1".to_vec(), 1000).unwrap();
        memtable.put(b"key2".to_vec(), b"value2".to_vec(), 2000).unwrap();
        
        assert_eq!(memtable.get(b"key1"), Some(b"value1".to_vec()));
        assert_eq!(memtable.get(b"key2"), Some(b"value2".to_vec()));
        assert_eq!(memtable.get(b"key3"), None);
    }
    
    #[test]
    fn test_get_with_timestamp() {
        let mut memtable = MemTable::new(1024);
        
        memtable.put(b"key1".to_vec(), b"value1".to_vec(), 1000).unwrap();
        
        let result = memtable.get_with_timestamp(b"key1");
        assert_eq!(result, Some((b"value1".to_vec(), 1000)));
    }
    
    #[test]
    fn test_overwrite() {
        let mut memtable = MemTable::new(1024);
        
        memtable.put(b"key".to_vec(), b"value1".to_vec(), 1000).unwrap();
        assert_eq!(memtable.get(b"key"), Some(b"value1".to_vec()));
        
        // Overwrite with newer timestamp
        memtable.put(b"key".to_vec(), b"value2".to_vec(), 2000).unwrap();
        assert_eq!(memtable.get(b"key"), Some(b"value2".to_vec()));
        
        // Check timestamp was updated
        let (_, ts) = memtable.get_with_timestamp(b"key").unwrap();
        assert_eq!(ts, 2000);
    }
    
    #[test]
    fn test_size_tracking() {
        let mut memtable = MemTable::new(1024);
        
        let initial_size = memtable.size_bytes();
        assert_eq!(initial_size, 0);
        
        memtable.put(b"key".to_vec(), b"value".to_vec(), 1000).unwrap();
        
        let after_insert = memtable.size_bytes();
        assert!(after_insert > initial_size);
        assert!(after_insert >= 56); // key + value + overhead + timestamp
    }
    
    #[test]
    fn test_full_detection() {
        let mut memtable = MemTable::new(100);
        
        let mut is_full = false;
        for i in 0..100 {
            let key = format!("key_{}", i);
            let value = vec![0u8; 50];
            is_full = memtable.put(key.into_bytes(), value, 1000 + i as u64).unwrap();
            if is_full {
                break;
            }
        }
        
        assert!(is_full);
    }
    
    #[test]
    fn test_scan() {
        let mut memtable = MemTable::new(1024);
        
        memtable.put(b"key3".to_vec(), b"value3".to_vec(), 3000).unwrap();
        memtable.put(b"key1".to_vec(), b"value1".to_vec(), 1000).unwrap();
        memtable.put(b"key5".to_vec(), b"value5".to_vec(), 5000).unwrap();
        memtable.put(b"key2".to_vec(), b"value2".to_vec(), 2000).unwrap();
        memtable.put(b"key4".to_vec(), b"value4".to_vec(), 4000).unwrap();
        
        let results = memtable.scan(b"key2", b"key5");
        
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].0, b"key2");
        assert_eq!(results[1].0, b"key3");
        assert_eq!(results[2].0, b"key4");
    }
    
    #[test]
    fn test_scan_with_timestamps() {
        let mut memtable = MemTable::new(1024);
        
        memtable.put(b"key1".to_vec(), b"value1".to_vec(), 1000).unwrap();
        memtable.put(b"key2".to_vec(), b"value2".to_vec(), 2000).unwrap();
        memtable.put(b"key3".to_vec(), b"value3".to_vec(), 3000).unwrap();
        
        let results = memtable.scan_with_timestamps(b"key1", b"key4");
        
        assert_eq!(results.len(), 3);
        assert_eq!(results[0], (b"key1".to_vec(), b"value1".to_vec(), 1000));
        assert_eq!(results[1], (b"key2".to_vec(), b"value2".to_vec(), 2000));
        assert_eq!(results[2], (b"key3".to_vec(), b"value3".to_vec(), 3000));
    }
    
    #[test]
    fn test_entries_with_timestamps() {
        let mut memtable = MemTable::new(1024);
        
        memtable.put(b"key1".to_vec(), b"value1".to_vec(), 1000).unwrap();
        memtable.put(b"key2".to_vec(), b"value2".to_vec(), 2000).unwrap();
        
        let entries = memtable.entries_with_timestamps();
        
        assert_eq!(entries.len(), 2);
        // BTreeMap maintains sorted order
        assert_eq!(entries[0], (b"key1".to_vec(), b"value1".to_vec(), 1000));
        assert_eq!(entries[1], (b"key2".to_vec(), b"value2".to_vec(), 2000));
    }
}