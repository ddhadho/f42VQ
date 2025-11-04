use crate::{Entry, Result, Wal, MemTable};
use crate::sstable::{SsTableWriter, SsTableReader};
use std::path::PathBuf;
use std::time::SystemTime;
use std::sync::atomic::{AtomicU64, Ordering};

const DEFAULT_BLOCK_SIZE: usize = 16 * 1024;

pub struct StorageEngine {
    wal: Wal,
    memtable: MemTable,
    immutable_memtable: Option<MemTable>,
    sstables: Vec<SsTableReader>,
    wal_path: PathBuf,
    data_dir: PathBuf,
    memtable_max_size: usize,
    sstable_counter: AtomicU64,
}

impl StorageEngine {
    pub fn new(dir: PathBuf, memtable_max_size: usize) -> Result<Self> {
        // Ensure directory exists
        std::fs::create_dir_all(&dir)?;
        
        let wal_path = dir.join("data.wal");
        
        // Create WAL
        let wal = Wal::new(&wal_path, 8192)?; // 8KB buffer
        
        // Recover from WAL
        let entries = Wal::recover(&wal_path)?;
        
        // Rebuild MemTable
        let mut memtable = MemTable::new(memtable_max_size);
        for entry in entries {
            memtable.put(entry.key, entry.value, entry.timestamp)?;
        }
        
        // Load existing SSTables from disk
        let mut sstables = Vec::new();
        let mut max_sstable_id = 0u64;
        
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            
            // Look for .sst files
            if path.extension().and_then(|s| s.to_str()) == Some("sst") {
                match SsTableReader::open(path.clone()) {
                    Ok(reader) => {
                        println!("Loaded SSTable: {:?}", path);
                        sstables.push(reader);
                        
                        // Track highest SSTable ID for counter
                        if let Some(stem) = path.file_stem() {
                            if let Some(id_str) = stem.to_str() {
                                if let Ok(id) = id_str.parse::<u64>() {
                                    max_sstable_id = max_sstable_id.max(id);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Warning: Failed to load SSTable {:?}: {}", path, e);
                        // Continue with other SSTables
                    }
                }
            }
        }
        
        // Sort SSTables by ID (oldest to newest)
        sstables.sort_by_key(|reader| {
            reader.info().path.file_stem()
                .and_then(|s| s.to_str())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0)
        });
        
        println!("Loaded {} existing SSTables", sstables.len());
        
        Ok(StorageEngine {
            wal,
            memtable,
            immutable_memtable: None,
            sstables,
            wal_path,
            data_dir: dir,
            memtable_max_size,
            sstable_counter: AtomicU64::new(max_sstable_id + 1),
        })
    }
    
    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        // Get current timestamp
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        let entry = Entry { 
            key: key.clone(), 
            value: value.clone(),
            timestamp,
        };
        
        // 1. Write to WAL (durability)
        self.wal.append(&entry)?;
        
        // 2. Write to MemTable (fast access)
        let is_full = self.memtable.put(key, value, timestamp)?;
        
        // 3. If MemTable full, flush to disk
        if is_full {
            self.flush_memtable()?;
        }
        
        Ok(())
    }
    
    /// Get a value by key
    /// 
    /// Search order:
    /// 1. MemTable (newest data)
    /// 2. Immutable MemTable (if exists, being flushed)
    /// 3. SSTables (newest to oldest)
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // 1. Check MemTable first (newest data)
        if let Some(value) = self.memtable.get(key) {
            return Ok(Some(value));
        }
        
        // 2. Check immutable MemTable if it exists
        if let Some(ref imm) = self.immutable_memtable {
            if let Some(value) = imm.get(key) {
                return Ok(Some(value));
            }
        }
        
        // 3. Check SSTables (newest to oldest)
        // Iterate in reverse to get most recent value first
        for (i, sstable) in self.sstables.iter_mut().enumerate().rev() {
            match sstable.get(key) {
                Ok(Some((value, _timestamp))) => {
                    return Ok(Some(value));
                }
                Ok(None) => {
                    // Key not in this SSTable, continue searching
                    continue;
                }
                Err(crate::StorageError::CorruptedData(msg)) => {
                    // Graceful degradation: log and continue
                    eprintln!("Warning: SSTable {} corrupted ({}), skipping", i, msg);
                    // TODO: Mark SSTable as bad, emit metric
                    continue;
                }
                Err(e) => {
                    // I/O errors should propagate
                    return Err(e);
                }
            }
        }
        
        // Key not found anywhere
        Ok(None)
    }
    
    /// Flush MemTable to disk as an SSTable
    /// 
    /// This is the CRITICAL function that prevents data loss!
    /// 
    /// Steps:
    /// 1. Generate unique filename
    /// 2. Write all MemTable entries to SSTable
    /// 3. Open SSTable for reading
    /// 4. Add to list of SSTables
    /// 5. Clear MemTable
    fn flush_memtable(&mut self) -> Result<()> {
        if self.memtable.is_empty() {
            return Ok(());
        }
        
        println!("Flushing MemTable to SSTable...");
        
        // 1. Generate unique SSTable filename
        let sstable_id = self.sstable_counter.fetch_add(1, Ordering::SeqCst);
        let sstable_path = self.data_dir.join(format!("{:06}.sst", sstable_id));
        
        // 2. Create SSTable writer
        let mut writer = SsTableWriter::new(
            sstable_path.clone(), 
            DEFAULT_BLOCK_SIZE
        )?;
        
        // 3. Write all entries from MemTable (already sorted in BTreeMap)
        let entries = self.memtable.entries_with_timestamps();
        
        println!("Writing {} entries to SSTable {:06}.sst", entries.len(), sstable_id);
        
        for (key, value, timestamp) in entries {
            writer.add(&key, &value, timestamp)?;
        }
        
        // 4. Finalize the SSTable
        writer.finish()?;
        
        let file_size = std::fs::metadata(&sstable_path)?.len();
        println!("Flushed SSTable: {} bytes", file_size);
        
        // 5. Open SSTable for reading
        let reader = SsTableReader::open(sstable_path)?;
        self.sstables.push(reader);
        
        // 6. Clear MemTable (safe now that data is on disk)
        self.memtable = MemTable::new(self.memtable_max_size);
        
        // TODO: Clear WAL since data is now in SSTable (Week 2)
        
        Ok(())
    }
    
    /// Range scan across all storage levels
    /// 
    /// Returns all entries where start <= key < end
    /// Merges results from MemTable and all SSTables
    pub fn scan(&mut self, start: &[u8], end: &[u8]) 
        -> Result<Vec<(Vec<u8>, Vec<u8>, u64)>> 
    {
        let mut results = Vec::new();
        
        // 1. Scan MemTable
        let memtable_entries = self.memtable.scan_with_timestamps(start, end);
        results.extend(memtable_entries);
        
        // 2. Scan immutable MemTable if exists
        if let Some(ref imm) = self.immutable_memtable {
            let imm_entries = imm.scan_with_timestamps(start, end);
            results.extend(imm_entries);
        }
        
        // 3. Scan all SSTables
        for (i, sstable) in self.sstables.iter_mut().enumerate() {
            match sstable.scan(start, end) {
                Ok(entries) => {
                    results.extend(entries);
                }
                Err(crate::StorageError::CorruptedData(msg)) => {
                    eprintln!("Warning: SSTable {} corrupted during scan: {}", i, msg);
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
        
        // 4. Merge and deduplicate results
        // Sort by key, then by timestamp (newest first)
        results.sort_by(|a, b| {
            a.0.cmp(&b.0).then(b.2.cmp(&a.2))
        });
        
        // Deduplicate: keep only newest value for each key
        results.dedup_by(|a, b| a.0 == b.0);
        
        Ok(results)
    }
    
    /// Get diagnostic information
    pub fn stats(&self) -> EngineStats {
        EngineStats {
            memtable_entries: self.memtable.len(),
            memtable_bytes: self.memtable.size_bytes(),
            num_sstables: self.sstables.len(),
            immutable_memtable_entries: self.immutable_memtable
                .as_ref()
                .map(|m| m.len())
                .unwrap_or(0),
        }
    }
}

/// Diagnostic statistics
#[derive(Debug)]
pub struct EngineStats {
    pub memtable_entries: usize,
    pub memtable_bytes: usize,
    pub num_sstables: usize,
    pub immutable_memtable_entries: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    
    #[test]
    fn test_storage_engine_basic() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let mut engine = StorageEngine::new(
            temp_dir.path().to_path_buf(),
            1024 * 1024, // 1MB memtable
        )?;
        
        // Write some data
        engine.put(b"key1".to_vec(), b"value1".to_vec())?;
        engine.put(b"key2".to_vec(), b"value2".to_vec())?;
        
        // Read it back
        assert_eq!(engine.get(b"key1")?, Some(b"value1".to_vec()));
        assert_eq!(engine.get(b"key2")?, Some(b"value2".to_vec()));
        assert_eq!(engine.get(b"key3")?, None);
        
        Ok(())
    }
    
    #[test]
    fn test_storage_engine_flush() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let mut engine = StorageEngine::new(
            temp_dir.path().to_path_buf(),
            200, // Tiny memtable to force flush
        )?;
        
        // Write enough data to trigger flush
        for i in 0..100 {
            let key = format!("key{:03}", i);
            let value = format!("value{:03}", i);
            engine.put(key.into_bytes(), value.into_bytes())?;
        }
        
        // Should have created at least one SSTable
        let stats = engine.stats();
        println!("Stats: {:?}", stats);
        assert!(stats.num_sstables > 0);
        
        // Data should still be readable
        assert_eq!(engine.get(b"key000")?, Some(b"value000".to_vec()));
        assert_eq!(engine.get(b"key050")?, Some(b"value050".to_vec()));
        assert_eq!(engine.get(b"key099")?, Some(b"value099".to_vec()));
        
        Ok(())
    }
    
    #[test]
    fn test_storage_engine_persistence() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let dir_path = temp_dir.path().to_path_buf();
        
        // Write data with first engine
        {
            let mut engine = StorageEngine::new(dir_path.clone(), 500)?;
            
            for i in 0..50 {
                let key = format!("persist{:03}", i);
                let value = format!("data{:03}", i);
                engine.put(key.into_bytes(), value.into_bytes())?;
            }
            
            // Explicitly drop to flush everything
        }
        
        // Open new engine and verify data persists
        {
            let mut engine = StorageEngine::new(dir_path.clone(), 500)?;
            
            assert_eq!(engine.get(b"persist000")?, Some(b"data000".to_vec()));
            assert_eq!(engine.get(b"persist025")?, Some(b"data025".to_vec()));
            assert_eq!(engine.get(b"persist049")?, Some(b"data049".to_vec()));
        }
        
        Ok(())
    }
}