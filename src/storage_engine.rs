// Step 1: Add Arc/Mutex and background thread infrastructure
// Replace entire src/storage_engine.rs with this

use crate::{Entry, Result, Wal, MemTable};
use crate::sstable::{SsTableWriter, SsTableReader};
use std::path::PathBuf;
use std::time::SystemTime;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use crossbeam::channel::{self, Sender, Receiver};
use std::thread;

const DEFAULT_BLOCK_SIZE: usize = 16 * 1024;

/// Message for background flush thread
enum FlushMessage {
    Flush {
        memtable: MemTable,  // Take ownership, not Arc
        path: PathBuf,
        sstable_id: u64,
    },
    Shutdown,
}

/// Result from background flush
struct FlushResult {
    sstable_id: u64,
    path: PathBuf,
}

pub struct StorageEngine {
    wal: Wal,
    memtable: MemTable,
    immutable_memtable: Option<MemTable>,
    sstables: Vec<SsTableReader>,
    wal_path: PathBuf,
    data_dir: PathBuf,
    memtable_max_size: usize,
    sstable_counter: AtomicU64,
    
    // Background flush (optional - can be None for sync mode)
    flush_tx: Option<Sender<FlushMessage>>,
    flush_rx: Option<Receiver<FlushResult>>,
    _flush_thread: Option<thread::JoinHandle<()>>,
    background_flush_enabled: bool,
}

impl StorageEngine {
    /// Create new StorageEngine with optional background flush
    pub fn new(dir: PathBuf, memtable_max_size: usize) -> Result<Self> {
        Self::new_with_config(dir, memtable_max_size, true)
    }
    
    /// Create StorageEngine with configurable background flush
    pub fn new_with_config(
        dir: PathBuf,
        memtable_max_size: usize,
        background_flush: bool,
    ) -> Result<Self> {
        std::fs::create_dir_all(&dir)?;
        
        let wal_path = dir.join("data.wal");
        let wal = Wal::new(&wal_path, 8192)?;
        
        // Recover from WAL
        let entries = Wal::recover(&wal_path)?;
        let mut memtable = MemTable::new(memtable_max_size);
        for entry in entries {
            memtable.put(entry.key, entry.value, entry.timestamp)?;
        }
        
        // Load existing SSTables
        let mut sstables = Vec::new();
        let mut max_sstable_id = 0u64;
        
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            
            if path.extension().and_then(|s| s.to_str()) == Some("sst") {
                match SsTableReader::open(path.clone()) {
                    Ok(reader) => {
                        println!("Loaded SSTable: {:?}", path);
                        sstables.push(reader);
                        
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
                    }
                }
            }
        }
        
        sstables.sort_by_key(|reader| {
            reader.info().path.file_stem()
                .and_then(|s| s.to_str())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0)
        });
        
        println!("Loaded {} existing SSTables", sstables.len());
        
        // Setup background flush if enabled
        let (flush_tx, flush_rx, flush_thread) = if background_flush {
            let (tx, rx_internal) = channel::unbounded();
            let (result_tx, rx) = channel::unbounded();
            
            let thread = Some(Self::spawn_flush_thread(rx_internal, result_tx));
            
            (Some(tx), Some(rx), thread)
        } else {
            (None, None, None)
        };
        
        Ok(StorageEngine {
            wal,
            memtable,
            immutable_memtable: None,
            sstables,
            wal_path,
            data_dir: dir,
            memtable_max_size,
            sstable_counter: AtomicU64::new(max_sstable_id + 1),
            flush_tx,
            flush_rx,
            _flush_thread: flush_thread,
            background_flush_enabled: background_flush,
        })
    }
    
    /// Spawn background flush thread
    fn spawn_flush_thread(
        rx: Receiver<FlushMessage>,
        result_tx: Sender<FlushResult>,
    ) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            println!("🧵 Background flush thread started");
            
            while let Ok(msg) = rx.recv() {
                match msg {
                    FlushMessage::Flush { memtable, path, sstable_id } => {
                        println!("🧵 Background: Flushing to {:?}", path);
                        
                        match Self::flush_memtable_to_disk(memtable, &path) {
                            Ok(()) => {
                                println!("🧵 Background: Flush complete");
                                let _ = result_tx.send(FlushResult {
                                    sstable_id,
                                    path,
                                });
                            }
                            Err(e) => {
                                eprintln!("🧵 Background flush FAILED: {}", e);
                            }
                        }
                    }
                    FlushMessage::Shutdown => {
                        println!("🧵 Background flush thread shutting down");
                        break;
                    }
                }
            }
        })
    }
    
    /// Flush memtable to disk (static method for background thread)
    fn flush_memtable_to_disk(memtable: MemTable, path: &PathBuf) -> Result<()> {
        if memtable.is_empty() {
            return Ok(());
        }
        
        let mut writer = SsTableWriter::new(path.clone(), DEFAULT_BLOCK_SIZE)?;
        
        let entries = memtable.entries_with_timestamps();
        println!("Writing {} entries to SSTable", entries.len());
        
        for (key, value, timestamp) in entries {
            writer.add(&key, &value, timestamp)?;
        }
        
        writer.finish()?;
        
        let file_size = std::fs::metadata(path)?.len();
        println!("Flushed SSTable: {} bytes", file_size);
        
        Ok(())
    }
    
    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        let entry = Entry {
            key: key.clone(),
            value: value.clone(),
            timestamp,
        };
        
        // 1. Write to WAL
        self.wal.append(&entry)?;
        
        // 2. Check for completed background flushes
        self.check_flush_completion()?;
        
        // 3. Write to MemTable
        let is_full = self.memtable.put(key, value, timestamp)?;
        
        // 4. If full, trigger flush (background or sync)
        if is_full {
            if self.background_flush_enabled {
                self.trigger_background_flush()?;
            } else {
                self.flush_memtable_sync()?;
            }
        }
        
        Ok(())
    }
    
    /// Trigger background flush (non-blocking!)
    fn trigger_background_flush(&mut self) -> Result<()> {
        // Wait if there's already an immutable being flushed
        let max_wait = 100; // 100ms max wait
        let mut waited = 0;
        
        while self.immutable_memtable.is_some() && waited < max_wait {
            self.check_flush_completion()?;
            
            if self.immutable_memtable.is_some() {
                thread::sleep(std::time::Duration::from_millis(1));
                waited += 1;
            }
        }
        
        // If still waiting, fall back to sync flush
        if self.immutable_memtable.is_some() {
            println!("⚠️  Background flush too slow, falling back to sync");
            return self.flush_memtable_sync();
        }
        
        println!("🚀 Triggering background flush...");
        
        // Move current memtable to immutable (no clone needed!)
        let old_memtable = std::mem::replace(
            &mut self.memtable,
            MemTable::new(self.memtable_max_size)
        );
        
        // Generate path
        let sstable_id = self.sstable_counter.fetch_add(1, Ordering::SeqCst);
        let sstable_path = self.data_dir.join(format!("{:06}.sst", sstable_id));
        
        // Keep a reference for reads (take ownership for flush)
        // We'll create entries list before moving
        let entries = old_memtable.entries_with_timestamps();
        let entries_clone = entries.clone(); // Clone just the Vec, not MemTable
        
        // Recreate MemTable from entries for immutable reads
        let mut immutable = MemTable::new(self.memtable_max_size);
        for (key, value, timestamp) in entries_clone {
            let _ = immutable.put(key, value, timestamp);
        }
        self.immutable_memtable = Some(immutable);
        
        // Send to background (non-blocking!)
        if let Some(ref tx) = self.flush_tx {
            tx.send(FlushMessage::Flush {
                memtable: old_memtable,
                path: sstable_path,
                sstable_id,
            }).map_err(|e| crate::StorageError::Io(
                std::io::Error::new(std::io::ErrorKind::Other, e.to_string())
            ))?;
        }
        
        println!("✅ Background flush triggered (writes continue!)");
        
        Ok(())
    }
    
    /// Check if background flush completed (non-blocking)
    fn check_flush_completion(&mut self) -> Result<()> {
        if let Some(ref rx) = self.flush_rx {
            while let Ok(result) = rx.try_recv() {
                println!("✅ Flush completed: {:?}", result.path);
                
                // Open flushed SSTable
                let reader = SsTableReader::open(result.path)?;
                self.sstables.push(reader);
                
                // Clear immutable
                self.immutable_memtable = None;
            }
        }
        
        Ok(())
    }
    
    /// Synchronous flush (blocking)
    fn flush_memtable_sync(&mut self) -> Result<()> {
        if self.memtable.is_empty() {
            return Ok(());
        }
        
        println!("Flushing MemTable to SSTable (sync)...");
        
        let sstable_id = self.sstable_counter.fetch_add(1, Ordering::SeqCst);
        let sstable_path = self.data_dir.join(format!("{:06}.sst", sstable_id));
        
        // Clone memtable data, then clear
        let memtable_to_flush = std::mem::replace(
            &mut self.memtable,
            MemTable::new(self.memtable_max_size)
        );
        
        Self::flush_memtable_to_disk(memtable_to_flush, &sstable_path)?;
        
        let reader = SsTableReader::open(sstable_path)?;
        self.sstables.push(reader);
        
        Ok(())
    }
    
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // 1. Check active MemTable
        if let Some(value) = self.memtable.get(key) {
            return Ok(Some(value));
        }
        
        // 2. Check immutable MemTable
        if let Some(ref imm) = self.immutable_memtable {
            if let Some(value) = imm.get(key) {
                return Ok(Some(value));
            }
        }
        
        // 3. Check SSTables
        for (i, sstable) in self.sstables.iter_mut().enumerate().rev() {
            match sstable.get(key) {
                Ok(Some((value, _timestamp))) => return Ok(Some(value)),
                Ok(None) => continue,
                Err(crate::StorageError::CorruptedData(msg)) => {
                    eprintln!("Warning: SSTable {} corrupted ({}), skipping", i, msg);
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
        
        Ok(None)
    }
    
    pub fn scan(&mut self, start: &[u8], end: &[u8])
        -> Result<Vec<(Vec<u8>, Vec<u8>, u64)>>
    {
        let mut results = Vec::new();
        
        // Scan MemTable
        results.extend(self.memtable.scan_with_timestamps(start, end));
        
        // Scan immutable
        if let Some(ref imm) = self.immutable_memtable {
            results.extend(imm.scan_with_timestamps(start, end));
        }
        
        // Scan SSTables
        for (i, sstable) in self.sstables.iter_mut().enumerate() {
            match sstable.scan(start, end) {
                Ok(entries) => results.extend(entries),
                Err(crate::StorageError::CorruptedData(msg)) => {
                    eprintln!("Warning: SSTable {} corrupted: {}", i, msg);
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
        
        // Merge and deduplicate
        results.sort_by(|a, b| a.0.cmp(&b.0).then(b.2.cmp(&a.2)));
        results.dedup_by(|a, b| a.0 == b.0);
        
        Ok(results)
    }
    
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

impl Drop for StorageEngine {
    fn drop(&mut self) {
        // Shutdown background thread
        if let Some(ref tx) = self.flush_tx {
            let _ = tx.send(FlushMessage::Shutdown);
        }
    }
}

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
            1024 * 1024,
        )?;
        
        engine.put(b"key1".to_vec(), b"value1".to_vec())?;
        engine.put(b"key2".to_vec(), b"value2".to_vec())?;
        
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
            200,
        )?;
        
        for i in 0..100 {
            let key = format!("key{:03}", i);
            let value = format!("value{:03}", i);
            engine.put(key.into_bytes(), value.into_bytes())?;
        }
        
        // Give background thread time to complete
        thread::sleep(std::time::Duration::from_millis(100));
        engine.check_flush_completion()?;
        
        let stats = engine.stats();
        println!("Stats: {:?}", stats);
        assert!(stats.num_sstables > 0);
        
        assert_eq!(engine.get(b"key000")?, Some(b"value000".to_vec()));
        assert_eq!(engine.get(b"key050")?, Some(b"value050".to_vec()));
        assert_eq!(engine.get(b"key099")?, Some(b"value099".to_vec()));
        
        Ok(())
    }
    
    #[test]
    fn test_storage_engine_persistence() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let dir_path = temp_dir.path().to_path_buf();
        
        {
            let mut engine = StorageEngine::new(dir_path.clone(), 500)?;
            
            for i in 0..50 {
                let key = format!("persist{:03}", i);
                let value = format!("data{:03}", i);
                engine.put(key.into_bytes(), value.into_bytes())?;
            }
            
            thread::sleep(std::time::Duration::from_millis(100));
        }
        
        {
            let mut engine = StorageEngine::new(dir_path.clone(), 500)?;
            
            assert_eq!(engine.get(b"persist000")?, Some(b"data000".to_vec()));
            assert_eq!(engine.get(b"persist025")?, Some(b"data025".to_vec()));
            assert_eq!(engine.get(b"persist049")?, Some(b"data049".to_vec()));
        }
        
        Ok(())
    }
}