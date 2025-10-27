use crate::{Entry, Result, Wal, MemTable};
use std::path::PathBuf;
use std::time::SystemTime;

pub struct StorageEngine {
    wal: Wal,
    memtable: MemTable,
    #[allow(dead_code)]
    wal_path: PathBuf,
    memtable_max_size: usize,
}

impl StorageEngine {
    pub fn new(dir: PathBuf, memtable_max_size: usize) -> Result<Self> {
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
        
        Ok(StorageEngine {
            wal,
            memtable,
            wal_path,
            memtable_max_size,
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
    
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        // For now, only check MemTable
        // Later: also check SSTables on disk
        self.memtable.get(key)
    }
    
    fn flush_memtable(&mut self) -> Result<()> {
        // TODO: Write MemTable to SSTable (Day 6-7)
        // For now, just clear it
        println!("MemTable full! Would flush to SSTable here...");
        
        // Create new MemTable
        self.memtable = MemTable::new(self.memtable_max_size);
        
        // TODO: Delete old WAL segment (data now in SSTable)
        
        Ok(())
    }
}