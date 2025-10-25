use timeseries_lsm::{StorageEngine, Result};
use tempfile::tempdir;
use std::time::SystemTime;

#[test]
fn test_basic_put_and_get() -> Result<()> {
    let dir = tempdir().unwrap();
    let mut engine = StorageEngine::new(dir.path().to_path_buf(), 1024 * 1024)?;
    
    // Write some data
    engine.put(b"key1".to_vec(), b"value1".to_vec())?;
    engine.put(b"key2".to_vec(), b"value2".to_vec())?;
    engine.put(b"key3".to_vec(), b"value3".to_vec())?;
    
    // Read it back
    assert_eq!(engine.get(b"key1"), Some(b"value1".to_vec()));
    assert_eq!(engine.get(b"key2"), Some(b"value2".to_vec()));
    assert_eq!(engine.get(b"key3"), Some(b"value3".to_vec()));
    assert_eq!(engine.get(b"nonexistent"), None);
    
    Ok(())
}

#[test]
fn test_overwrite() -> Result<()> {
    let dir = tempdir().unwrap();
    let mut engine = StorageEngine::new(dir.path().to_path_buf(), 1024 * 1024)?;
    
    // Write initial value
    engine.put(b"key".to_vec(), b"value1".to_vec())?;
    assert_eq!(engine.get(b"key"), Some(b"value1".to_vec()));
    
    // Overwrite
    engine.put(b"key".to_vec(), b"value2".to_vec())?;
    assert_eq!(engine.get(b"key"), Some(b"value2".to_vec()));
    
    Ok(())
}

#[test]
fn test_crash_recovery() -> Result<()> {
    let dir = tempdir().unwrap();
    let dir_path = dir.path().to_path_buf();
    
    // Write some data
    {
        let mut engine = StorageEngine::new(dir_path.clone(), 1024 * 1024)?;
        engine.put(b"key1".to_vec(), b"value1".to_vec())?;
        engine.put(b"key2".to_vec(), b"value2".to_vec())?;
        engine.put(b"key3".to_vec(), b"value3".to_vec())?;
        // Engine goes out of scope (simulates crash after WAL flush)
    }
    
    // Recover from WAL
    {
        let engine = StorageEngine::new(dir_path.clone(), 1024 * 1024)?;
        
        // Data should be recovered from WAL
        assert_eq!(engine.get(b"key1"), Some(b"value1".to_vec()));
        assert_eq!(engine.get(b"key2"), Some(b"value2".to_vec()));
        assert_eq!(engine.get(b"key3"), Some(b"value3".to_vec()));
    }
    
    Ok(())
}

#[test]
fn test_large_values() -> Result<()> {
    let dir = tempdir().unwrap();
    let mut engine = StorageEngine::new(dir.path().to_path_buf(), 1024 * 1024)?;
    
    // Write large values (1MB each)
    let large_value = vec![42u8; 1024 * 1024];
    
    engine.put(b"large1".to_vec(), large_value.clone())?;
    engine.put(b"large2".to_vec(), large_value.clone())?;
    
    // Read back
    assert_eq!(engine.get(b"large1"), Some(large_value.clone()));
    assert_eq!(engine.get(b"large2"), Some(large_value));
    
    Ok(())
}

#[test]
fn test_many_small_writes() -> Result<()> {
    let dir = tempdir().unwrap();
    let mut engine = StorageEngine::new(dir.path().to_path_buf(), 1024 * 1024)?;
    
    // Write 1000 small entries
    for i in 0..1000 {
        let key = format!("key_{:04}", i);
        let value = format!("value_{:04}", i);
        engine.put(key.into_bytes(), value.into_bytes())?;
    }
    
    // Read them back
    for i in 0..1000 {
        let key = format!("key_{:04}", i);
        let expected_value = format!("value_{:04}", i);
        assert_eq!(
            engine.get(key.as_bytes()),
            Some(expected_value.into_bytes())
        );
    }
    
    Ok(())
}

#[test]
fn test_memtable_flush() -> Result<()> {
    let dir = tempdir().unwrap();
    // Small memtable to trigger flush (10KB)
    let mut engine = StorageEngine::new(dir.path().to_path_buf(), 10 * 1024)?;
    
    // Write enough data to trigger memtable flush
    for i in 0..1000 {
        let key = format!("key_{:04}", i);
        let value = vec![0u8; 100]; // 100 bytes per value
        engine.put(key.into_bytes(), value)?;
    }
    
    // Should still be able to read recent data (in current memtable)
    let key = "key_0999";
    assert!(engine.get(key.as_bytes()).is_some());
    
    // Note: We can't read flushed data yet (SSTables not implemented)
    // This test just verifies flush doesn't crash
    
    Ok(())
}

#[test]
fn test_empty_database() -> Result<()> {
    let dir = tempdir().unwrap();
    let engine = StorageEngine::new(dir.path().to_path_buf(), 1024 * 1024)?;
    
    // Reading from empty database should return None
    assert_eq!(engine.get(b"any_key"), None);
    
    Ok(())
}

#[test]
fn test_binary_keys_and_values() -> Result<()> {
    let dir = tempdir().unwrap();
    let mut engine = StorageEngine::new(dir.path().to_path_buf(), 1024 * 1024)?;
    
    // Test with binary data (not UTF-8)
    let binary_key = vec![0xFF, 0xFE, 0xFD, 0xFC];
    let binary_value = vec![0x00, 0x01, 0x02, 0x03, 0x04];
    
    engine.put(binary_key.clone(), binary_value.clone())?;
    assert_eq!(engine.get(&binary_key), Some(binary_value));
    
    Ok(())
}

#[test]
fn test_sequential_time_series_workload() -> Result<()> {
    let dir = tempdir().unwrap();
    let mut engine = StorageEngine::new(dir.path().to_path_buf(), 1024 * 1024)?;
    
    // Simulate time-series data: sensor readings every second
    let base_time = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    
    for i in 0..100 {
        let timestamp = base_time + i;
        let key = format!("sensor_123_{}", timestamp);
        let value = format!("{{\"temp\": 25.{}, \"humidity\": 60}}", i);
        
        engine.put(key.into_bytes(), value.into_bytes())?;
    }
    
    // Verify we can read the data back
    for i in 0..100 {
        let timestamp = base_time + i;
        let key = format!("sensor_123_{}", timestamp);
        assert!(engine.get(key.as_bytes()).is_some());
    }
    
    Ok(())
}

#[test]
fn test_wal_memtable_integration() -> Result<()> {
    let dir = tempdir().unwrap();
    let dir_path = dir.path().to_path_buf();
    
    println!("Testing WAL + MemTable integration...");
    
    // Phase 1: Write data
    {
        let mut engine = StorageEngine::new(dir_path.clone(), 1024 * 1024)?;
        
        println!("Writing 10 entries...");
        for i in 0..10 {
            let key = format!("metric_{}", i);
            let value = format!("value_{}", i);
            engine.put(key.into_bytes(), value.into_bytes())?;
        }
        
        // Verify in-memory reads work
        println!("Verifying in-memory reads...");
        for i in 0..10 {
            let key = format!("metric_{}", i);
            let expected = format!("value_{}", i);
            assert_eq!(
                engine.get(key.as_bytes()),
                Some(expected.into_bytes()),
                "Failed to read key: {}", key
            );
        }
        
        println!("Phase 1 complete: All writes successful");
    }
    
    // Phase 2: Crash recovery
    {
        println!("Simulating crash and recovery...");
        let engine = StorageEngine::new(dir_path.clone(), 1024 * 1024)?;
        
        println!("Verifying recovered data...");
        for i in 0..10 {
            let key = format!("metric_{}", i);
            let expected = format!("value_{}", i);
            assert_eq!(
                engine.get(key.as_bytes()),
                Some(expected.into_bytes()),
                "Failed to recover key: {}", key
            );
        }
        
        println!("Phase 2 complete: Recovery successful");
    }
    
    println!("✅ WAL + MemTable integration test passed!");
    Ok(())
}