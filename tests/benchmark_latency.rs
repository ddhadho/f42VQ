use timeseries_lsm::{StorageEngine, Result};
use tempfile::TempDir;
use std::time::Instant;

#[test]
#[ignore]
fn benchmark_write_latency() -> Result<()> {
    println!("\n=== Write Latency Benchmark ===\n");
    
    let temp_dir = TempDir::new()?;
    let mut engine = StorageEngine::new(
        temp_dir.path().to_path_buf(),
        200, // Small memtable to trigger frequent flushes
    )?;
    
    let mut latencies = Vec::new();
    
    // Write 1000 entries, measure each
    for i in 0..1000 {
        let key = format!("key{:06}", i);
        let value = vec![0u8; 100];
        
        let start = Instant::now();
        engine.put(key.into_bytes(), value)?;
        let duration = start.elapsed();
        
        latencies.push(duration.as_micros());
    }
    
    // Calculate stats
    latencies.sort();
    let p50 = latencies[latencies.len() / 2];
    let p99 = latencies[latencies.len() * 99 / 100];
    let p999 = latencies[latencies.len() * 999 / 1000];
    let max = latencies[latencies.len() - 1];
    
    println!("Write Latency Distribution:");
    println!("  p50:  {}μs", p50);
    println!("  p99:  {}μs", p99);
    println!("  p99.9: {}μs", p999);
    println!("  max:  {}μs", max);
    println!("\n✅ p99 < 1000μs (1ms): {}", p99 < 1000);
    
    Ok(())
}
