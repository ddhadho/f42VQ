# SSTable Implementation

## Overview
Sorted String Table (SSTable) - immutable on-disk storage format for LSM-tree.

## Components

### Writer (`writer.rs`)
Writes sorted data from MemTable to disk.

**Features:**
- Block-based storage (configurable block size)
- Prefix compression for keys
- Snappy compression per block
- Bloom filter for fast negative lookups
- Index for efficient key location

**Usage:**
```rust
let mut writer = SsTableWriter::new(path)?;
writer.add(b"key1", b"value1", timestamp1)?;
writer.add(b"key2", b"value2", timestamp2)?;
writer.finish()?;
```

### Block Builder (`block.rs`)
Accumulates entries and compresses them into blocks.

**Prefix Compression:**
- Stores shared prefix length + unique suffix
- Huge savings for time-series keys: `sensor:123:temp`, `sensor:123:humidity`

### Format (`format.rs`)
On-disk layout:
```
[Header: 16 bytes]
  - Magic number (4 bytes)
  - Version (4 bytes)
  - Block size (4 bytes)
  - Number of entries (4 bytes)

[Data Blocks: Variable]
  - Compressed key-value pairs
  - Prefix compressed keys

[Index: Variable]
  - First key of each block + offset
  - Enables binary search

[Bloom Filter: Variable]
  - Probabilistic filter
  - Avoids disk reads for missing keys

[Footer: 24 bytes]
  - Index offset (8 bytes)
  - Bloom filter offset (8 bytes)
  - Checksum (4 bytes)
  - Magic number (4 bytes)
```

## Status
- ✅ Writer implemented
- ⏳ Reader (Day 7)
- ⏳ Integration with compaction (Week 3)

## Tests
```bash
cargo test sstable::writer
```