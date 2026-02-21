# QuokkaDB Storage Architecture Summary

This document describes the architecture of the `storage` directory, which implements a high-performance, log-structured merge-tree (LSM) engine for QuokkaDB.

## 1. Purpose
The storage engine provides persistent, versioned (MVCC), and crash-safe storage for BSON documents. It manages the lifecycle of data from in-memory ingestion to multi-level disk residency, ensuring transactional consistency through a Write-Ahead Log (WAL) and a Manifest system.

## 2. Invariants
- **Key Ordering**: All internal keys are byte-comparable and follow the schema: `[record_key][!sequence_number][operation_type]`. The sequence number is inverted (`!`) so that newer versions of the same record key sort first.
- **Level Integrity**: 
    - **L0**: Files may have overlapping key ranges.
    - **L1+**: Files must be non-overlapping and sorted by key range within each level.
- **Durability**: No write is acknowledged until it is successfully appended to the WAL and synced to disk (depending on sync settings).
- **MVCC**: Every entry is tagged with a 56-bit sequence number. Read snapshots ensure that only versions with `sequence <= snapshot_id` are visible.
- **Immutability**: Once an SSTable is written to disk, it is immutable. Changes are handled via new versions or deletion tombstones.

## 3. Data Structures

| Structure | Location | Description |
| :--- | :--- | :--- |
| `StorageEngine` | `storage_engine.rs` | The central coordinator. Manages the write pipeline, memtable rotations, and orchestration of flushes. |
| `Memtable` | `memtable.rs` | In-memory `SkipMap` (crossbeam) storing the most recent writes. Rotates to "Immutable Memtable" when size thresholds are reached. |
| `LsmTree` | `lsm_tree.rs` | Combines the active memtable, immutable memtables, and the persisted `ManifestState` into a single searchable view. |
| `SSTable` | `sstable/` | Persistent "Sorted String Tables". Contains Data Blocks, Index Blocks, Bloom Filters, and Metadata/Properties. |
| `InternalKey` | `internal_key.rs` | The physical key format that allows for collection isolation and MVCC versioning. |
| `Catalog` | `catalog.rs` | Manages collection and index metadata, including IDs and names. |
| `Manifest` | `manifest.rs` | A rolling log of "Version Edits" (e.g., file additions, deletions, WAL rotations) that defines the current state of the LSM tree. |

## 4. Algorithms

### Write Pipeline (Ingestion)
1. **Batching**: Writers enter a queue; a "Leader" is elected to group writes.
2. **WAL Append**: The leader writes the batch to the `WriteAheadLog`.
3. **Memtable Update**: Writes are inserted into the current `Memtable`.
4. **Visibility**: The `last_visible_seq` is updated only after the WAL and Memtable are consistent.

### Read Path (Query)
1. **Snapshotting**: Reads use a snapshot sequence number to ensure point-in-time consistency.
2. **Search Order**: Active Memtable → Immutable Memtables (Newest to Oldest) → L0 SSTables → Ln SSTables.
3. **Filtering**: Bloom filters are checked before opening SSTable data blocks to minimize disk I/O.

### Compaction (2L-Spooky Strategy)
- Defined in `compaction_picker.rs`.
- **Full Compaction**: Used for higher levels. Merges all overlapping files from source to target.
- **Partial Compaction**: Limited to the bottom two levels. Merges specific partitions to reduce write amplification.
- **Scores**: Compaction is triggered when a level's size or file count exceeds its target (score > 1.0).

## 5. Components Interaction
- **Flush Management**: `StorageEngine` triggers a rotation. The `Memtable` is moved to a background queue handled by `FlushManager`, which uses `SSTableWriter` to create a new file and then notifies the engine to update the `Manifest`.
- **Append Log**: Both `WAL` and `Manifest` leverage a generic `AppendLog<F>` which handles buffering, checksumming, and file rotation.
- **Block Cache**: `SSTableReader` uses a central `BlockCache` to store decompressed blocks in memory, reducing redundant disk reads and decompression overhead.
- **Persistence**: `Manifest` is the "source of truth". On restart, the engine reads the `CURRENT` file to find the latest Manifest, reconstructs the `LsmTree`, and replays the `WAL` to recover any unflushed memtable data.
