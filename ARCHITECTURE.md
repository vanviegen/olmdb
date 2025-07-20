# OLMDB Architecture

> **Note**: This is a focused architectural overview. For detailed usage examples and API documentation, see [README.md](README.md). For AI contribution guidelines, see [AI_CONTRIBUTING.md](AI_CONTRIBUTING.md).

## High-Level Architecture

OLMDB implements optimistic concurrency control over LMDB through a 5-layer architecture:

```
┌─────────────────────────────────────┐
│    High-Level TypeScript API       │  ← User-facing: transact(), put(), get()
│         (src/olmdb.ts)              │
├─────────────────────────────────────┤
│    Low-Level TypeScript API        │  ← Native module loader & interface
│        (src/lowlevel.ts)            │
├─────────────────────────────────────┤
│         NAPI Module                 │  ← JavaScript-C bridge
│       (lowlevel/napi.c)             │
├─────────────────────────────────────┤
│      OLMDB Client Library           │  ← Core optimistic transaction logic
│  (lowlevel/transaction_client.c)    │
├─────────────────────────────────────┤
│      Commit Worker Daemon           │  ← Background process for write commits
│   (lowlevel/commit_worker.c)        │
└─────────────────────────────────────┘
                    │
            ┌───────▼───────┐
            │     LMDB      │  ← Underlying embedded database
            │ (vendor/lmdb) │
            └───────────────┘
```

## Key Components

### Transaction Management
- **Read transactions**: Synchronous, use read-only LMDB transactions
- **Write transactions**: Asynchronous commits via background daemon
- **Optimistic locking**: Tracks reads with checksums, validates on commit
- **Retries**: Automatic retry up to 3 times on validation failure

### Data Flow

#### Reads
1. Check transaction's write buffer (skiplist)
2. If not found, read from LMDB snapshot
3. Log read operation (key + checksum) for validation

#### Writes
1. Store in transaction's in-memory write buffer
2. Subsequent reads check buffer first
3. On commit: queue transaction for background processing

#### Commits
1. **Read-only**: Immediate, synchronous
2. **Read-write**: Queued to commit worker daemon
   - Validate all logged reads still match
   - If validation passes: apply writes to LMDB
   - If validation fails: signal retry needed

### Concurrency Model
- **Multiple processes**: Supported via LMDB's multi-process safety
- **Multiple threads per process**: Supported via transaction isolation
- **Single writer**: Commit worker daemon serializes all writes
- **Batched commits**: Multiple transactions committed in single LMDB transaction

### Performance Characteristics
- **Reads**: Zero-copy, memory-mapped access to LMDB pages
- **Writes**: Batched for improved throughput
- **Scaling**: Vertical only (single machine, multiple processes)

## Data Model

- **Keys**: Up to 511 bytes, stored as-is
- **Values**: Up to 4GB, stored as-is  
- **Encoding**: Application-layer concern (JSON, binary, etc.)
- **Ordering**: Lexicographic byte ordering

## Process Model

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Client 1      │    │   Client 2      │    │   Client N      │
│                 │    │                 │    │                 │
│ ┌─────────────┐ │    │ ┌─────────────┐ │    │ ┌─────────────┐ │
│ │Transaction 1│ │    │ │Transaction 1│ │    │ │Transaction 1│ │
│ │Transaction 2│ │    │ │Transaction 2│ │    │ │Transaction 2│ │
│ │     ...     │ │    │ │     ...     │ │    │ │     ...     │ │
│ └─────────────┘ │    │ └─────────────┘ │    │ └─────────────┘ │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │   Commit Worker Daemon  │
                    │                         │
                    │ ┌─────────────────────┐ │
                    │ │   Transaction       │ │
                    │ │   Validation &      │ │
                    │ │   Commit Queue      │ │
                    │ └─────────────────────┘ │
                    └─────────────────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │         LMDB            │
                    │      Database File      │
                    └─────────────────────────┘
```

## Memory Management

- **Zero-copy reads**: Direct pointers to LMDB memory pages
- **Buffer validity**: Only guaranteed during transaction lifetime
- **Write buffers**: Per-transaction skiplist for fast lookups
- **Shared memory**: Communication between clients and commit worker

---

**⚠️ Maintenance Note**: When modifying the architecture, update this file and [AI_CONTRIBUTING.md](AI_CONTRIBUTING.md) to reflect changes.