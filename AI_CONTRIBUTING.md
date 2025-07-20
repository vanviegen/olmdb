# AI Contributor Guide for OLMDB

**⚠️ Important: Keep this file updated when making architectural changes!**

This guide provides essential information for AI assistants contributing to OLMDB. It focuses on understanding the codebase structure and how to modify the library effectively.

## Project Overview

OLMDB is a high-performance embedded key-value store that combines LMDB's speed with optimistic concurrency control. For usage examples, see README.md.

**Key Architecture**: 5-layer design from TypeScript API down to LMDB with optimistic transactions

## Code Organization & File Purposes

### TypeScript Layer (`src/`)

**`src/olmdb.ts`** - High-level user-facing API
- Exports main functions: `transact()`, `put()`, `get()`, `del()`, `list()`, `init()`  
- Manages AsyncLocalStorage context for transaction isolation
- Handles automatic transaction retries (up to 3 attempts) on validation failures
- Tracks pending transactions via Map and coordinates with commit callbacks
- Implements `onCommit`/`onRevert` callback system for transaction lifecycle
- Uses TextEncoder/TextDecoder for string <-> Uint8Array conversion

**`src/lowlevel.ts`** - Native module loader and bridge
- Uses `dlopen()` to load `transaction_client.node` native module
- Exposes core native functions: `init()`, `startTransaction()`, `commit()`, `get()`, `put()`, etc.
- Handles binary path resolution (`OLMDB_BIN_DIR` environment variable)
- Provides TypeScript type definitions for native function signatures

### Native C Layer (`lowlevel/`)

**`lowlevel/napi.c`** - N-API bridge between JavaScript and C
- Implements Node.js N-API bindings for all database operations
- Manages JavaScript callback references (especially `onCommit` callback)
- Handles marshalling between JavaScript values and C data structures
- Implements async work queues for commit result delivery from background daemon
- Error handling: translates C errors to JavaScript `DatabaseError` exceptions

**`lowlevel/transaction_client.c`** - Core optimistic transaction engine
- **Shared memory management**: Creates 4GB shared memory region via `memfd_create()`
- **Transaction tracking**: Maintains skiplist of read/write operations per transaction
- **Optimistic validation**: Tracks read checksums, validates on commit against current DB state  
- **Daemon communication**: Unix socket communication with commit worker process
- **Read transactions**: Direct LMDB read access, cached for 100ms windows
- **Write buffering**: In-memory skiplist stores writes until commit time
- **Iterator support**: Cursor-based iteration with optimistic consistency

**`lowlevel/commit_worker.c`** - Background commit daemon  
- **Batched commits**: Processes up to 10K transactions per LMDB write transaction
- **Multi-client handling**: Serves multiple processes via epoll-based event loop  
- **Write validation**: Re-validates read checksums before applying writes
- **Conflict resolution**: Marks conflicting transactions for retry
- **Signal handling**: Notifies client processes of commit results via socket

**`lowlevel/common.c`** - Shared utility code
- **Important**: This file is #included (not linked) in both `transaction_client.c` and `commit_worker.c`
- Contains shared data structures, constants, and utility functions
- Skiplist implementation for write buffering
- Checksum calculation (FNV-1a hash) for optimistic validation
- Shared memory layout definitions

**`lowlevel/common.h`** - Shared header definitions
- Transaction state constants (`TRANSACTION_FREE`, `TRANSACTION_OPEN`, etc.)
- Data structure definitions for shared memory layout
- Macro definitions for error handling and logging
- LMDB integration constants and helpers

## Quick Usage Example (for context)

```typescript
import { init, transact, put, get } from 'olmdb';

init(); // Start database and commit worker
await transact(async () => {
  await put('key', 'value');
  return await get('key'); // Returns 'value'
});
```

## Development Workflow

### Building
```bash
npm install        # Builds native modules via node-gyp
npm run build      # Compiles TypeScript to dist/
```

### Testing  
```bash
npm test           # Runs both Bun and Node.js test suites
npm run test:bun   # Fast Bun-based tests
npm run test:node  # Node.js tests with Jest
```

### Documentation Updates
- **Auto-generated API docs**: Run `node tools/update-readme-tsdoc.js` to update function documentation in README.md
- **Manual docs**: Update README.md directly for architecture/usage sections
- **This file**: Keep AI_CONTRIBUTING.md updated when making architectural changes

## Common Development Patterns

### Adding New API Functions
1. Add C implementation to `transaction_client.c`
2. Export via N-API in `napi.c` 
3. Add TypeScript declaration to `lowlevel.ts`
4. Add high-level wrapper to `olmdb.ts` if needed
5. Update tests and run documentation script

### Debugging Transaction Issues
- Check transaction states in shared memory structures
- Verify checksum calculations for read validation  
- Trace daemon communication via socket messages
- Use logging macros (`LOG`, `SET_ERROR`) in C code

### Memory Management
- Shared memory is managed by `transaction_client.c`
- JavaScript buffers use zero-copy access to shared memory when possible
- C code must handle cleanup on process exit/crash scenarios

## Architecture Notes for Contributors

- **Optimistic concurrency**: Reads are tracked via checksums, validated at commit time
- **Process model**: Client processes communicate with single commit worker daemon
- **Zero-copy semantics**: Data buffers are shared between JavaScript and C when possible  
- **Platform limitation**: Currently Linux-only due to shared memory and socket usage
- **Transaction isolation**: Uses AsyncLocalStorage to maintain transaction context across async calls

- **Don't access zero-copy buffers outside transaction context** - `Uint8Array`/`ArrayBuffer` from LMDB are only valid during transaction
- **Native module changes require rebuild** - Run `npm install` after C code changes  
- **Transaction retries** - High-level API must handle automatic retries, low-level API does not
- **Platform assumptions** - OLMDB is Linux-only; avoid cross-platform code patterns
- **Memory safety** - C code must handle cleanup on process exit/crash scenarios
- **Async context** - AsyncLocalStorage can be lost across certain async boundaries

## Contribution Guidelines for AI Assistants

### Making Effective Changes
1. **Understand the layer**: Identify whether changes are needed in TypeScript API, N-API bridge, or core C logic
2. **Minimal modifications**: Make surgical changes rather than large refactors
3. **Test incrementally**: Run `npm run test:node` after each change
4. **Performance first**: OLMDB prioritizes speed - avoid changes that add overhead

### Common Development Tasks
- **API modifications**: Usually in `src/olmdb.ts`, with tests in `tests/olmdb.test.ts`
- **Low-level changes**: May need `src/lowlevel.ts` and/or C files in `lowlevel/`
- **Documentation updates**: Use `node tools/update-readme-tsdoc.js` for API docs

### Testing Approach
- Run existing tests before and after changes: `npm run build && npm run test:node`  
- Add focused tests for new functionality rather than comprehensive test suites
- Prefer updating existing test files over creating new ones
- Use Node.js tests (`test:node`) if Bun is unavailable

### Files to Update for Architectural Changes
- **This file** (`AI_CONTRIBUTING.md`) - Keep architectural highlights current
- **README.md** - Use existing tooling for API docs, update manually for architecture/usage 
- **Test files** - Add coverage for new functionality
- **TypeScript definitions** - Update interfaces and types as needed

---

**Remember**: This guide focuses on understanding and modifying OLMDB, not using it. For usage examples and detailed API documentation, see README.md. Keep this file updated when making architectural changes.