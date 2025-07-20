# AI Contributor Guide for OLMDB

**⚠️ Important: Keep this file updated when making architectural changes!**

This guide provides essential information for AI assistants contributing to OLMDB. It focuses on architectural highlights and key development information to enable efficient contributions without requiring full project analysis each time.

## Project Overview

OLMDB is a high-performance embedded key-value store that combines LMDB's speed with optimistic concurrency control. Key characteristics:

- **Performance**: Built on LMDB with zero-copy data access and batched commits
- **Concurrency**: Optimistic locking with automatic transaction retries
- **Platform**: Currently Linux-only (contributions for other platforms welcome)
- **Languages**: TypeScript/JavaScript API over native C components
- **Runtime**: Supports Node.js 14+ and Bun

## Architecture Overview

OLMDB has a 5-layer architecture (see README.md "Architecture" section for details):

1. **High-level TypeScript API** (`src/olmdb.ts`) - Main user-facing API with `transact()`, `put()`, `get()`, etc.
2. **Low-level TypeScript API** (`src/lowlevel.ts`) - Loads and exposes the NAPI module API
3. **NAPI module** (`lowlevel/napi.c`) - JavaScript-C bridge for database operations
4. **OLMDB client library** (`lowlevel/transaction_client.c`) - Core optimistic transaction logic
5. **Commit worker daemon** (`lowlevel/commit_worker.c`) - Background process handling write commits

### Key Files and Directories

```
src/
├── olmdb.ts         # High-level API with transact(), AsyncLocalStorage context
└── lowlevel.ts      # Low-level API loading native module

lowlevel/
├── napi.c           # Node.js native module interface
├── transaction_client.c  # Core transaction and optimistic locking logic
└── commit_worker.c  # Background daemon for write commits

tests/
├── olmdb.test.ts    # High-level API tests
└── lowlevel.test.ts # Low-level API tests

vendor/lmdb/         # LMDB source code
benchmark/           # Performance benchmarking suite
tools/               # Build and documentation tools
```

## Development Setup

### Prerequisites
- Linux (primary supported platform)
- Node.js 14+ or Bun
- GCC for native compilation
- Python 3 (for node-gyp)

### Build Process
```bash
npm install          # Installs deps + compiles native modules via node-gyp
npm run build        # Compiles TypeScript to dist/
npm run test         # Runs both Bun and Node.js tests (use test:node if no Bun)
```

### Testing Strategy
- **Unit tests**: Jest-based tests in `tests/` directory
- **Two test suites**: `olmdb.test.ts` (high-level API), `lowlevel.test.ts` (native module)
- **Multi-runtime**: Tests run on both Node.js and Bun when available
- **Build validation**: Tests include TypeScript compilation

## Key Concepts for Contributors

### Transaction Model
- All operations must occur within `transact()` calls
- Read operations are synchronous, commits are asynchronous (for write transactions)
- Optimistic locking: transactions retry up to 3 times on conflicts
- AsyncLocalStorage preserves transaction context across async calls

### Data Handling
- Keys: Limited to 511 bytes
- Values: Up to 4GB
- Zero-copy: `Uint8Array`/`ArrayBuffer` point directly to LMDB memory (valid only during transaction)
- Type conversions: `asString()`, `asBuffer()`, `asArray()` helpers

### Performance Considerations
- Read-only transactions are fully synchronous
- Write transactions use batched commits via background daemon
- Iterator cleanup is important for performance (auto-closed on transaction end)

## Contribution Guidelines for AI Assistants

### Making Changes
1. **Understand the layer**: Know whether you're working on high-level API, low-level API, or native code
2. **Minimal changes**: Focus on surgical modifications rather than large refactors
3. **Test early**: Run `npm run build && npm run test:node` frequently
4. **Consider performance**: OLMDB prioritizes speed - avoid changes that add overhead

### Common Tasks
- **API changes**: Usually modify `src/olmdb.ts` and update tests
- **Low-level changes**: May require changes to `src/lowlevel.ts` and/or C code in `lowlevel/`
- **Documentation**: Update README.md using `npm run update-readme-tsdoc` for API docs

### Testing
- Always run existing tests before and after changes
- Add focused tests for new functionality
- Prefer updating existing tests over creating new test files
- Use `npm run test:node` if Bun is unavailable

### Files to Update When Changing Architecture
When making significant architectural changes, update:
- This file (`AI_CONTRIBUTING.md`) - architectural highlights only
- `README.md` - user-facing documentation (use existing tooling)
- Relevant test files
- TypeScript types and interfaces

### Common Pitfalls
- Don't access zero-copy buffers outside transaction context
- Don't forget to handle transaction retries in high-level API
- Remember OLMDB is Linux-specific - avoid platform-specific assumptions
- Native module changes require `npm install` to recompile

---

**Remember**: Keep this guide focused on architectural highlights and essential development info. For detailed API documentation, refer to README.md. Update this file when making changes that affect the architecture or development workflow.