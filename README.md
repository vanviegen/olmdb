# OLMDB - Optimistic LMDB
**A very fast embedded key/value store featuring ACID transactions using optimistic locking, based on LMDB.**

OLMDB is a high-performance, embedded database that combines the speed and reliability of LMDB with optimistic concurrency control. It provides ACID transactions with automatic retry logic for handling concurrent access patterns, making it ideal for applications that need performance, consistency and parallel (long running) read/write transactions.

**Features:**

- 🚀 **High Performance**: Built on LMDB, one of the fastest embedded databases, providing zero-copy data access.
- 🔒 **ACID Transactions**: Full transaction support with optimistic locking and built-in retry for race conditions.
- 📦 **Zero Dependencies**: Minimal footprint, running on both Node and Bun.
- 🔧 **Simple API**: Fully typed, fast synchronous database reads, promise-based commits.


## Quick Demo
```typescript
import { transact, put, getString, del, scan, asString } from 'olmdb';

async function main() {
    // Basic operations (database auto-initializes)
    const result = await transact(() => {
        // Store data
        put('user:1', JSON.stringify({ name: 'Alice', age: 30 }));
        put('user:2', JSON.stringify({ name: 'Bob', age: 25 }));
        put('user:3', JSON.stringify({ name: 'Charlie', age: 59 }));
        
        // Read back (uncommitted transaction-local) data
        const user = JSON.parse(getString('user:1') || '{}');
        console.log(user); // { name: 'Alice', age: 30 }
        
        // Delete data
        del('user:2');
        
        return user;
    });
    
    // Scan through data, starting at 'user:2' (meaning 'user:3' comes first as 'user:2' does not exist)
    await transact(() => {
        for (const { key, value } of scan({
            start: 'user:2',
            keyConvert: asString,
            valueConvert: asString
        })) {
            console.log(`${key}: ${value}`);
        }
    });
}

main().catch(console.error);
```


## Installation
```bash
npm install olmdb
```

**Requirements:**
- Node.js 14+ or Bun runtime
- Standard build tools (GCC/G++, make) for native compilation via node-gyp
- Python 3.x (required by node-gyp)


## High-level API Tutorial

### Initialization
The database auto-initializes on first use, but you can explicitly open it specifying a directory for the database file.

```typescript
import { open } from 'olmdb';

open('./my-app-data'); // Custom directory
```

The call to `open` may happen only once, and should be done before the first call to `transact`.

Without an explicit directory, the `OLMDB_DIR` environment variable is used or if that isn't set `.olmdb` is used.

### Basic Operations
All database operations must be performed within a transaction:

```typescript
import { transact, put, get, getString, getBuffer, del } from 'olmdb';

let promise = transact(() => {
  // Store different data types
  put('string-key', 'string-value');
  put('buffer-key', new Uint8Array([1, 2, 3, 4]));
  put('json-key', JSON.stringify({ complex: 'data' }));
  
  // Read data with different return types
  const asBytes = get('string-key');        // Uint8Array
  const asString = getString('string-key'); // "string-value"
  const asBuffer = getBuffer('string-key'); // ArrayBuffer (zero-copy!)
  
  // Delete entries
  del('unwanted-key');
  
  return asString;
});
// The `promise` resolves to the value that our function returned (`asString`).
```

### Data Type Handling
OLMDB provides convenience methods for different data types with zero-copy ArrayBuffer operations:

```typescript
import { transact, put, get, getString, getBuffer } from 'olmdb';

await transact(() => {
  // Store any data type
  put('text', 'Hello World');
  put('binary', new Uint8Array([0xFF, 0xEE, 0xDD]));
  
  // Read as different types
  const asBytes = get('text');        // Uint8Array (zero-copy!)
  const asBuffer = getBuffer('text'); // ArrayBuffer (zero-copy!)
  const asString = getString('text'); // "Hello World" (decoded)
  console.log(asBytes, asBuffer, asString);
});
```

Zero-copy means that the `Uint8Array` or `ArrayBuffer` directly points at LMDB's memory mapped data, so it'll be very fast even for large keys and values. Note that keys can only be 511 bytes long, while values can be up to 4 gigabytes.

**However**, the data is only guaranteed to be valid *for the duration of the transaction*! If you were to read from a `Uint8Array` or `ArrayBuffer` some time after the function you provided to `transact()` has finished, you may get back garbage/unrelated data (as LMDB pages and commit buffers are being recycled)!! So don't do that!

I'm considering marking all data buffers returned within a transaction as 'detached' when commit is called (so that any later access will throw an exception), but this involves a fair amount of bookkeeping and thus performance. I'll try this once we have a good way to measure performance.

### Iteration
```typescript
import { transact, scan, asString, asBuffer } from 'olmdb';

transact(() => {
  // Scan all entries
  for (const { key, value } of scan()) {
    console.log(`Key: ${asString(key.buffer)}, Value: ${asString(value.buffer)}`);
  }
  
  // Scan a specific range, with type conversion
  for (const { key, value } of scan({
    start: 'user:',
    end: 'user;',  // Note: ';' comes after ':' in ASCII
    keyConvert: asString,
    valueConvert: asString
  })) {
    console.log(`User: ${key} = ${value}`);
  }
  
  // Reverse iteration
  for (const { key, value } of scan({
    reverse: true,
    keyConvert: asString,
    valueConvert: asString
  })) {
    console.log(`${key}: ${value}`);
  }
  
  // Collection helper methods
  let xs = scan({keyConvert: asString}).map((kv) => kv.key.includes('x')).toArray();
  console.log(xs);
  
  // Manual iterator control
  const iterator = scan({start: 'prefix:'});
  const first = iterator.next();
  if (!first.done) {
    console.log('First entry:', first.value);
  }
  // Closing iterators when not iterating them till the end is optional,
  // as they will auto-close on transaction end, but recommended if many
  // iterators are created (in a loop).
  iterator.close(); 
});
```

### Transaction retries
```typescript
import { transact, put, getString } from 'olmdb';

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function main() {
    await transact(() => {
        put('counter', '42');
    });
    
    const result1 = transact(async () => {
        // This will be retried up to 3 times if another transaction
        // modifies the same data concurrently
        const value = parseInt(getString('counter') || '0') + 1;
        console.log('transaction1: put counter', value);
        await sleep(1000);
        put('counter', value.toString());
        return value;
    });
    
    const result2 = transact(async () => {
        await sleep(500);
        const value = parseInt(getString('counter') || '0') + 10;
        console.log('transaction2: put counter', value);
        put('counter', value.toString());
        return value;
    });
    
    console.log(await Promise.all([result1, result2]));
}

main();
```

Expected output:
```
transaction1: put counter 43
transaction2: put counter 52
transaction1: put counter 53
[53, 52]
```

What this demonstrates is a race condition, gracefully handled by a retry:
- The result1 transaction will read `counter` and then wait for a second, before writing back an updated value.
- In the meantime, the result2 transaction will have read and updated the `counter`.
- On write-commit, the result1 transaction notices that `counter` has changed since it has read it, invalidating the transaction, and running the provided function again. This time it reads 52 instead of 42.

OLMDB retries a transaction 3 times before giving up (and throwing an exception).


## Architecture

### Components
OLMDB consists of two parts:
- A TypeScript file providing the recommended high-level API for this library. This adds automatic transaction retries (in case of race conditions), an implicit transaction context, promise-based transactions and type conversions.
- A native module, providing a low-level API on which the former is built.

### Transactions
The native module manages transactions (start, commit, abort), and all read (get, scan) and write (put, del) operations must happen within such a transaction. Multiple transactions can run simultaneously from the same JavaScript process, allowing work for different clients to be interleaved (using async/await or callbacks). 

### Reads
For each OLMDB transaction, a corresponding read-only LMDB transaction is created. That gives us a consistent snapshot view of the store, from which all reads (get, scan) are done. For every read from LMDB, we log into memory associated with the transaction which key was read and a checksum of its value. We'll use this for commits, as explained below.

### Writes
When writes (put, del) are performed, they are initially only stored in-memory in a buffer local to this transaction. Any further reads will first search the buffer, before consulting the underlying LMDB store, such that a transaction can read back its own writes. The buffer is organized as a skiplist, making searching the buffer very fast even when a transaction contains many writes.

### Aborts and commits
If a transaction is aborted, the write buffer is just discarded and nothing is saved to LMDB. In case of a read-only transaction commit, we do the same. This happens synchronously. For read/write commits, we queue the transaction, including its read and write buffers for processing by the *write-commit thread* (explained below). Once a commit has been processed, its callback function is invoked with the commit status as an argument (from the main thread).

### Write-commit thread
Once one or more read/write commits have been queued, the write-commit thread will start a read/write LMDB transaction. Note that LMDB only allows for a single read/write transaction at a time, so that's why we do this from a single thread and we try to process transactions as quickly as we can.

Within a single LMDB transaction, we can run a bunch of OLMDB transactions as follows:

1. First replay all of the *reads* the transaction has performed (and stored in the read buffer) in its initial phase, verifying that the results are still the same based on the stored checksums. If not, the transaction has been raced by another transaction. Writes for this transaction will not be performed, and the 'raced' status is communicated back to committing code.
2. All of the writes are performed on LMDB. This should never fail, under normal circumstances. The 'success' status is communicated back to the committing code. 

After a batch of OLMDB transactions has been processed, the LMDB transaction is committed, and work can be started on the next batch. Batching commits can help write performance, as writes to pages in the top regions of b-tree can often be shared between transactions.

### Retryable transactions
The high-level API, written in TypeScript provides a `transact(func)` method that runs a function within the context of an OLMDB transaction. This transaction context is preserved in [AsyncLocalStorage](https://nodejs.org/api/async_context.html#class-asynclocalstorage), so that you can just call functions like `put` and `get` without having to pass along a transaction object. This can be convenient when database accesses are deeply nested.

After the provided function is done running, `transact()` does an OLMDB commit, and (for read/write transactions) registers a callback function. In case it gets called with a 'raced' status, the function will be executed again within a new transaction context. This happens up to three times before giving up.

### Scaling
LMDB allows multiple processes (on the same machine) to safely operate on the same database file. However, only one read/write transaction will be allowed to run at a time. So when scaling your deployment by creating multiple JavaScript processes, each of these will create its own worker thread, but only one of them will be committing transactions at a time. Given the trade-offs that LMDB makes, this is close to the best we can do (though coordinating between processes to share a single write-commit thread may be slightly more efficient, but a lot more complex/fragile).

I expect this architecture to be able to provide huge throughput and very low latencies, though actual performance tests haven't been done yet.

In case you eventually need to scale beyond a single server, that can only be done at the application level. 


## High-Level API Reference

The high-level API provides a promise-based, type-safe interface with automatic transaction retries and convenient data type conversions.

### Error Handling

OLMDB uses a custom `DatabaseError` class that extends the standard JavaScript `Error` class. This provides structured error handling with specific error codes.

#### `DatabaseError`
```typescript
class DatabaseError extends Error {
  constructor(message: string, code?: string);
  code?: string;
}
```

**Example:**
```typescript
import { DatabaseError, open } from 'olmdb';

try {
  open('./database');
  // Try to open again
  open('./database');
} catch (error) {
  if (error instanceof DatabaseError && error.code === 'ALREADY_OPEN') {
    console.log('Database is already open');
  }
}
```

#### `TypeError`
For basic API usage errors, such as invalid parameter counts or types, a `TypeError` is thrown. These types of errors generally don't benefit from a machine readable error `code`, as they can't be handled gracefully by code but just need to be fixed by the programmer.

```typescript
import { transact } from 'olmdb';
open(123); // Throws TypeError("Expected database path as string")
```

### Database Management

#### `open(directory?)`
Initialize the database with an optional directory path.

```typescript
import { open } from 'olmdb';

// Use default directory (.olmdb or $OLMDB_DIR)
open();

// Use custom directory
open('./my-database');
```

**Parameters:**
- `directory` (optional): Database directory path

**Throws:**
- `DatabaseError` with code `ALREADY_OPEN`: If database is already initialized
- `DatabaseError` with code `CREATE_DIR_FAILED`: If directory creation fails
- `DatabaseError` with code `LMDB-{code}`: For LMDB-specific errors

### Transaction Management

#### `transact<T>(fn: () => T): Promise<T>`
Execute a function within a database transaction context with automatic retry on conflicts.

```typescript
import { transact, put, get } from 'olmdb';

const result = await transact(() => {
  put('key', 'value');
  return get('key');
});
```

**Parameters:**
- `fn`: Function to execute within transaction context

**Returns:** Promise resolving to the function's return value

**Throws:**
- `TypeError`: If nested transactions are attempted
- `DatabaseError` with code `RACING_TRANSACTION`: If transaction fails after 3 retries due to conflicts (or errors)
- `DatabaseError` with code `TXN_LIMIT`: If maximum number of transactions is reached
- `DatabaseError` with code `LMDB-{code}`: For LMDB-specific errors

### Data Operations

All data operations must be performed within a `transact()` context.

#### `get(key: Data): Uint8Array | undefined`
Retrieve a value as a Uint8Array.

```typescript
const value = get('my-key');
if (value) {
  console.log('Found:', value);
}
```

#### `getBuffer(key: Data): ArrayBuffer | undefined`
Retrieve a value as an ArrayBuffer (zero-copy).

```typescript
const buffer = getBuffer('my-key');
if (buffer) {
  console.log('Size:', buffer.byteLength);
}
```

#### `getString(key: Data): string | undefined`
Retrieve a value as a UTF-8 decoded string.

```typescript
const text = getString('my-key');
if (text) {
  console.log('Text:', text);
}
```

#### `put(key: Data, value: Data): void`
Store a key-value pair.

```typescript
put('string-key', 'string-value');
put('buffer-key', new Uint8Array([1, 2, 3]));
put('json-key', JSON.stringify({ data: 'value' }));
```

#### `del(key: Data): void`
Delete a key-value pair.

```typescript
del('unwanted-key');
```

**Common Parameters:**
- `key`: Key as Uint8Array, ArrayBuffer, or string
- `value`: Value as Uint8Array, ArrayBuffer, or string

**Common Throws:**
- `TypeError`: If called outside transaction context
- `DatabaseError` with code `INVALID_TRANSACTION`: If transaction is invalid or closed
- `DatabaseError` with code `KEY_TOO_LONG`: If key exceeds 511 bytes
- `DatabaseError` with code `LMDB-{code}`: For LMDB-specific errors

### Iteration

#### `scan<K, V>(options?: ScanOptions<K, V>): DbIterator<K, V>`
Create an iterator to scan through database entries.

```typescript
// Basic iteration
for (const { key, value } of scan()) {
  console.log('Entry:', key, value);
}

// With type conversion
for (const { key, value } of scan({
  keyConvert: asString,
  valueConvert: asString
})) {
  console.log(`${key}: ${value}`);
}

// Range iteration
for (const { key, value } of scan({
  start: 'prefix:',
  end: 'prefix;',
  reverse: false
})) {
  console.log('Match:', key, value);
}

// Manual control
const iterator = scan({ start: 'user:' });
const first = iterator.next();
if (!first.done) {
  console.log('First:', first.value);
}
iterator.close(); // Clean up early
```

**ScanOptions:**
- `start?: Data` - Starting key for iteration
- `end?: Data` - Ending key for iteration (exclusive)
- `reverse?: boolean` - Iterate in reverse order
- `keyConvert?: (buffer: ArrayBuffer) => K` - Key conversion function
- `valueConvert?: (buffer: ArrayBuffer) => V` - Value conversion function

**Returns:** `DbIterator<K, V>` implementing standard iterator protocol

#### `DbIterator<K, V>`
Iterator class with standard TypeScript iterator protocol.

**Methods:**
- `next(): IteratorResult<DbEntry<K, V>>` - Get next entry
- `close(): void` - Close iterator and free resources
- `[Symbol.iterator](): DbIterator<K, V>` - Make iterable

**Types:**
- `DbEntry<K, V>`: `{ key: K, value: V }`

### Type Conversion Helpers

#### `asArray(buffer: ArrayBuffer): Uint8Array`
Convert ArrayBuffer to Uint8Array.

#### `asBuffer(buffer: ArrayBuffer): ArrayBuffer`
Return ArrayBuffer as-is (identity function).

#### `asString(buffer: ArrayBuffer): string`
Convert ArrayBuffer to UTF-8 decoded string.

```typescript
// Usage with scan
for (const { key, value } of scan({
  keyConvert: asString,
  valueConvert: asBuffer
})) {
  console.log(`Key: ${key}, Value size: ${value.byteLength}`);
}
```

### Data Types

#### `Data`
Union type for keys and values: `Uint8Array | ArrayBuffer | string`


## Low-Level API example

The low-level API provides direct access to the native LMDB bindings. **Use the high-level API unless you have some very specific requirements.**

```typescript
import lowlevel from 'olmdb/lowlevel';

// Manual transaction management
lowlevel.open('./database');

const txnId = lowlevel.startTransaction();

// All operations use ArrayBuffer
const key = new TextEncoder().encode('test-key').buffer;
const value = new TextEncoder().encode('test-value').buffer;

lowlevel.put(txnId, key, value);
const retrieved = lowlevel.get(txnId, key);

// Commit with callback for write transactions
const isAsync = lowlevel.commitTransaction(txnId, (transactionId, status) => {
  if (status === lowlevel.TRANSACTION_SUCCEEDED) {
    console.log('Transaction committed successfully');
  } else if (status === lowlevel.TRANSACTION_RACED) {
    console.log('Transaction raced - would need manual retry');
  }
});

if (!isAsync) {
  console.log('Read-only transaction completed immediately');
}

// Manual iteration
const txnId2 = lowlevel.startTransaction();
const iteratorId = lowlevel.createIterator(txnId2);
let result;
while ((result = lowlevel.readIterator(iteratorId)) !== undefined) {
  console.log('Key:', result.key, 'Value:', result.value);
}
lowlevel.closeIterator(iteratorId);
lowlevel.abortTransaction(txnId2);
```


## Low-Level API Reference

#### Database Management
- **`open(directory?)`**: Initialize database at specified directory. Defaults to `$OLMDB_DIR` environment variable or `./.olmdb`. Throws `DatabaseError` if already open or directory creation fails.

#### Transaction Management
- **`startTransaction()`**: Begin new transaction, returns transaction ID. Throws `DatabaseError` if transaction limit reached.
- **`commitTransaction(txnId, callback?)`**: Commit transaction. Returns `false` for read-only transactions (completed immediately) or `true` for write transactions. Transaction becomes invalid after commit. When a write transaction completes, `callback(txnId, result)` will be called, where `result` is one of `TRANSACTION_SUCCEEDED` (writes have been committed to disk), `TRANSACTION_RACED` (the read results have changed since the transaction started, no writes have been performed) or `TRANSACTION_FAILED` (an unexpected error occurred, see stderr for details).
- **`abortTransaction(txnId)`**: Abort transaction and release resources. Transaction becomes invalid after abort.

The transaction id `txnId` is a number (within the 32 bit integer range).

#### Data Operations
- **`get(txnId, key)`**: Retrieve value by key. Returns `ArrayBuffer` or `undefined` if not found. Zero-copy operation.
- **`put(txnId, key, value)`**: Store key-value pair. Both key and value must be `ArrayBuffer`.
- **`del(txnId, key)`**: Delete key from database. Key must be `ArrayBuffer`.

Keys and values are `ArrayBuffer` objects.

#### Iterator Operations
- **`createIterator(txnId, startKey?, endKey?, reverse?)`**: Create iterator for scanning. Returns iterator ID. Iterators are automatically closed when transaction ends or when they reach the end.
- **`readIterator(iteratorId)`**: Read next item from iterator. Returns `{key: ArrayBuffer, value: ArrayBuffer}` or `undefined` when exhausted. Iterator auto-closes when exhausted.
- **`closeIterator(iteratorId)`**: Manually close iterator and free resources. Safe to call multiple times.

The iterator id `iteratorId` is a number (within the 32 bit integer range). The `reverse` flag is a boolean, defaulting to false.

**Note**: Iterators are automatically closed when their transaction ends, when they're exhausted by reading all items, or when manually closed. You can close iterators early if you don't iterate to completion to free resources immediately.


## License

ISC License - see LICENSE file for details.