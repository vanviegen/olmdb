# OLMDB - Optimistic LMDB
**A very fast embedded key/value store featuring ACID transactions using optimistic locking, based on LMDB.**

OLMDB is a high-performance, embedded on-disk key/value store that combines the speed and reliability of LMDB with optimistic concurrency control and fast batched commits. It provides ACID transactions with automatic retries for handling concurrent access patterns, making it ideal for applications that need performance, consistency and parallel (long running) read/write transactions.

**Features:**

- 🚀 **High Performance**: Built on LMDB, one of the fastest embedded databases, providing zero-copy data access. Read/write commits are automatically batched to enhance throughput.
- 🔒 **ACID Transactions**: Full transaction support with optimistic locking and built-in retry for race conditions.
- 📦 **Zero Dependencies**: Minimal footprint, running on both Node and Bun. No server process to manage.
- 🔧 **Simple API**: Near-instantaneous synchronous database reads and updates. Promise-based commits.

**Important caveat**: OLMDB (probably) only runs on Linux currently.

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
        for (const { key, value } of scan({start: 'user:2'})) {
            console.log(key, value);
        }
    });
}

main().catch(console.error);
```


## Installation
```bash
npm install olmdb
# or
bun add olmdb
```

**Requirements:**
- Linux (contributions to support other platforms are very welcome!)
- Node.js 14+ or Bun runtime
- GCC for native module compilation


## High-level API Tutorial

### Initialization
The database auto-initializes on first use, but you can explicitly initialize it specifying a directory for the database file.

```typescript
import { init } from 'olmdb';

init('./my-app-data'); // Custom data directory
```

The call to `init` may happen only once, and should be done before the first call to `transact`.

Without an explicit directory, the `OLMDB_DIR` environment variable is used or if that isn't set `./.olmdb` is used.

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

OLMDB retries a transaction 4 times before giving up (and throwing an exception).


## Performance

Performance seems to be predictably strong, for various loads and data sizes.

To get some idea, on my mediocre laptop with a store prefilled with 1 million values of 100 bytes each, I get: 

1) 1 read + 0 writes per txn, 1 process, 1 task per process: 420,000 txn/s.
2) 1 read + 0 writes per txn, 16 processes, 1 task per process: 2,495,000 txn/s.
3) 10 reads + 0 writes per txn, 1 process, 1 task per process: 67,000 txn/s.
4) 10 reads + 0 writes per txn, 16 processes, 1 task per process: 403,000 txn/s.
5) 10 reads + 2 writes per txn, 1 process, 1 task per process: 100 txn/s.
6) 10 reads + 2 writes per txn, 1 process, 1000 tasks per process: 6,000 txn/s.
7) 10 reads + 2 writes per txn, 16 processes, 1000 tasks per process: 16,000 txn/s.

Scaling up the number of tasks per process wouldn't improve performance for read-only transactions, as they're fully synchronous anyway. The performance for 5) is pretty bad, but as expected, as each transaction requires a full sync() to disk (apparently taking about 10ms on my system) before starting the next transaction.


## Architecture

### Components
OLMDB consists of five parts, each building on the one below:
- The high-level TypeScript API (`olmdb.ts`) for this library.
- The low-level TypeScript API (`lowlevel.ts`) that loads and exposes the API provided by the NAPI module.
- The NAPI module (`napi.c`) that provides a low-level JavaScript API for the OLMDB client library.
- The OLMDB client library (`transaction_client.c`) that provides optimistic read/write transactions on top of LMDB. It has no relation to JavaScript. Database reads are performed synchronously. Commits for read/write transactions are performed asyncronously by the commit worker daemon.
- The commit worker daemon (`commit_worker.c`), shared by all clients for a database and spawned automatically by the first client. It processes queued read/write transactions and commits them to LMDB.

### Transactions
The native module manages transactions (start, commit, abort), and all read (get, scan) and write (put, del) operations must happen within such a transaction. Multiple transactions can run simultaneously from the same JavaScript process, allowing work for different clients to be interleaved (using async/await or callbacks). 

### Reads
For each OLMDB transaction, a corresponding read-only LMDB transaction is created. That gives us a consistent snapshot view of the store, from which all reads (get, scan) are done. For every read from LMDB, we log into memory associated with the transaction which key was read and a checksum of its value. We'll use this for commits, as explained below.

### Writes
When writes (put, del) are performed, they are initially only stored in-memory in a buffer local to this transaction. Any further reads will first search the buffer, before consulting the underlying LMDB store, such that a transaction can read back its own writes. The buffer is organized as a skiplist, so that searching the buffer is very fast even when a transaction contains many writes.

### Aborts and commits
If a transaction is aborted, the write buffer is just discarded and nothing is saved to LMDB. In case of a read-only transaction commit, we do the same. This happens synchronously. For read/write commits, we queue the transaction, including its read and write buffers for processing by the *commit worker daemon* (explained below). Once a commit has been processed, its callback function is invoked with the commit status as an argument (from the main thread).

### Commit worker daemon
The commit worker daemon is a single-threaded process that runs in the background, processing queued read/write transactions. It is spawned automatically by the first OLMDB client that accesses the database, and it will run until 10s after the last client has disconnected. Clients connect to the daemon using a unix domain socket (in the abstract socket
namespace). Clients store their transaction buffers in a shared memory region, which is also opened by the commit worker daemon. The socket is only used to communicate the address of each transaction to be committed to the daemon, and to signal "at least one transaction has been processed" back to the client.

Once one or more read/write commits have been queued, the write-commit thread will start a read/write LMDB transaction. Note that LMDB only allows for a single read/write transaction at a time, so that's why we do this from a single thread and we try to process transactions as quickly as we can.

Within a single LMDB transaction, we can run a bunch of OLMDB transactions as follows:

1. First replay all of the *reads* the transaction has performed (and stored in the read buffer) in its initial phase, verifying that the results are still the same based on the stored checksums. If not, the transaction has been raced by another transaction. Writes for this transaction will not be performed, and the 'raced' status is communicated back to committing code.
2. All of the writes are performed on LMDB. This should never fail, under normal circumstances. The 'success' status is communicated back to the committing code. 

After a batch of OLMDB transactions has been processed, the LMDB transaction is committed, and work can be started on the next batch. Batching commits can improve write performance immensely, as writes to pages in the top regions of b-tree and file syncs can usually be shared between transactions.

### Retryable transactions
The high-level API, written in TypeScript provides a `transact(func)` method that runs a function within the context of an OLMDB transaction. This transaction context is preserved in [AsyncLocalStorage](https://nodejs.org/api/async_context.html#class-asynclocalstorage), so that you can just call functions like `put` and `get` without having to pass along a transaction object. This can be convenient when database accesses are deeply nested.

After the provided function is done running, `transact()` does an OLMDB commit, and (for read/write transactions) registers a callback function. In case it gets called with a 'raced' status, the function will be executed again within a new transaction context. This happens up to four times before giving up.

### Scaling
You can run as many JavaScript processes, and within each as many concurrent request handlers as you want. LMDB allows multiple processes (on the same machine) to safely run read-only transaction in the same database file simultaneously. All read/write operations are committed to LMDB by the single threaded commit worker daemon, which may be a bottleneck for write-heavy workloads, though it's highly efficient and sure therefore scale to thousands of transactions per second. 

In the `benchmark` directory you can find the start of a benchmarking suite to get some feel for performance.

OLMDB can only scale vertically, on a single server. In case you eventually need to scale beyond a single server, that can only be done at the application level. 


## API Reference

The high-level API provides a promise-based, type-safe interface with automatic transaction retries and convenient data type conversions.

The following is auto-generated from `src/olmdb.ts`:

### get

Retrieves a value from the database by key within the current transaction.

| Function | Type |
| ---------- | ---------- |
| `get` | `(key: Data) => Uint8Array<ArrayBufferLike> or undefined` |

Parameters:

* `key`: - The key to look up as a Uint8Array, ArrayBuffer, or string.


### getBuffer

Retrieves a value from the database by key within the current transaction.

| Function | Type |
| ---------- | ---------- |
| `getBuffer` | `(key: Data) => ArrayBuffer or undefined` |

Parameters:

* `key`: - The key to look up as a Uint8Array, ArrayBuffer, or string.


### getString

Retrieves a value from the database by key within the current transaction and decodes it as a string.

| Function | Type |
| ---------- | ---------- |
| `getString` | `(key: Data) => string or undefined` |

Parameters:

* `key`: - The key to look up as a Uint8Array, ArrayBuffer, or string.


### put

Stores a key-value pair in the database within the current transaction.

| Function | Type |
| ---------- | ---------- |
| `put` | `(key: Data, val: Data) => void` |

Parameters:

* `key`: - The key to store as a Uint8Array, ArrayBuffer, or string.
* `val`: - The value to associate with the key as a Uint8Array, ArrayBuffer, or string.


### del

Deletes a key-value pair from the database within the current transaction.

| Function | Type |
| ---------- | ---------- |
| `del` | `(key: Data) => void` |

Parameters:

* `key`: - The key to delete as a Uint8Array, ArrayBuffer, or string.


### init

Initialize the database with the specified directory path.
This function may only be called once. If it is not called before the first transact(),
the database will be automatically initialized with the default directory.

| Function | Type |
| ---------- | ---------- |
| `init` | `(dbDir?: string or undefined) => void` |

Parameters:

* `dbDir`: - Optional directory path for the database (defaults to environment variable $OLMDB_DIR or "./.olmdb").


Examples:

```typescript
init("./my-database");
```


### onRevert

Registers a callback to be executed when the current transaction is reverted (aborted due to error).
The callback will be executed outside of transaction context.

| Function | Type |
| ---------- | ---------- |
| `onRevert` | `(callback: () => void) => void` |

Parameters:

* `callback`: - Function to execute when transaction is reverted


### onCommit

Registers a callback to be executed when the current transaction commits successfully.
The callback will be executed outside of transaction context.

| Function | Type |
| ---------- | ---------- |
| `onCommit` | `(callback: () => void) => void` |

Parameters:

* `callback`: - Function to execute when transaction commits


### transact

Executes a function within a database transaction context.

All database operations (get, put, del) must be performed within a transaction.
Transactions are automatically committed if the function completes successfully,
or aborted if an error occurs. Failed transactions may be automatically retried
up to 3 times in case of validation conflicts.

| Function | Type |
| ---------- | ---------- |
| `transact` | `<T>(fn: () => T) => Promise<T>` |

Parameters:

* `fn`: - The function to execute within the transaction context


Examples:

```typescript
const result = await transact(() => {
  const value = get(keyBytes);
  if (value) {
    put(keyBytes, newValueBytes);
  }
  return value;
});
```


### scan

Creates an iterator to scan through database entries within the current transaction.

The iterator implements the standard TypeScript iterator protocol and can be used with for...of loops.
Supports both forward and reverse iteration with optional start and end boundaries.

| Function | Type |
| ---------- | ---------- |
| `scan` | `<K = Uint8Array<ArrayBufferLike>, V = Uint8Array<ArrayBufferLike>>(opts?: ScanOptions<K, V>) => DbIterator<K, V>` |

Parameters:

* `opts`: - Configuration options for the scan operation.
* `opts.start`: - Optional starting key for iteration. If not provided, starts from the beginning/end.
* `opts.end`: - Optional ending key for iteration. Iteration stops before reaching this key.
* `opts.reverse`: - Whether to iterate in reverse order (defaults to false).
* `opts.keyConvert`: - Function to convert key ArrayBuffers to type K (defaults to asArray).
* `opts.valueConvert`: - Function to convert value ArrayBuffers to type V (defaults to asArray).


Examples:

```typescript
await transact(() => {
  // Iterate over all entries
  for (const { key, value } of scan()) {
    console.log('Key:', key, 'Value:', value);
  }
  
  // Iterate with string conversion
  for (const { key, value } of scan({ 
    keyConvert: asString, 
    valueConvert: asString 
  })) {
    console.log('Key:', key, 'Value:', value);
  }
  
  // Iterate with boundaries
  for (const { key, value } of scan({ 
    start: "prefix_", 
    end: "prefix~" 
  })) {
    console.log('Key:', key, 'Value:', value);
  }
  
  // Manual iteration control
  const iter = scan({ reverse: true });
  const first = iter.next();
  iter.close(); // Always close when done early
});
```


### asArray

Converts an ArrayBuffer to a Uint8Array.
Helper function for use with scan() keyConvert and valueConvert options.

| Function | Type |
| ---------- | ---------- |
| `asArray` | `(buffer: ArrayBuffer) => Uint8Array<ArrayBufferLike>` |

Parameters:

* `buffer`: - The ArrayBuffer to convert.


### asBuffer

Returns the ArrayBuffer as-is.
Helper function for use with scan() keyConvert and valueConvert options.

| Function | Type |
| ---------- | ---------- |
| `asBuffer` | `(buffer: ArrayBuffer) => ArrayBuffer` |

Parameters:

* `buffer`: - The ArrayBuffer to return.


### asString

Converts an ArrayBuffer to a UTF-8 decoded string.
Helper function for use with scan() keyConvert and valueConvert options.

| Function | Type |
| ---------- | ---------- |
| `asString` | `(buffer: ArrayBuffer) => string` |

Parameters:

* `buffer`: - The ArrayBuffer to decode.
## Low-level API Reference

The low-level API is what's exposed by the native module. It's a somewhat less convenient than the high-level API, but it's a good starting point if you require a different abstraction.

The following is auto-generated from `src/lowlevel.ts`:

### init

Initializes the database system with the specified directory.

| Function | Type |
| ---------- | ---------- |
| `init` | `(onCommit: (transactionId: number, success: boolean) => void, directory?: string or undefined, commitWorkerBin?: string) => void` |

Parameters:

* `onCommit`: Callback function that will be invoked when an asynchronous 
transaction commit completes. The callback receives the transaction ID and
whether the commit succeeded.
* `directory`: Optional path to the database directory. If not provided,
defaults to the OLMDB_DIR environment variable or "./.olmdb".
* `commitWorkerBin`: Path to the commit worker binary. Defaults to
`<base_dir>/build/release/commit_worker`.


### startTransaction

Starts a new transaction for database operations.

| Function | Type |
| ---------- | ---------- |
| `startTransaction` | `() => number` |

### commitTransaction

Commits the transaction with the given ID.

If the transaction is read-only, commit will complete immediately and return true.
If the transaction has modifications, it will be queued for asynchronous commit and return false.
When the commit is processed, the onCommit callback provided to init() will be invoked.

| Function | Type |
| ---------- | ---------- |
| `commitTransaction` | `(transactionId: number) => boolean` |

Parameters:

* `transactionId`: The ID of the transaction to commit


### abortTransaction

Aborts the transaction with the given ID, discarding all changes.

| Function | Type |
| ---------- | ---------- |
| `abortTransaction` | `(transactionId: number) => void` |

Parameters:

* `transactionId`: The ID of the transaction to abort


### get

Retrieves a value for the given key within a transaction.

| Function | Type |
| ---------- | ---------- |
| `get` | `(transactionId: number, key: ArrayBufferLike) => ArrayBuffer or undefined` |

Parameters:

* `transactionId`: The ID of the transaction
* `key`: Key to look up


### put

Stores a key-value pair within a transaction.

| Function | Type |
| ---------- | ---------- |
| `put` | `(transactionId: number, key: ArrayBufferLike, value: ArrayBufferLike) => void` |

Parameters:

* `transactionId`: The ID of the transaction
* `key`: Key to store
* `value`: Value to store


### del

Deletes a key-value pair within a transaction.

| Function | Type |
| ---------- | ---------- |
| `del` | `(transactionId: number, key: ArrayBufferLike) => void` |

Parameters:

* `transactionId`: The ID of the transaction
* `key`: Key to delete


### createIterator

Creates an iterator for scanning a range of keys within a transaction.

| Function | Type |
| ---------- | ---------- |
| `createIterator` | `(transactionId: number, startKey?: ArrayBufferLike or undefined, endKey?: ArrayBufferLike or undefined, reverse?: boolean or undefined) => number` |

Parameters:

* `transactionId`: The ID of the transaction
* `startKey`: Optional key to start iteration from (inclusive)
* `endKey`: Optional key to end iteration at (exclusive)
* `reverse`: If true, keys are returned in descending order


### readIterator

Reads the next key-value pair from an iterator.

| Function | Type |
| ---------- | ---------- |
| `readIterator` | `(iteratorId: number) => { key: ArrayBuffer; value: ArrayBuffer; } or undefined` |

Parameters:

* `iteratorId`: The ID of the iterator


### closeIterator

Closes an iterator when it's no longer needed.

| Function | Type |
| ---------- | ---------- |
| `closeIterator` | `(iteratorId: number) => void` |

Parameters:

* `iteratorId`: The ID of the iterator to close


### DatabaseError

Interface for DatabaseError, which extends the standard Error class.
Contains an additional code property for machine-readable error identification.
The DatabaseError class is used to represent errors that occur during database operations.
It extends the built-in Error class and has a machine readable error code string property.

The lowlevel API will throw DatabaseError instances for all database-related errors.
Invalid function arguments will throw TypeError.

| Function | Type |
| ---------- | ---------- |
| `DatabaseError` | `DatabaseErrorConstructor` |