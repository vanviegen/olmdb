import { AsyncLocalStorage } from "async_hooks";
import * as lowlevel from "olmdb/lowlevel";
import { assert } from "console";
export { DatabaseError } from "olmdb/lowlevel";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/**
 * Union type representing the supported data formats for keys and values.
 * Can be a Uint8Array, ArrayBuffer, or string.
 */
export type Data = Uint8Array | ArrayBuffer | string;

type Transaction = {
    fn: () => any;
    resolve: (value: any) => void;
    reject: (reason: any) => void;
    id: number;
    result?: any;
    retryCount: number;
    onCommitCallbacks?: Array<() => void>;
    onRevertCallbacks?: Array<() => void>;
} & {
    // Any symbol key is user data.
    [key: symbol]: any;
}

function runCallbacks(txn: Transaction, callbackList: "onCommitCallbacks" | "onRevertCallbacks") {
    const cbs = txn[callbackList];
    if (cbs) {
        // Invoke callbacks in reverse order, to match backtracking semantics
        for(let i = cbs.length - 1; i >= 0; i--) {
            const cb = cbs[i];
            try {
                cb();
            } catch (err) {
                console.error(err);
            }
        }
    }
    delete txn.onCommitCallbacks;
    delete txn.onRevertCallbacks;
}

// Track pending transactions by ID
const pendingTransactions = new Map<number, Transaction>();
// Track if database has been initialized
let isInitialized = false;

// Global commit handler that will be passed to lowlevel.init
function globalCommitHandler(transactionId: number, success: boolean): void {
    const txn = pendingTransactions.get(transactionId);
    if (!txn) {
        console.warn(`Received commit for unknown transaction ${transactionId}`);
        return;
    }
    
    pendingTransactions.delete(transactionId);
    txn.id = -1; // Mark as done
    
    if (success) {
        // Execute onCommit callbacks outside transaction context
        runCallbacks(txn, "onCommitCallbacks");
        
        txn.resolve(txn.result);
    } else {
        // Execute onRevert callbacks for retry failure
        runCallbacks(txn, "onRevertCallbacks");
        
        // Attempt retry if transaction failed due to conflicts
        txn.retryCount++;
        if (txn.retryCount > 100) {
            txn.reject(new lowlevel.DatabaseError("Transaction keeps getting raced", "RACING_TRANSACTION"));
        } else {
            console.log(`Retrying raced transaction (${txn.retryCount}/10)`);
            tryTransaction(txn); // Retry the transaction
        }
    }
}

async function tryTransaction(txn: Transaction) {
    txn.id = lowlevel.startTransaction();
    assert(txn.id >= 0, "Transaction ID should be valid 1");
    pendingTransactions.set(txn.id, txn);
    
    let error: any = null;
    let immediateCommit = false;
    
    await transactionStorage.run(txn, async () => {
        assert(txn.id >= 0, "Transaction ID should be valid 2");
        try {
            assert(txn.id >= 0, "Transaction ID should be valid 2a");
            txn.result = await txn.fn();
            assert(txn.id >= 0, "Transaction ID should be valid 2b");
        } catch(err) {
            error = err;
            lowlevel.abortTransaction(txn.id);
            return;
        }

        assert(txn.id >= 0, "Transaction ID should be valid 3");
        immediateCommit = lowlevel.commitTransaction(txn.id);
        // Note: Don't execute callbacks here as we're still within transaction context
    });
    
    if (error || immediateCommit) {        
        pendingTransactions.delete(txn.id);
        txn.id = -1;
        if (error) {
            runCallbacks(txn, "onRevertCallbacks");
            txn.reject(error);
        } else {
            runCallbacks(txn, "onCommitCallbacks");        
            txn.resolve(txn.result);
        }
    }
    // else: globalCommitHandler will be called later
}

const transactionStorage = new AsyncLocalStorage<Transaction|undefined>();

function getTransaction(): Transaction {
    const transaction = transactionStorage.getStore();
    if (!transaction) {
        throw new TypeError("Db operations should be performed within in a transact()");
    }
    return transaction;
}

/**
 * Attach some arbitrary user data to the current transaction context, which is
 * attached to the currently running (async) task.
 * @param key - A symbol key to store data in the current transaction context.
 * @param value - The value to store.
 * @throws {TypeError} If called outside of a transaction context.
 * @example
 * ```typescript
 * const MY_SYMBOL = Symbol("myKey");
 * await transact(async () => {
 *   setTransactionData(MY_SYMBOL, "myValue");
 *   await somethingAsync(); // Can be interleaved with other transactions
 *   const value = getTransactionData(MY_SYMBOL);
 *   console.log(value); // "myValue"
 * });
 * ```
 */
export function setTransactionData(key: symbol, value: any) {
    const transaction = getTransaction();
    transaction[key] = value;
}

/**
 * Retrieves data from the current transaction context.
 * @param key - A symbol key to retrieve data from the current transaction context.
 * @returns - The value associated with the key, or undefined if not set.
 * @throws {TypeError} If called outside of a transaction context.
 */
export function getTransactionData(key: symbol) {
    const transaction = getTransaction();
    return transaction[key] as any;
}

function toBuffer(key: Data): ArrayBufferLike {
    if (key instanceof ArrayBuffer) return key;
    if (key instanceof Uint8Array) {
        const b = key.buffer;
        return b.byteLength == key.byteLength ? b : b.slice(key.byteOffset, key.byteOffset + key.byteLength);
    }
    return encoder.encode(key).buffer;
}

/**
 * Retrieves a value from the database by key within the current transaction.
 * 
 * @param key - The key to look up as a Uint8Array, ArrayBuffer, or string.
 * @returns The value associated with the key as a Uint8Array, or undefined if not found.
 * @throws {TypeError} If called outside of a transaction context.
 * @throws {DatabaseError} With code "INVALID_TRANSACTION" if transaction is invalid or already closed.
 * @throws {DatabaseError} With code "KEY_TOO_LONG" if key exceeds maximum allowed length.
 * @throws {DatabaseError} With code "LMDB-{code}" for LMDB-specific errors.
 */
export function get(key: Data): Uint8Array | undefined {
    const result = lowlevel.get(getTransaction().id, toBuffer(key));
    return result && new Uint8Array(result);
}

/**
 * Retrieves a value from the database by key within the current transaction.
 * 
 * @param key - The key to look up as a Uint8Array, ArrayBuffer, or string.
 * @returns The value associated with the key as an ArrayBuffer, or undefined if not found.
 * @throws {TypeError} If called outside of a transaction context.
 * @throws {DatabaseError} With code "INVALID_TRANSACTION" if transaction is invalid or already closed.
 * @throws {DatabaseError} With code "KEY_TOO_LONG" if key exceeds maximum allowed length.
 * @throws {DatabaseError} With code "LMDB-{code}" for LMDB-specific errors.
 */
export function getBuffer(key: Data): ArrayBuffer | undefined {
    return lowlevel.get(getTransaction().id, toBuffer(key));
}

/**
 * Retrieves a value from the database by key within the current transaction and decodes it as a string.
 * 
 * @param key - The key to look up as a Uint8Array, ArrayBuffer, or string.
 * @returns The value associated with the key as a UTF-8 decoded string, or undefined if not found.
 * @throws {TypeError} If called outside of a transaction context.
 * @throws {DatabaseError} With code "INVALID_TRANSACTION" if transaction is invalid or already closed.
 * @throws {DatabaseError} With code "KEY_TOO_LONG" if key exceeds maximum allowed length.
 * @throws {DatabaseError} With code "LMDB-{code}" for LMDB-specific errors.
 */
export function getString(key: Data): string | undefined {
    const result = lowlevel.get(getTransaction().id, toBuffer(key));
    return result && decoder.decode(result);
}

/**
 * Stores a key-value pair in the database within the current transaction.
 * 
 * @param key - The key to store as a Uint8Array, ArrayBuffer, or string.
 * @param val - The value to associate with the key as a Uint8Array, ArrayBuffer, or string.
 * @throws {TypeError} If called outside of a transaction context.
 * @throws {DatabaseError} With code "INVALID_TRANSACTION" if transaction is invalid or already closed.
 * @throws {DatabaseError} With code "KEY_TOO_LONG" if key exceeds maximum allowed length.
 * @throws {DatabaseError} With code "LMDB-{code}" for LMDB-specific errors.
 */
export function put(key: Data, val: Data): void {
    lowlevel.put(getTransaction().id, toBuffer(key), toBuffer(val));
}

/**
 * Deletes a key-value pair from the database within the current transaction.
 * 
 * @param key - The key to delete as a Uint8Array, ArrayBuffer, or string.
 * @throws {TypeError} If called outside of a transaction context.
 * @throws {DatabaseError} With code "INVALID_TRANSACTION" if transaction is invalid or already closed.
 * @throws {DatabaseError} With code "KEY_TOO_LONG" if key exceeds maximum allowed length.
 * @throws {DatabaseError} With code "LMDB-{code}" for LMDB-specific errors.
 */
export function del(key: Data): void {
    lowlevel.del(getTransaction().id, toBuffer(key));
}

/**
 * Initialize the database with the specified directory path.
 * This function may only be called once. If it is not called before the first transact(),
 * the database will be automatically initialized with the default directory.
 * 
 * @param dbDir - Optional directory path for the database (defaults to environment variable $OLMDB_DIR or "./.olmdb").
 * @throws {DatabaseError} With code "DUP_INIT" if database is already initialized.
 * @throws {DatabaseError} With code "CREATE_DIR_FAILED" if directory creation fails.
 * @throws {DatabaseError} With code "LMDB-{code}" for LMDB-specific errors.
 * 
 * @example
 * ```typescript
 * init("./my-database");
 * ```
 */
export function init(dbDir?: string): void {
    isInitialized = true;
    lowlevel.init(globalCommitHandler, dbDir);
}

/**
 * Registers a callback to be executed when the current transaction is reverted (aborted due to error).
 * The callback will be executed outside of transaction context.
 * 
 * @param callback - Function to execute when transaction is reverted
 * @throws {TypeError} If called outside of a transaction context
 */
export function onRevert(callback: () => void): void {
    const transaction = getTransaction();
    (transaction.onRevertCallbacks ||= []).push(callback);
}

/**
 * Registers a callback to be executed when the current transaction commits successfully.
 * The callback will be executed outside of transaction context.
 * 
 * @param callback - Function to execute when transaction commits
 * @throws {TypeError} If called outside of a transaction context
 */
export function onCommit(callback: () => void): void {
    const transaction = getTransaction();
    (transaction.onCommitCallbacks ||= []).push(callback);
}

/**
 * Executes a function within a database transaction context.
 * 
 * All database operations (get, put, del) must be performed within a transaction.
 * Transactions are automatically committed if the function completes successfully,
 * or aborted if an error occurs. Failed transactions may be automatically retried
 * up to 3 times in case of validation conflicts.
 * 
 * @template T - The return type of the transaction function
 * @param fn - The function to execute within the transaction context
 * @returns A promise that resolves with the function's return value
 * @throws {TypeError} If nested transactions are attempted
 * @throws {DatabaseError} With code "RACING_TRANSACTION" if the transaction fails after retries due to conflicts
 * @throws {DatabaseError} With code "TRANSACTION_FAILED" if the transaction fails for other reasons
 * @throws {DatabaseError} With code "TXN_LIMIT" if maximum number of transactions is reached
 * @throws {DatabaseError} With code "LMDB-{code}" for LMDB-specific errors
 * 
 * @example
 * ```typescript
 * const result = await transact(() => {
 *   const value = get(keyBytes);
 *   if (value) {
 *     put(keyBytes, newValueBytes);
 *   }
 *   return value;
 * });
 * ```
 */
export function transact<T>(fn: () => T): Promise<T> {
    // Auto-initialize if needed
    if (!isInitialized) init();

    if (transactionStorage.getStore()) {
        throw new TypeError("Nested transactions are not allowed");
    }
    
    return new Promise((resolve, reject) => {
        tryTransaction({
            fn, 
            resolve, 
            reject, 
            retryCount: 0, 
            id: -1
        });
    });
}

/**
 * Represents a key-value pair returned by the iterator
 */
export interface DbEntry<K,V> {
    key: K;
    value: V;
}

/**
 * Database iterator that implements the standard TypeScript iterator protocol
 */
export class DbIterator<K,V> extends Iterator<DbEntry<K,V>,undefined> implements Iterator<DbEntry<K,V>,undefined> {
    private iteratorId: number;
    private convertKey: (buffer: ArrayBuffer) => K;
    private convertValue: (buffer: ArrayBuffer) => V;
    constructor(iteratorId: number, convertKey: (buffer: ArrayBuffer) => K, convertValue: (buffer: ArrayBuffer) => V) {
        super();
        this.iteratorId = iteratorId;
        this.convertKey = convertKey;
        this.convertValue = convertValue;
    }
    
    [Symbol.iterator](): DbIterator<K,V> {
        return this;
    }
    
    /**
     * Advances the iterator to the next key-value pair.
     * 
     * @returns An IteratorResult with the next DbEntry or done: true if no more entries.
     * @throws {DatabaseError} With code "INVALID_ITERATOR" if iterator is invalid or already closed.
     * @throws {DatabaseError} With code "LMDB-{code}" for LMDB-specific errors.
     */
    next(): IteratorResult<DbEntry<K,V>> {
        if (this.iteratorId < 0) {
            return { done: true, value: undefined };
        }
        const result = lowlevel.readIterator(this.iteratorId);
        if (!result) {
            this.close();
            return { done: true, value: undefined };
        }
        return {
            done: false,
            value: {
                key: this.convertKey(result.key),
                value: this.convertValue(result.value),
            }
        };
    }
    
    /**
     * Closes the iterator and frees its resources.
     * Should be called when done iterating to prevent resource leaks.
     * 
     * @throws {DatabaseError} With code "INVALID_ITERATOR" if iterator is invalid or already closed.
     */
    close(): void {
        if (this.iteratorId >= 0) {
            lowlevel.closeIterator(this.iteratorId);
            this.iteratorId = -1;
        }
    }
}

/**
 * Creates an iterator to scan through database entries within the current transaction.
 * 
 * The iterator implements the standard TypeScript iterator protocol and can be used with for...of loops.
 * Supports both forward and reverse iteration with optional start and end boundaries.
 * 
 * @template K - The type to convert keys to (defaults to Uint8Array).
 * @template V - The type to convert values to (defaults to Uint8Array).
 * @param opts - Configuration options for the scan operation.
 * @param opts.start - Optional starting key for iteration. If not provided, starts from the beginning/end.
 * @param opts.end - Optional ending key for iteration. Iteration stops before reaching this key.
 * @param opts.reverse - Whether to iterate in reverse order (defaults to false).
 * @param opts.keyConvert - Function to convert key ArrayBuffers to type K (defaults to asArray).
 * @param opts.valueConvert - Function to convert value ArrayBuffers to type V (defaults to asArray).
 * @returns A DbIterator instance.
 * @throws {TypeError} If called outside of a transaction context.
 * @throws {DatabaseError} With code "INVALID_TRANSACTION" if transaction is invalid or already closed.
 * @throws {DatabaseError} With code "KEY_TOO_LONG" if start or end key exceeds maximum allowed length.
 * @throws {DatabaseError} With code "ITERATOR_LIMIT" if maximum number of iterators is reached.
 * @throws {DatabaseError} With code "OUT_OF_MEMORY" if memory allocation fails.
 * 
 * @example
 * ```typescript
 * await transact(() => {
 *   // Iterate over all entries
 *   for (const { key, value } of scan()) {
 *     console.log('Key:', key, 'Value:', value);
 *   }
 *   
 *   // Iterate with string conversion
 *   for (const { key, value } of scan({ 
 *     keyConvert: asString, 
 *     valueConvert: asString 
 *   })) {
 *     console.log('Key:', key, 'Value:', value);
 *   }
 *   
 *   // Iterate with boundaries
 *   for (const { key, value } of scan({ 
 *     start: "prefix_", 
 *     end: "prefix~" 
 *   })) {
 *     console.log('Key:', key, 'Value:', value);
 *   }
 *   
 *   // Manual iteration control
 *   const iter = scan({ reverse: true });
 *   const first = iter.next();
 *   iter.close(); // Always close when done early
 * });
 * ```
 */
export function scan<K = Uint8Array,V = Uint8Array>(opts: ScanOptions<K,V> = {}): DbIterator<K,V> {
    const transaction = getTransaction();
    const iteratorId = lowlevel.createIterator(transaction.id, opts.start==null ? undefined : toBuffer(opts.start), opts.end==null ? undefined : toBuffer(opts.end), opts.reverse || false);
    return new DbIterator(iteratorId,
        opts.keyConvert || asArray as (b: ArrayBuffer) => K,
        opts.valueConvert || asArray as (b: ArrayBuffer) => V
    );
}

interface ScanOptions<K = Uint8Array,V = Uint8Array> {
    start?: Data; // Starting key for iteration
    end?: Data; // Ending key for iteration
    reverse?: boolean; // Whether to iterate in reverse order
    keyConvert?: (buffer: ArrayBuffer) => K,
    valueConvert?: (buffer: ArrayBuffer) => V,
}

/**
 * Converts an ArrayBuffer to a Uint8Array.
 * Helper function for use with scan() keyConvert and valueConvert options.
 * 
 * @param buffer - The ArrayBuffer to convert.
 * @returns A new Uint8Array view of the buffer.
 */
export function asArray(buffer: ArrayBuffer): Uint8Array { return new Uint8Array(buffer); }

/**
 * Returns the ArrayBuffer as-is.
 * Helper function for use with scan() keyConvert and valueConvert options.
 * 
 * @param buffer - The ArrayBuffer to return.
 * @returns The same ArrayBuffer.
 */
export function asBuffer(buffer: ArrayBuffer): ArrayBuffer { return buffer; }

/**
 * Converts an ArrayBuffer to a UTF-8 decoded string.
 * Helper function for use with scan() keyConvert and valueConvert options.
 * 
 * @param buffer - The ArrayBuffer to decode.
 * @returns A UTF-8 decoded string.
 */
export function asString(buffer: ArrayBuffer): string { return decoder.decode(buffer); }

