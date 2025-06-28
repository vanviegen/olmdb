/**
 * TypeScript definitions for the OLMDB-lowlevel native module.
 * 
 */

declare module "*/olmdb_lowlevel.node" {
  
  /**
   * Custom error class for database-related errors
   */
  class DatabaseError extends Error {
    /**
     * Creates a new DatabaseError
     * @param message Error message
     * @param code Error code
     */
    constructor(message: string, code?: string);
    
    /**
     * Error code identifying the specific error type
     */
    code?: string;
  }

  /**
   * Initialize the database with an optional directory path.
   * @param directory Optional directory path for the database (defaults to environment variable $OLMDB_DIR or "./.olmdb").
   * @throws {DatabaseError} With code "ALREADY_OPEN" if database is already initialized.
   * @throws {DatabaseError} With code "CREATE_DIR_FAILED" if directory creation fails.
   * @throws {DatabaseError} With code "LMDB-{code}" for LMDB-specific errors.
   */
  function open(directory?: string): void;
  
  /**
   * Start a new transaction.
   * @returns Transaction ID to be used with other functions.
   * @throws {DatabaseError} With code "TXN_LIMIT" if maximum number of transactions is reached.
   * @throws {DatabaseError} With code "LMDB-{code}" for LMDB-specific errors.
   */
  function startTransaction(): number;

  /**
   * Commit a transaction, making all changes permanent.
   * @param transactionId A transaction ID returned by `startTransaction`. If the transaction is read-only, it will be closed immediately. If the transaction is writeable, it will be committed asynchronously and the callback will be invoked with the transaction ID and final status. The transaction will be closed and cannot be used again.
   * @param callback Optional callback function for read/write transactions, receives transaction ID and status (TRANSACTION_SUCCEEDED, TRANSACTION_FAILED on db error, TRANSACTION_RACED when data changed after read and before commit).
   * @returns `false` if the transaction is read-only and closed immediately, or `true` if read/write and the commit will be processed asynchronously. The callback will be invoked when the commit is complete.
   * @throws {DatabaseError} With code "INVALID_TRANSACTION" if transaction ID is invalid or already closed.
   */
  function commitTransaction(
    transactionId: number,
    callback?: (transactionId: number, status: number) => void
  ): number;

  /**
   * Abort a transaction (reverting changes) and release all resources.
   * @param transactionId A transaction ID returned by `startTransaction`. It will be closed and cannot be used again.
   * @throws {DatabaseError} With code "INVALID_TRANSACTION" if transaction ID is invalid or already closed.
   */
  function abortTransaction(transactionId: number): void;

  /**
   * Get a value from the database.
   * @param transactionId A transaction ID returned by `startTransaction`.
   * @param key The key as an ArrayBuffer.
   * @returns ArrayBuffer with the value or undefined if not found.
   * @throws {DatabaseError} With code "INVALID_TRANSACTION" if transaction ID is invalid or already closed.
   * @throws {DatabaseError} With code "KEY_TOO_LONG" if key exceeds maximum allowed length.
   * @throws {DatabaseError} With code "LMDB-{code}" for LMDB-specific errors.
   */
  function get(transactionId: number, key: ArrayBuffer): ArrayBuffer | undefined;

  /**
   * Put a key-value pair into the database
   * @param transactionId A transaction ID returned by `startTransaction`.
   * @param key The key as an ArrayBuffer.
   * @param value The value as an ArrayBuffer.
   * @throws {DatabaseError} With code "INVALID_TRANSACTION" if transaction ID is invalid or already closed.
   * @throws {DatabaseError} With code "KEY_TOO_LONG" if key exceeds maximum allowed length.
   */
  function put(transactionId: number, key: ArrayBuffer, value: ArrayBuffer): void;

  /**
   * Delete a key-value pair from the database.
   * @param transactionId A transaction ID returned by `startTransaction`.
   * @param key The key as an ArrayBuffer.
   * @throws {DatabaseError} With code "INVALID_TRANSACTION" if transaction ID is invalid or already closed.
   * @throws {DatabaseError} With code "KEY_TOO_LONG" if key exceeds maximum allowed length.
   */
  function del(transactionId: number, key: ArrayBuffer): void;

  /**
   * Create an iterator for scanning through the database.
   * @param transactionId The transaction ID.
   * @param startKey Optional starting key as an ArrayBuffer. Defaults to starting at the beginning/end.
   * @param endKey Optional ending key as an ArrayBuffer, meaning the iterator will stop before this key.
   * @param reverse Whether to iterate in reverse order.
   * @returns Iterator ID to be used with readIterator and closeIterator.
   * @throws {DatabaseError} With code "INVALID_TRANSACTION" if transaction ID is invalid or already closed.
   * @throws {DatabaseError} With code "KEY_TOO_LONG" if key exceeds maximum allowed length.
   * @throws {DatabaseError} With code "ITERATOR_LIMIT" if maximum number of iterators is reached.
   * @throws {DatabaseError} With code "OUT_OF_MEMORY" if memory allocation fails.
   */
  function createIterator(transactionId: number, startKey?: ArrayBuffer, endKey?: ArrayBuffer, reverse?: boolean): number;

  /**
   * Read the next item from an iterator.
   * @param iteratorId An iterator ID returned by `createIterator`.
   * @returns Object with key and value as ArrayBuffers, or undefined if no more items.
   * @throws {DatabaseError} With code "INVALID_ITERATOR" if iterator ID is invalid or already closed.
   * @throws {DatabaseError} With code "LMDB-{code}" for LMDB-specific errors.
   */
  function readIterator(iteratorId: number): { key: ArrayBuffer; value: ArrayBuffer } | undefined;

  /**
   * Close an iterator and free its resources.
   * @param iteratorId An iterator ID returned by `createIterator`.
   * @throws {DatabaseError} With code "INVALID_ITERATOR" if iterator ID is invalid or already closed.
   */
  function closeIterator(iteratorId: number): void;

  /**
   * Transaction status constants
   */
  /** Transaction failed due to a race condition (data changed by another transaction) */
  const TRANSACTION_RACED: 3;
  /** Transaction successfully committed */
  const TRANSACTION_SUCCEEDED: 4;
  /** Transaction failed due to an error */
  const TRANSACTION_FAILED: 5;
}