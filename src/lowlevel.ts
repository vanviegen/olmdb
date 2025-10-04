import * as os from "node:os";
import { dlopen } from "process";
import { fileURLToPath } from 'url';
import { dirname, resolve } from 'path';
import { existsSync } from "node:fs";

const PACKAGE_ROOT = (function() {
  let dir = dirname(fileURLToPath(import.meta.url));
  while (dir !== dirname(dir)) {
    if (existsSync(resolve(dir, 'package.json'))) return dir;
    dir = dirname(dir);
  }
  throw new Error('package.json not found');
})();

// Take from OLMDB_BIN_DIR, or default to <package_root>/build/release
const BIN_DIR = process.env.OLMDB_BIN_DIR || resolve(PACKAGE_ROOT, 'build', 'Release');

const lowlevel = {} as any;
dlopen({exports: lowlevel}, `${BIN_DIR}/transaction_client.node`, os.constants.dlopen.RTLD_NOW);

/**
 * Initializes the database system with the specified directory.
 * 
 * Can be called multiple times if directory and commitWorkerBin are identical.
 * 
 * @param directory Optional path to the database directory. If not provided,
 *   defaults to the OLMDB_DIR environment variable or "./.olmdb".
 * @param commitWorkerBin Path to the commit worker binary. Defaults to
 *  `<base_dir>/build/release/commit_worker`.
 * @throws DatabaseError if initialization fails
 */
export function init(
  directory?: string,
  commitWorkerBin: string = `${BIN_DIR}/commit_worker`
): void {
  lowlevel.init(directory, commitWorkerBin);
}

/**
 * Starts a new transaction for database operations.
 * 
 * @returns A transaction ID (positive integer) to be used in subsequent operations
 * @throws DatabaseError if the transaction cannot be created
 */
export const startTransaction = lowlevel.startTransaction as () => number;

/**
 * Commits the transaction with the given ID.
 * 
 * If the transaction is read-only, returns the commit sequence number immediately.
 * If the transaction has modifications, returns a Promise that resolves to the commit sequence when the commit completes.
 * 
 * @param transactionId The ID of the transaction to commit
 * @returns For read-only transactions: the commit sequence number (synchronous)
 *          For write transactions: a Promise that resolves to the commit sequence number (0 when the transaction failed due to conflicts)
 * @throws DatabaseError if the transaction cannot be committed
 */
export const commitTransaction = lowlevel.commitTransaction as (transactionId: number) => number | Promise<number>;

/**
 * Aborts the transaction with the given ID, discarding all changes.
 * 
 * @param transactionId The ID of the transaction to abort
 * @throws DatabaseError if the transaction cannot be aborted
 */
export const abortTransaction = lowlevel.abortTransaction as (transactionId: number) => void;

/**
 * Retrieves a value for the given key within a transaction.
 * 
 * @param transactionId The ID of the transaction
 * @param key Key to look up
 * @returns The value if found, or undefined if the key doesn't exist
 * @throws DatabaseError if the operation fails
 */
export const get = lowlevel.get as (
  transactionId: number, 
  key: ArrayBufferLike
) => ArrayBuffer | undefined;

/**
 * Stores a key-value pair within a transaction.
 * 
 * @param transactionId The ID of the transaction
 * @param key Key to store
 * @param value Value to store
 * @throws DatabaseError if the operation fails
 */
export const put = lowlevel.put as (
  transactionId: number, 
  key: ArrayBufferLike, 
  value: ArrayBufferLike
) => void;

/**
 * Deletes a key-value pair within a transaction.
 * 
 * @param transactionId The ID of the transaction
 * @param key Key to delete
 * @throws DatabaseError if the operation fails
 */
export const del = lowlevel.del as (
  transactionId: number, 
  key: ArrayBufferLike
) => void;

/**
 * Creates an iterator for scanning a range of keys within a transaction.
 * 
 * @param transactionId The ID of the transaction
 * @param startKey Optional key to start iteration from (inclusive)
 * @param endKey Optional key to end iteration at (exclusive)
 * @param reverse If true, keys are returned in descending order
 * @returns An iterator ID to be used with readIterator() and closeIterator()
 * @throws DatabaseError if the operation fails
 */
export const createIterator = lowlevel.createIterator as (
  transactionId: number,
  startKey?: ArrayBufferLike,
  endKey?: ArrayBufferLike,
  reverse?: boolean
) => number;

/**
 * Reads the next key-value pair from an iterator.
 * 
 * @param iteratorId The ID of the iterator
 * @returns An object containing the key and value, or undefined if iteration is complete
 * @throws DatabaseError if the operation fails
 */
export const readIterator = lowlevel.readIterator as (
  iteratorId: number
) => { key: ArrayBuffer; value: ArrayBuffer } | undefined;

/**
 * Closes an iterator when it's no longer needed.
 * 
 * @param iteratorId The ID of the iterator to close
 * @throws DatabaseError if the operation fails
 */
export const closeIterator = lowlevel.closeIterator as (
  iteratorId: number
) => void;

/**
 * Interface for DatabaseError, which extends the standard Error class.
 * Contains an additional code property for machine-readable error identification.
 */
export interface DatabaseError extends Error {
  /**
   * A machine-readable string code identifying the type of error.
   * Example codes include: "NOT_INIT", "INVALID_TRANSACTION", "KEY_TOO_LONG", etc.
   */
  code: string;
}

/**
 * Constructor interface for DatabaseError.
 */
export interface DatabaseErrorConstructor {
  /**
   * Creates a new DatabaseError with the specified message and code.
   * 
   * @param message Human-readable error message
   * @param code Machine-readable error code
   */
  new (message: string, code: string): DatabaseError;
  prototype: DatabaseError;
}

/**
 * The DatabaseError class is used to represent errors that occur during database operations.
 * It extends the built-in Error class and has a machine readable error code string property.
 * 
 * The lowlevel API will throw DatabaseError instances for all database-related errors.
 * Invalid function arguments will throw TypeError.
 */
export const DatabaseError = lowlevel.DatabaseError as DatabaseErrorConstructor;
