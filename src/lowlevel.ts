import { dlopen } from "process";
import os from "node:os";

const lowlevel = {} as any;
const mode = process.env.OLMDB_DEBUG ? "debug" : "release";
dlopen({exports: lowlevel}, `${__dirname}/../dist/olmdb_lowlevel_${mode}.node`, os.constants.dlopen.RTLD_NOW);


export interface DatabaseError extends Error {
  code?: string;
}

export interface DatabaseErrorConstructor {
  new (message: string, code?: string): DatabaseError;
  prototype: DatabaseError;
}

/**
 * The DatabaseError class is used to represent errors that occur during database operations.
 * It extends the built-in Error class and has a machine readable error code string property.
 * 
 * The lowlevel API will throw DatabaseError instances for all errors, except for invalid
 * arguments counts and types, which will throw a TypeError.
 */
export const DatabaseError = lowlevel.DatabaseError as DatabaseErrorConstructor;

/**
 * Must be called before any other functions. A callback function for async transaction commits must be provided.
 */
export const init = lowlevel.init as (onCommit: (transactionId: number, success: boolean) => void, directory?: string) => number;

export const startTransaction = lowlevel.startTransaction as () => number;

/**
 * Commits the transaction with the given ID.
 * 
 * If the transaction is read-only, commit will have the same effect as abort, and will be instantaneous. In 
 * this case `true` is returned.
 * 
 * If the transaction needs to do updates, `false` will be returned and the work will be queued for the commit worker
 * to process asynchronously. Once done `onCommit` will be called.
 */
export const commitTransaction = lowlevel.commitTransaction as (transactionId: number) => boolean;
export const abortTransaction = lowlevel.abortTransaction as (transactionId: number) => number;
export const get = lowlevel.get as (transactionId: number, key: ArrayBufferLike) => ArrayBuffer | undefined;
export const put = lowlevel.put as (transactionId: number, key: ArrayBufferLike, value: ArrayBufferLike) => number;
export const del = lowlevel.del as (transactionId: number, key: ArrayBufferLike) => number;
export const createIterator = lowlevel.createIterator as (
  transactionId: number,
  startKey?: ArrayBufferLike,
  endKey?: ArrayBufferLike,
  reverse?: boolean
) => number;
export const readIterator = lowlevel.readIterator as (iteratorId: number) => { key: ArrayBuffer; value: ArrayBuffer } | undefined;
export const closeIterator = lowlevel.closeIterator as (iteratorId: number) => number;
export const getCommitResults = lowlevel.getCommitResults as () => Array<{ltxn_id: number, success: boolean}> | undefined;
export const errorCode = lowlevel.errorCode as () => string | undefined;
export const errorMessage = lowlevel.errorMessage as () => string | undefined;
export const TRANSACTION_RACED = lowlevel.TRANSACTION_RACED as number;
export const TRANSACTION_SUCCEEDED = lowlevel.TRANSACTION_SUCCEEDED as number;
export const TRANSACTION_FAILED = lowlevel.TRANSACTION_FAILED as number;
