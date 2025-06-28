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

export const DatabaseError = lowlevel.DatabaseError as DatabaseErrorConstructor;
export const open = lowlevel.open as (directory? : string) => void;
export const startTransaction = lowlevel.startTransaction as () => number;
export const commitTransaction = lowlevel.commitTransaction as (
  transactionId: number,
  callback?: (transactionId: number, status: number) => void
) => number;
export const abortTransaction = lowlevel.abortTransaction as (transactionId: number) => void;
export const get = lowlevel.get as (transactionId: number, key: ArrayBufferLike) => ArrayBuffer | undefined;
export const put = lowlevel.put as (transactionId: number, key: ArrayBufferLike, value: ArrayBufferLike) => void;
export const del = lowlevel.del as (transactionId: number, key: ArrayBufferLike) => void;
export const createIterator = lowlevel.createIterator as (
  transactionId: number,
  startKey?: ArrayBufferLike,
  endKey?: ArrayBufferLike,
  reverse?: boolean
) => number;
export const readIterator = lowlevel.readIterator as (iteratorId: number) => { key: ArrayBuffer; value: ArrayBuffer } | undefined;
export const closeIterator = lowlevel.closeIterator as (iteratorId: number) => void;
export const TRANSACTION_RACED = lowlevel.TRANSACTION_RACED as number;
export const TRANSACTION_SUCCEEDED = lowlevel.TRANSACTION_SUCCEEDED as number;
export const TRANSACTION_FAILED = lowlevel.TRANSACTION_FAILED as number;
