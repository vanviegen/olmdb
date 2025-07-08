import * as lowlevel from "../dist/lowlevel.js";
import { describe, test, expect, beforeEach, beforeAll } from "@jest/globals";


// Helper functions
function stringToArrayBuffer(str: string): ArrayBufferLike {
    const encoder = new TextEncoder();
    return encoder.encode(str).buffer;
}

function arrayBufferToString(buffer: ArrayBuffer): string {
    const decoder = new TextDecoder();
    return decoder.decode(buffer);
}

// Map to track pending transaction commits
const pendingCommits = new Map<number, { resolve: (success: boolean) => void }>();

function commitAndWait(transactionId: number): Promise<boolean> {
    return new Promise((resolve) => {
        // Store the resolver in our map
        pendingCommits.set(transactionId, { resolve });
        
        // Attempt to commit the transaction
        const isImmediate = lowlevel.commitTransaction(transactionId);
        
        if (isImmediate) {
            // Read-only transaction completed immediately
            pendingCommits.delete(transactionId);
            resolve(true);
        }
        // If not immediate, the onCommit callback will handle resolution
    });
}

try {
    // Initialize with global onCommit callback
    lowlevel.init((transactionId: number, success: boolean) => {
        // Resolve the corresponding promise
        const pendingCommit = pendingCommits.get(transactionId);
        if (pendingCommit) {
            pendingCommit.resolve(success);
            pendingCommits.delete(transactionId);
        }
    }, "./.olmdb_test");
} catch (e: any) {
    if (e.code !== "DUP_INIT") throw e;
}

describe('Lowlevel Tests', () => {
    beforeEach(async () => {
        // Clear the database before each test
        const txnId = lowlevel.startTransaction();
        const iterId = lowlevel.createIterator(txnId);
        
        let item;
        while ((item = lowlevel.readIterator(iterId)) !== undefined) {
            lowlevel.del(txnId, item.key);
        }
        lowlevel.closeIterator(iterId);
        await commitAndWait(txnId);
    });
    
    describe("CRUD Operations", () => {
        test("basic put/get/delete", async () => {
            // Test PUT operation
            const txnId = lowlevel.startTransaction();
            lowlevel.put(txnId, stringToArrayBuffer("test1"), stringToArrayBuffer("value1"));
            await commitAndWait(txnId);
            
            // Verify PUT results
            const readTxnId = lowlevel.startTransaction();
            const value = lowlevel.get(readTxnId, stringToArrayBuffer("test1"));
            expect(value).toBeDefined();
            expect(arrayBufferToString(value!)).toBe("value1");
            
            // Test DELETE operation
            lowlevel.del(readTxnId, stringToArrayBuffer("test1"));
            await commitAndWait(readTxnId);
            
            // Verify DELETE results
            const verifyTxnId = lowlevel.startTransaction();
            const deletedValue = lowlevel.get(verifyTxnId, stringToArrayBuffer("test1"));
            expect(deletedValue).toBeUndefined();
            await commitAndWait(verifyTxnId);
        });
    });
    
    describe("Transaction Properties", () => {
        test("read own writes", async () => {
            const txnId = lowlevel.startTransaction();
            
            // Initial write and read
            lowlevel.put(txnId, stringToArrayBuffer("test1"), stringToArrayBuffer("value1"));
            let value = lowlevel.get(txnId, stringToArrayBuffer("test1"));
            expect(arrayBufferToString(value!)).toBe("value1");
            
            // Update and verify
            lowlevel.put(txnId, stringToArrayBuffer("test1"), stringToArrayBuffer("value2"));
            value = lowlevel.get(txnId, stringToArrayBuffer("test1"));
            expect(arrayBufferToString(value!)).toBe("value2");
            
            // Delete and verify
            lowlevel.del(txnId, stringToArrayBuffer("test1"));
            value = lowlevel.get(txnId, stringToArrayBuffer("test1"));
            expect(value).toBeUndefined();
            
            await commitAndWait(txnId);
        });
        
        test("concurrent validation", async () => {
            const txnId = lowlevel.startTransaction();
            lowlevel.put(txnId, stringToArrayBuffer("test1"), stringToArrayBuffer("value1"));
            lowlevel.put(txnId, stringToArrayBuffer("test2"), stringToArrayBuffer("value2"));
            await commitAndWait(txnId);
            
            // First transaction setup
            const txn1Id = lowlevel.startTransaction();
            const value1 = lowlevel.get(txn1Id, stringToArrayBuffer("concurrent1"));
            expect(value1).toBeUndefined();
            
            // Second transaction modification
            const txn2Id = lowlevel.startTransaction();
            lowlevel.put(txn2Id, stringToArrayBuffer("concurrent1"), stringToArrayBuffer("changed"));
            await commitAndWait(txn2Id);
            
            // Validation failure check
            lowlevel.put(txn1Id, stringToArrayBuffer("concurrent1"), stringToArrayBuffer("conflict"));
            const success = await commitAndWait(txn1Id);
            expect(success).toBe(false); // Expecting commit to fail
            
            // Final state verification
            const finalTxnId = lowlevel.startTransaction();
            const finalValue = lowlevel.get(finalTxnId, stringToArrayBuffer("concurrent1"));
            expect(arrayBufferToString(finalValue!)).toBe("changed");
            await commitAndWait(finalTxnId);
        });
        
        test("isolation level", async () => {
            // Setup uncommitted data in transaction 1
            const txn1Id = lowlevel.startTransaction();
            lowlevel.put(txn1Id, stringToArrayBuffer("test1"), stringToArrayBuffer("value1"));
            
            // Verify transaction 2 can't see uncommitted data
            const txn2Id = lowlevel.startTransaction();
            let value = lowlevel.get(txn2Id, stringToArrayBuffer("test1"));
            expect(value).toBeUndefined();
            
            // Commit transaction 1
            await commitAndWait(txn1Id);
            
            // Verify transaction 2 still can't see committed data (snapshot isolation)
            value = lowlevel.get(txn2Id, stringToArrayBuffer("test1"));
            expect(value).toBeUndefined();
            
            await commitAndWait(txn2Id);
        });
    });
    
    describe("Iterator Operations", () => {
        test("forward scan", async () => {
            // Setup test data
            const txnId = lowlevel.startTransaction();
            lowlevel.put(txnId, stringToArrayBuffer("key3"), stringToArrayBuffer("value3"));
            lowlevel.put(txnId, stringToArrayBuffer("key1"), stringToArrayBuffer("value1"));
            lowlevel.put(txnId, stringToArrayBuffer("key5"), stringToArrayBuffer("value5"));
            await commitAndWait(txnId);
            
            // Create iterator and scan forward
            const readTxnId = lowlevel.startTransaction();
            const iterId = lowlevel.createIterator(readTxnId);
            
            let item = lowlevel.readIterator(iterId);
            expect(item).toBeDefined();
            expect(arrayBufferToString(item!.key)).toBe("key1");
            expect(arrayBufferToString(item!.value)).toBe("value1");
            
            item = lowlevel.readIterator(iterId);
            expect(arrayBufferToString(item!.key)).toBe("key3");
            
            item = lowlevel.readIterator(iterId);
            expect(arrayBufferToString(item!.key)).toBe("key5");
            
            // Verify end of iteration
            item = lowlevel.readIterator(iterId);
            expect(item).toBeUndefined();
            
            lowlevel.closeIterator(iterId);
            await commitAndWait(readTxnId);
        });
        
        test("reverse scan", async () => {
            // Setup test data
            const txnId = lowlevel.startTransaction();
            lowlevel.put(txnId, stringToArrayBuffer("key1"), stringToArrayBuffer("value1"));
            lowlevel.put(txnId, stringToArrayBuffer("key3"), stringToArrayBuffer("value3"));
            lowlevel.put(txnId, stringToArrayBuffer("key5"), stringToArrayBuffer("value5"));
            await commitAndWait(txnId);
            
            // Create reverse iterator
            const readTxnId = lowlevel.startTransaction();
            const iterId = lowlevel.createIterator(readTxnId, undefined, undefined, true);
            
            let item = lowlevel.readIterator(iterId);
            expect(arrayBufferToString(item!.key)).toBe("key5");
            
            item = lowlevel.readIterator(iterId);
            expect(arrayBufferToString(item!.key)).toBe("key3");
            
            item = lowlevel.readIterator(iterId);
            expect(arrayBufferToString(item!.key)).toBe("key1");
            
            // Verify end of iteration
            item = lowlevel.readIterator(iterId);
            expect(item).toBeUndefined();
            
            lowlevel.closeIterator(iterId);
            await commitAndWait(readTxnId);
        });
        
        test("key positioning", async () => {
            // Setup test data
            const txnId = lowlevel.startTransaction();
            lowlevel.put(txnId, stringToArrayBuffer("key1"), stringToArrayBuffer("value1"));
            lowlevel.put(txnId, stringToArrayBuffer("key3"), stringToArrayBuffer("value3"));
            lowlevel.put(txnId, stringToArrayBuffer("key5"), stringToArrayBuffer("value5"));
            await commitAndWait(txnId);
            
            const readTxnId = lowlevel.startTransaction();
            
            // Forward positioning at existing key
            let iterId = lowlevel.createIterator(readTxnId, stringToArrayBuffer("key3"));
            let item = lowlevel.readIterator(iterId);
            expect(arrayBufferToString(item!.key)).toBe("key3");
            lowlevel.closeIterator(iterId);
            
            // Position at non-existent key (should find next key)
            iterId = lowlevel.createIterator(readTxnId, stringToArrayBuffer("key2"));
            item = lowlevel.readIterator(iterId);
            expect(arrayBufferToString(item!.key)).toBe("key3");
            lowlevel.closeIterator(iterId);
            
            // Backward positioning
            iterId = lowlevel.createIterator(readTxnId, stringToArrayBuffer("key4"), undefined, true);
            item = lowlevel.readIterator(iterId);
            expect(arrayBufferToString(item!.key)).toBe("key3");
            lowlevel.closeIterator(iterId);
            
            await commitAndWait(readTxnId);
        });
        
        test("visibility of transaction writes", async () => {
            // Setup initial committed data
            const txnId = lowlevel.startTransaction();
            lowlevel.put(txnId, stringToArrayBuffer("key1"), stringToArrayBuffer("value1"));
            lowlevel.put(txnId, stringToArrayBuffer("key3"), stringToArrayBuffer("value3"));
            await commitAndWait(txnId);
            
            // Add uncommitted write and create iterator
            const readTxnId = lowlevel.startTransaction();
            lowlevel.put(readTxnId, stringToArrayBuffer("key2"), stringToArrayBuffer("value2"));
            const iterId = lowlevel.createIterator(readTxnId);
            
            // Verify first committed record
            let item = lowlevel.readIterator(iterId);
            expect(arrayBufferToString(item!.key)).toBe("key1");
            expect(arrayBufferToString(item!.value)).toBe("value1");
            
            // Modify existing record
            lowlevel.put(readTxnId, stringToArrayBuffer("key3"), stringToArrayBuffer("modified3"));
            
            // Verify uncommitted write is visible
            item = lowlevel.readIterator(iterId);
            expect(arrayBufferToString(item!.key)).toBe("key2");
            expect(arrayBufferToString(item!.value)).toBe("value2");
            
            // Verify modified record is visible
            item = lowlevel.readIterator(iterId);
            expect(arrayBufferToString(item!.key)).toBe("key3");
            expect(arrayBufferToString(item!.value)).toBe("modified3");
            
            // Verify end of iteration
            item = lowlevel.readIterator(iterId);
            expect(item).toBeUndefined();
            
            lowlevel.closeIterator(iterId);
            await commitAndWait(readTxnId);
        });
        
        test("uncommitted changes visibility", async () => {
            // Buffer uncommitted writes
            const txnId = lowlevel.startTransaction();
            lowlevel.put(txnId, stringToArrayBuffer("uKey1"), stringToArrayBuffer("uVal1"));
            lowlevel.put(txnId, stringToArrayBuffer("uKey2"), stringToArrayBuffer("uVal2"));
            lowlevel.put(txnId, stringToArrayBuffer("uKey3"), stringToArrayBuffer("uVal3"));
            
            // Create iterator over uncommitted data
            const iterId = lowlevel.createIterator(txnId);
            let item = lowlevel.readIterator(iterId);
            expect(arrayBufferToString(item!.key)).toBe("uKey1");
            expect(arrayBufferToString(item!.value)).toBe("uVal1");
            
            // Delete key and continue iteration
            lowlevel.del(txnId, stringToArrayBuffer("uKey3"));
            
            item = lowlevel.readIterator(iterId);
            expect(arrayBufferToString(item!.key)).toBe("uKey2");
            expect(arrayBufferToString(item!.value)).toBe("uVal2");
            
            // Verify end of iteration after delete
            item = lowlevel.readIterator(iterId);
            expect(item).toBeUndefined();
            
            lowlevel.closeIterator(iterId);
            await commitAndWait(txnId);
        });
        
        test("delete while iterating", async () => {
            // Setup test data
            const txnId = lowlevel.startTransaction();
            lowlevel.put(txnId, stringToArrayBuffer("key1"), stringToArrayBuffer("value1"));
            lowlevel.put(txnId, stringToArrayBuffer("key2"), stringToArrayBuffer("value2"));
            lowlevel.put(txnId, stringToArrayBuffer("key3"), stringToArrayBuffer("value3"));
            lowlevel.put(txnId, stringToArrayBuffer("key4"), stringToArrayBuffer("value4"));
            lowlevel.put(txnId, stringToArrayBuffer("key5"), stringToArrayBuffer("value5"));
            await commitAndWait(txnId);
            
            // Iterate and delete all keys
            const deleteTxnId = lowlevel.startTransaction();
            const iterId = lowlevel.createIterator(deleteTxnId);
            let iterationCount = 0;
            let item;
            
            while ((item = lowlevel.readIterator(iterId)) !== undefined) {
                iterationCount++;
                lowlevel.del(deleteTxnId, item.key);
            }
            
            // Verify iteration count
            expect(iterationCount).toBe(5);
            lowlevel.closeIterator(iterId);
            
            // Verify uncommitted database is empty
            const verifyIterId = lowlevel.createIterator(deleteTxnId);
            item = lowlevel.readIterator(verifyIterId);
            expect(item).toBeUndefined();
            lowlevel.closeIterator(verifyIterId);
            
            await commitAndWait(deleteTxnId);
            
            // Verify database is empty after commit
            const finalTxnId = lowlevel.startTransaction();
            const finalIterId = lowlevel.createIterator(finalTxnId);
            item = lowlevel.readIterator(finalIterId);
            expect(item).toBeUndefined();
            lowlevel.closeIterator(finalIterId);
            await commitAndWait(finalTxnId);
        });
        
        test("end key bounds", async () => {
            // Setup test data
            const txnId = lowlevel.startTransaction();
            lowlevel.put(txnId, stringToArrayBuffer("key1"), stringToArrayBuffer("value1"));
            lowlevel.put(txnId, stringToArrayBuffer("key2"), stringToArrayBuffer("value2"));
            lowlevel.put(txnId, stringToArrayBuffer("key3"), stringToArrayBuffer("value3"));
            lowlevel.put(txnId, stringToArrayBuffer("key4"), stringToArrayBuffer("value4"));
            lowlevel.put(txnId, stringToArrayBuffer("key5"), stringToArrayBuffer("value5"));
            await commitAndWait(txnId);
            
            const readTxnId = lowlevel.startTransaction();
            
            // Forward iteration with endKey
            let iterId = lowlevel.createIterator(
                readTxnId,
                stringToArrayBuffer("key2"),
                stringToArrayBuffer("key4")
            );
            
            // Should get key2
            let item = lowlevel.readIterator(iterId);
            expect(arrayBufferToString(item!.key)).toBe("key2");
            
            // Should get key3
            item = lowlevel.readIterator(iterId);
            expect(arrayBufferToString(item!.key)).toBe("key3");
            
            // Should stop before key4 (iterator respects endKey bound)
            item = lowlevel.readIterator(iterId);
            while (item !== undefined) {
                expect(arrayBufferToString(item.key)).not.toBe("key4");
                item = lowlevel.readIterator(iterId);
            }
            lowlevel.closeIterator(iterId);
            
            // Reverse iteration with endKey
            iterId = lowlevel.createIterator(
                readTxnId,
                stringToArrayBuffer("key4"),
                stringToArrayBuffer("key1"),
                true
            );
            
            // Should get key4
            item = lowlevel.readIterator(iterId);
            expect(arrayBufferToString(item!.key)).toBe("key4");
            
            // Should get key3
            item = lowlevel.readIterator(iterId);
            expect(arrayBufferToString(item!.key)).toBe("key3");
            
            // Should get key2
            item = lowlevel.readIterator(iterId);
            expect(arrayBufferToString(item!.key)).toBe("key2");
            
            // Should stop before key1 (iterator respects endKey bound)
            item = lowlevel.readIterator(iterId);
            while (item !== undefined) {
                expect(arrayBufferToString(item.key)).not.toBe("key1");
                item = lowlevel.readIterator(iterId);
            }
            
            lowlevel.closeIterator(iterId);
            await commitAndWait(readTxnId);
        });
    });
});