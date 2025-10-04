import * as lowlevel from "../src/lowlevel.js";
import { describe, test, expect, beforeEach } from "@jest/globals";


// Helper functions
function stringToArrayBuffer(str: string): ArrayBufferLike {
    const encoder = new TextEncoder();
    return encoder.encode(str).buffer;
}

function arrayBufferToString(buffer: ArrayBuffer): string {
    const decoder = new TextDecoder();
    return decoder.decode(buffer);
}


lowlevel.init("./.olmdb_test");


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
        await lowlevel.commitTransaction(txnId);
    });
    
    describe("CRUD Operations", () => {
        test("basic put/get/delete", async () => {
            // Test PUT operation
            const txn1 = lowlevel.startTransaction();
            lowlevel.put(txn1, stringToArrayBuffer("test1"), stringToArrayBuffer("value1"));
            await lowlevel.commitTransaction(txn1);
            
            // Verify PUT results
            const txn2 = lowlevel.startTransaction();
            const value = lowlevel.get(txn2, stringToArrayBuffer("test1"));
            expect(value).toBeDefined();
            expect(arrayBufferToString(value!)).toBe("value1");
            
            // Test DELETE operation
            lowlevel.del(txn2, stringToArrayBuffer("test1"));
            await lowlevel.commitTransaction(txn2);
            
            // Verify DELETE results
            const txn3 = lowlevel.startTransaction();
            const deletedValue = lowlevel.get(txn3, stringToArrayBuffer("test1"));
            expect(deletedValue).toBeUndefined();
            await lowlevel.commitTransaction(txn3);
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
            
            await lowlevel.commitTransaction(txnId);
        });
        
        test("concurrent validation", async () => {
            const setupTxn = lowlevel.startTransaction();
            lowlevel.put(setupTxn, stringToArrayBuffer("test1"), stringToArrayBuffer("value1"));
            lowlevel.put(setupTxn, stringToArrayBuffer("test2"), stringToArrayBuffer("value2"));
            await lowlevel.commitTransaction(setupTxn);
            
            // First transaction setup
            const txn1 = lowlevel.startTransaction();
            const value1 = lowlevel.get(txn1, stringToArrayBuffer("concurrent1"));
            expect(value1).toBeUndefined();
            
            // Second transaction modification
            const txn2 = lowlevel.startTransaction();
            lowlevel.put(txn2, stringToArrayBuffer("concurrent1"), stringToArrayBuffer("changed"));
            await lowlevel.commitTransaction(txn2);
            
            // Validation failure check
            lowlevel.put(txn1, stringToArrayBuffer("concurrent1"), stringToArrayBuffer("conflict"));
            const success = await lowlevel.commitTransaction(txn1);
            expect(success).toBe(0); // Expecting commit to fail
            
            // Final state verification
            const finalTxn = lowlevel.startTransaction();
            const finalValue = lowlevel.get(finalTxn, stringToArrayBuffer("concurrent1"));
            expect(arrayBufferToString(finalValue!)).toBe("changed");
            await lowlevel.commitTransaction(finalTxn);
        });
        
        test("isolation level", async () => {
            // Setup uncommitted data in transaction 1
            const txn1 = lowlevel.startTransaction();
            lowlevel.put(txn1, stringToArrayBuffer("test1"), stringToArrayBuffer("value1"));
            
            // Verify transaction 2 can't see uncommitted data
            const txn2 = lowlevel.startTransaction();
            let value = lowlevel.get(txn2, stringToArrayBuffer("test1"));
            expect(value).toBeUndefined();
            
            // Commit transaction 1
            await lowlevel.commitTransaction(txn1);
            
            // Verify transaction 2 still can't see committed data (snapshot isolation)
            value = lowlevel.get(txn2, stringToArrayBuffer("test1"));
            expect(value).toBeUndefined();
            
            await lowlevel.commitTransaction(txn2);
        });
    });
    
    describe("Iterator Operations", () => {
        test("forward scan", async () => {
            // Setup test data
            const setupTxn = lowlevel.startTransaction();
            lowlevel.put(setupTxn, stringToArrayBuffer("key3"), stringToArrayBuffer("value3"));
            lowlevel.put(setupTxn, stringToArrayBuffer("key1"), stringToArrayBuffer("value1"));
            lowlevel.put(setupTxn, stringToArrayBuffer("key5"), stringToArrayBuffer("value5"));
            await lowlevel.commitTransaction(setupTxn);
            
            // Create iterator and scan forward
            const txnId = lowlevel.startTransaction();
            const iterId = lowlevel.createIterator(txnId);
            
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
            await lowlevel.commitTransaction(txnId);
        });
        
        test("reverse scan", async () => {
            // Setup test data
            const setupTxn = lowlevel.startTransaction();
            lowlevel.put(setupTxn, stringToArrayBuffer("key1"), stringToArrayBuffer("value1"));
            lowlevel.put(setupTxn, stringToArrayBuffer("key3"), stringToArrayBuffer("value3"));
            lowlevel.put(setupTxn, stringToArrayBuffer("key5"), stringToArrayBuffer("value5"));
            await lowlevel.commitTransaction(setupTxn);
            
            // Create reverse iterator
            const txnId = lowlevel.startTransaction();
            const iterId = lowlevel.createIterator(txnId, undefined, undefined, true);
            
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
            await lowlevel.commitTransaction(txnId);
        });
        
        test("key positioning", async () => {
            // Setup test data
            const setupTxn = lowlevel.startTransaction();
            lowlevel.put(setupTxn, stringToArrayBuffer("key1"), stringToArrayBuffer("value1"));
            lowlevel.put(setupTxn, stringToArrayBuffer("key3"), stringToArrayBuffer("value3"));
            lowlevel.put(setupTxn, stringToArrayBuffer("key5"), stringToArrayBuffer("value5"));
            await lowlevel.commitTransaction(setupTxn);
            
            const txnId = lowlevel.startTransaction();
            
            // Forward positioning at existing key
            let iterId = lowlevel.createIterator(txnId, stringToArrayBuffer("key3"));
            let item = lowlevel.readIterator(iterId);
            expect(arrayBufferToString(item!.key)).toBe("key3");
            lowlevel.closeIterator(iterId);
            
            // Position at non-existent key (should find next key)
            iterId = lowlevel.createIterator(txnId, stringToArrayBuffer("key2"));
            item = lowlevel.readIterator(iterId);
            expect(arrayBufferToString(item!.key)).toBe("key3");
            lowlevel.closeIterator(iterId);
            
            // Backward positioning
            iterId = lowlevel.createIterator(txnId, stringToArrayBuffer("key4"), undefined, true);
            item = lowlevel.readIterator(iterId);
            expect(arrayBufferToString(item!.key)).toBe("key3");
            lowlevel.closeIterator(iterId);
            
            await lowlevel.commitTransaction(txnId);
        });
        
        test("visibility of transaction writes", async () => {
            // Setup initial committed data
            const setupTxn = lowlevel.startTransaction();
            lowlevel.put(setupTxn, stringToArrayBuffer("key1"), stringToArrayBuffer("value1"));
            lowlevel.put(setupTxn, stringToArrayBuffer("key3"), stringToArrayBuffer("value3"));
            await lowlevel.commitTransaction(setupTxn);
            
            // Add uncommitted write and create iterator
            const txnId = lowlevel.startTransaction();
            lowlevel.put(txnId, stringToArrayBuffer("key2"), stringToArrayBuffer("value2"));
            const iterId = lowlevel.createIterator(txnId);
            
            // Verify first committed record
            let item = lowlevel.readIterator(iterId);
            expect(arrayBufferToString(item!.key)).toBe("key1");
            expect(arrayBufferToString(item!.value)).toBe("value1");
            
            // Modify existing record
            lowlevel.put(txnId, stringToArrayBuffer("key3"), stringToArrayBuffer("modified3"));
            
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
            await lowlevel.commitTransaction(txnId);
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
            await lowlevel.commitTransaction(txnId);
        });
        
        test("delete while iterating", async () => {
            // Setup test data
            const setupTxn = lowlevel.startTransaction();
            lowlevel.put(setupTxn, stringToArrayBuffer("key1"), stringToArrayBuffer("value1"));
            lowlevel.put(setupTxn, stringToArrayBuffer("key2"), stringToArrayBuffer("value2"));
            lowlevel.put(setupTxn, stringToArrayBuffer("key3"), stringToArrayBuffer("value3"));
            lowlevel.put(setupTxn, stringToArrayBuffer("key4"), stringToArrayBuffer("value4"));
            lowlevel.put(setupTxn, stringToArrayBuffer("key5"), stringToArrayBuffer("value5"));
            await lowlevel.commitTransaction(setupTxn);
            
            // Iterate and delete all keys
            const txnId = lowlevel.startTransaction();
            const iterId = lowlevel.createIterator(txnId);
            let iterationCount = 0;
            let item;
            
            while ((item = lowlevel.readIterator(iterId)) !== undefined) {
                iterationCount++;
                lowlevel.del(txnId, item.key);
            }
            
            // Verify iteration count
            expect(iterationCount).toBe(5);
            lowlevel.closeIterator(iterId);
            
            // Verify uncommitted database is empty
            const verifyIterId = lowlevel.createIterator(txnId);
            item = lowlevel.readIterator(verifyIterId);
            expect(item).toBeUndefined();
            lowlevel.closeIterator(verifyIterId);
            
            await lowlevel.commitTransaction(txnId);
            
            // Verify database is empty after commit
            const finalTxn = lowlevel.startTransaction();
            const finalIterId = lowlevel.createIterator(finalTxn);
            item = lowlevel.readIterator(finalIterId);
            expect(item).toBeUndefined();
            lowlevel.closeIterator(finalIterId);
            await lowlevel.commitTransaction(finalTxn);
        });
        
        test("end key bounds", async () => {
            // Setup test data
            const setupTxn = lowlevel.startTransaction();
            lowlevel.put(setupTxn, stringToArrayBuffer("key1"), stringToArrayBuffer("value1"));
            lowlevel.put(setupTxn, stringToArrayBuffer("key2"), stringToArrayBuffer("value2"));
            lowlevel.put(setupTxn, stringToArrayBuffer("key3"), stringToArrayBuffer("value3"));
            lowlevel.put(setupTxn, stringToArrayBuffer("key4"), stringToArrayBuffer("value4"));
            lowlevel.put(setupTxn, stringToArrayBuffer("key5"), stringToArrayBuffer("value5"));
            await lowlevel.commitTransaction(setupTxn);
            
            const txnId = lowlevel.startTransaction();
            
            // Forward iteration with endKey
            let iterId = lowlevel.createIterator(
                txnId,
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
                txnId,
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
            await lowlevel.commitTransaction(txnId);
        });
    });
});
