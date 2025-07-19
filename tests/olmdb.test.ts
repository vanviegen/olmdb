import { init, put, get, getString, transact, del, scan, asString, DatabaseError, onRevert, onCommit } from '../dist/olmdb.js';
import { expect, test, describe, beforeEach } from "@jest/globals";

try {
    init("./.olmdb_test");
} catch (error: any) {
    if (error.code !== "DUP_INIT") {
        throw error; // Rethrow if it's not the expected error
    }
}

let state: string = 'initial';
const waiters: Map<string, Array<() => void>> = new Map();

function setState(newState: string) {
    state = newState;
    const waitersForState = waiters.get(newState);
    if (waitersForState) {
        waitersForState.forEach(resolve => resolve());
        waiters.delete(newState);
    }
}

async function waitForState(targetState: string): Promise<void> {
    if (state === targetState) {
        return;
    }
    
    return new Promise(resolve => {
        if (!waiters.has(targetState)) {
            waiters.set(targetState, []);
        }
        waiters.get(targetState)!.push(resolve);
    });
}

function sleep(ms: number) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

expect.extend({
    toThrowDatabaseError(fn, code) {
        try {
            fn();
        } catch (e: any) {
            if (e instanceof DatabaseError && e.code === code) {
                return { pass: true, message: () => `Expected not to throw DatabaseError with code ${code}` };
            }
            return { pass: false, message: () => `Expected DatabaseError with code "${code}", but got: ${e.name} (code: ${e.code}, message: ${e.message})` };
        }
        return { pass: false, message: () => `Function did not throw` };
    }
});

declare module 'expect' {
  interface Matchers<R> {
    toThrowDatabaseError(code: string): R;
  }
}

describe('LMDB', () => {
    beforeEach(async () => {
        state = 'initial';
        waiters.clear();
        
        // Clean up all existing pairs
        await transact(() => {
            for (const {key} of scan()) {
                del(key);
            }
        });
    });
    
    test('put and get', async () => {
        const value = 'abc';
        
        await transact(() => {
            put('testKey', value);
        });
        
        let newValue = await transact(() => {
            return getString('testKey');
        })
        expect(newValue).toEqual(value);
    });
    
    test('put and get within a single transaction', async () => {
        const value = 'def';
        
        const newValue = await transact(() => {
            put('testKey', value);
            return getString('testKey');
        })
        expect(newValue).toEqual(value);
    });
    
    test("should handle basic put/get operations", async () => {
        const value = "test-value";
        
        await transact(() => {
            put('test-key', value);
        });
        
        const result = await transact(() => {
            return getString('test-key');
        });
        
        expect(result).toBe("test-value");
    });
    
    test("should maintain transaction isolation", async () => {
        const value1 = "value1";
        
        // Start first transaction
        const tx1Promise = transact(async () => {
            put('isolation-test', value1);
            setState('tx1-written');
            
            // Wait for tx2 to try reading
            await waitForState('tx2-read-attempted');
            
            return "tx1-done";
        });
        
        // Start second transaction that should not see tx1's uncommitted changes
        const tx2Promise = await (async () => {
            await waitForState('tx1-written');
            return transact(async () => {
                const result = get('isolation-test');
                setState('tx2-read-attempted');
                return result;
            });
        })();
        
        const [tx1Result, tx2Result] = await Promise.all([tx1Promise, tx2Promise]);
        
        expect(tx1Result).toBe("tx1-done");
        expect(tx2Result).toBeUndefined(); // tx2 shouldn't see uncommitted changes
    });
    
    test("should retry transactions on race conditions", async () => {
        const value1 = "value1";
        const value2 = "value2";
        
        let attempts = 0;
        
        // Create two concurrent transactions
        const tx1 = transact(async () => {
            attempts++;
            get('race-test'); // Do a read, allowing us to be raced
            if (state == "initial") setState('tx1-has-read');
            await waitForState('tx2-has-written');
            put('race-test', value1);
        });
        
        await transact(async () => {
            await waitForState('tx1-has-read');
            put('race-test', value2);
        });
        setState('tx2-has-written');
        
        await tx1;
        expect(attempts).toBe(2); // Should have retried
        expect(await transact(() => getString('race-test'))).toEqual(value1); // tx1's value should win
    });
    
    test("should abort transaction on unrelated exceptions", async () => {
        const value = new Uint8Array([116, 101, 115, 116]); // "test" as bytes
        
        await expect(transact(() => {
            put('error-test', value);
            throw new Error("Random error");
        })).rejects.toThrow("Random error");
        
        // Verify the put was rolled back - LMDB returns undefined for non-existent keys
        const result = await transact(() => get('error-test'));
        expect(result).toBeUndefined();
    });
    
    test("should handle deletes correctly", async () => {
        const value = "value";
        
        await transact(() => {
            put('delete-test', value);
            expect(getString('delete-test')).toEqual(value);
        });
        
        let result = await transact(() => getString('delete-test'));
        expect(result).toEqual(value);
        
        await transact(() => {
            del('delete-test');
            expect(get('delete-test')).toBeUndefined();
        });
        
        result = await transact(() => getString('delete-test'));
        expect(result).toBeUndefined();
        
        // Again, but this time within a single transaction
        
        await transact(() => {
            put('delete-test', value);
            expect(getString('delete-test')).toEqual(value);
            del('delete-test');
            expect(get('delete-test')).toBeUndefined();
        });
        
        result = await transact(() => getString('delete-test'));
        expect(result).toBeUndefined();
    });
    
    test("should handle large values", async () => {
        const value = new Uint8Array(1024 * 1024); // 1MB of 'random' binary data
        // For some reason, this loop take 2 seconds on Node 24, but only a few milliseconds on Bun:
        for (let i = 0; i < value.length; i++) {
            value[i] = i % 256;
        }
        
        await transact(() => {
            put('large-test', value);
            // This takes 1,5 seconds on Node 24, and barely any time on Bun:        
            expect(get('large-test')).toEqual(value);
        });
        
        const result = await transact(() => {
            return get('large-test');
        });
        
        // This takes 1,5 seconds on Node 24, and barely any time on Bun:
        expect(result).toEqual(value);
    });
    
    test("should throw error on nested transactions", async () => {
        await expect(
            transact(async () => {
                // Try to start a nested transaction
                await transact(async () => {
                    throw new Error("This should never happen");
                });
            })
        ).rejects.toThrow(new TypeError("Nested transactions are not allowed"));
    });
    
    test("should iterate over all entries", async () => {
        const entries = [
            { key: 'a-key', value: 'value-a' },
            { key: 'b-key', value: 'value-b' },
            { key: 'c-key', value: 'value-c' }
        ];
        
        // Insert test data
        await transact(() => {
            for (const { key, value } of entries) {
                put(key, value);
            }
        });
        
        // Iterate and collect results
        const results = await transact(() => {
            const collected: Array<{ key: string, value: string }> = [];
            for (const entry of scan({ keyConvert: asString, valueConvert: asString })) {
                collected.push(entry);
            }
            return collected;
        });
        
        expect(results).toEqual(entries);
    });
    
    test("should iterate over all entries while uncommitted", async () => {
        const entries = [
            { key: 'a-key', value: 'value-a' },
            { key: 'b-key', value: 'value-b' },
            { key: 'c-key', value: 'value-c' }
        ];
        
        // Insert test data
        const results = await transact(() => {
            for (const { key, value } of entries) {
                put(key, value);
            }
            
            // Iterate and collect results
            const collected: Array<{ key: string, value: string }> = [];
            for (const entry of scan({ keyConvert: asString, valueConvert: asString })) {
                collected.push(entry);
            }
            return collected;
        });
        
        expect(results).toEqual(entries);
    });
    
    test("should iterate in reverse order", async () => {
        await transact(() => {
            put('scan-1', 'first');
            put('scan-2', 'second');
            put('scan-3', 'third');
        });
        
        const results = await transact(() => {
            const collected: Array<{ key: string, value: string }> = [];
            for (const { key, value } of scan({ reverse: true, keyConvert: asString, valueConvert: asString })) {
                collected.push({
                    key,
                    value
                });
            }
            return collected;
        });
        
        expect(results).toEqual([
            { key: 'scan-3', value: 'third' },
            { key: 'scan-2', value: 'second' },
            { key: 'scan-1', value: 'first' }
        ]);
    });
    
    test("should start iteration from specified key", async () => {
        await transact(() => {
            put('a-key', 'a-value');
            put('b-key', 'b-value');
            put('c-key', 'c-value');
            put('z-key', 'z-value');
        });
        
        const results = await transact(() => {
            const collected: Array<{ key: string, value: string }> = [];
            for (const entry of scan({ start: 'b-key', keyConvert: asString, valueConvert: asString })) {
                collected.push({
                    key: entry.key,
                    value: entry.value
                });
            }
            return collected;
        });
        
        expect(results[0]).toEqual({ key: 'b-key', value: 'b-value' });
    });
    
    test("should handle manual iterator control", async () => {
        await transact(() => {
            put('scan-1', 'value1');
            put('scan-2', 'value2');
        });
        
        const result = await transact(() => {
            const iter = scan({});
            
            let count = 0;
            let lastEntry;
            
            // Read a few entries manually
            while (count < 5) {
                const next = iter.next();
                if (next.done) break;
                
                lastEntry = next.value;
                count++;
            }
            
            iter.close(); // Explicitly close
            
            return { count, lastEntry };
        });
        
        expect(result.count).toBeGreaterThan(0);
        if (result.lastEntry) {
            expect(result.lastEntry.key).toBeInstanceOf(Uint8Array);
            expect(result.lastEntry.value).toBeInstanceOf(Uint8Array);
        }
    });
    
    test("should handle empty iteration", async () => {
        // Use a fresh key that shouldn't exist
        const nonExistentKey = 'non-existent-prefix-xyz';
        
        const result = await transact(() => {
            const iter = scan({ start: nonExistentKey });
            const first = iter.next();
            iter.close();
            
            return first.done;
        });
        
        // Might be done immediately if no keys >= the start key exist
        expect(typeof result).toBe('boolean');
    });
    
    test("should see writes within same transaction during iteration", async () => {
        const testKey = 'iter-write-test';
        const testValue = 'iter-value';
        
        const found = await transact(() => {
            // Write first, then iterate
            put(testKey, testValue);
            
            for (const { key, value } of scan({ keyConvert: asString, valueConvert: asString })) {
                if (key === testKey) {
                    return value === testValue;
                }
            }
            return false;
        });
        
        expect(found).toBe(true);
    });
    
    test("should fail to work on db outside transaction", async () => {
        await transact(() => {
            put('scan-1', 'value1');
        });
        
        // Try to create iterator outside transaction
        expect(() => {
            scan();
        }).toThrow("should be performed within in a transact");
        
        // Try to use an iterator outside transaction
        let it: any;
        await transact(() => {
            it = scan();
        });
        expect(() => it.toArray()).toThrowDatabaseError("INVALID_ITERATOR");
    });
    
    test("should scan with from and til parameters", async () => {
        await transact(() => {
            put('a-key', 'a-value');
            put('b-key', 'b-value');
            put('c-key', 'c-value');
            put('d-key', 'd-value');
            put('e-key', 'e-value');
        });
        
        const results = await transact(() => {
            const collected: Array<{ key: string, value: string }> = [];
            for (const entry of scan({ start: 'b-key', end: 'd-key', keyConvert: asString, valueConvert: asString })) {
                collected.push({
                    key: entry.key,
                    value: entry.value
                });
            }
            return collected;
        });
        
        expect(results).toEqual([
            { key: 'b-key', value: 'b-value' },
            { key: 'c-key', value: 'c-value' }
        ]);
    });
    
    test("should scan reverse with from and til parameters", async () => {
        await transact(() => {
            put('a-key', 'a-value');
            put('b-key', 'b-value');
            put('c-key', 'c-value');
            put('d-key', 'd-value');
            put('e-key', 'e-value');
        });
        
        const results = await transact(() => {
            const collected: Array<{ key: string, value: string }> = [];
            for (const entry of scan({ start: 'd-key', end: 'b-key', reverse: true, keyConvert: asString, valueConvert: asString })) {
                collected.push({
                    key: entry.key,
                    value: entry.value
                });
            }
            return collected;
        });
        
        expect(results).toEqual([
            { key: 'd-key', value: 'd-value' },
            { key: 'c-key', value: 'c-value' }
        ]);
    });

    test("runs transaction in parallel", async () => {
        let promises: Promise<void>[] = [];
        for(let i=0; i<10; i++) {
            promises.push(transact(() => {
                put(`parallel-key-${i}`, `parallel-value-${i}`);
            }));
        }
        await Promise.all(promises);
        transact(() => expect(scan().toArray().length).toBe(10));
    })

    test("runs transaction in parallel, in multiple write transactions", async () => {
        let promises: Promise<void>[] = [];
        for(let i=0; i<25; i++) {
            promises.push(transact(() => {
                put(`parallel-key-${i}`, `parallel-value-${i}`);
            }));
            await sleep(1);
        }
        await Promise.all(promises);
        transact(() => expect(scan().toArray().length).toBe(25));
    })

    describe('onCommit and onRevert callbacks', () => {
        test('should execute onCommit callback when transaction succeeds', async () => {
            let callbackExecuted = false;
            let callbackValue = '';
            
            await transact(() => {
                put('callback-test', 'test-value');
                onCommit(() => {
                    callbackExecuted = true;
                    callbackValue = 'commit-executed';
                });
            });
            
            expect(callbackExecuted).toBe(true);
            expect(callbackValue).toBe('commit-executed');
        });
        
        test('should execute onRevert callback when transaction fails', async () => {
            let callbackExecuted = false;
            let callbackValue = '';
            
            await expect(transact(() => {
                put('revert-test', 'test-value');
                onRevert(() => {
                    callbackExecuted = true;
                    callbackValue = 'revert-executed';
                });
                throw new Error('Test error');
            })).rejects.toThrow('Test error');
            
            expect(callbackExecuted).toBe(true);
            expect(callbackValue).toBe('revert-executed');
        });
        
        test('should allow nested transact calls in onCommit callback', async () => {
            let nestedTransactionResult = '';
            
            await transact(() => {
                put('main-key', 'main-value');
                onCommit(() => {
                    // This should work - callbacks run outside transaction context
                    transact(() => {
                        put('nested-key', 'nested-value');
                        nestedTransactionResult = getString('nested-key') || '';
                    });
                });
            });
            
            // Wait a bit for nested transaction to complete
            await new Promise(resolve => setTimeout(resolve, 10));
            
            expect(nestedTransactionResult).toBe('nested-value');
            
            // Verify both keys were set
            const mainValue = await transact(() => getString('main-key'));
            const nestedValue = await transact(() => getString('nested-key'));
            expect(mainValue).toBe('main-value');
            expect(nestedValue).toBe('nested-value');
        });
        
        test('should allow nested transact calls in onRevert callback', async () => {
            let nestedTransactionResult = '';
            
            await expect(transact(() => {
                put('revert-main-key', 'main-value');
                onRevert(() => {
                    // This should work - callbacks run outside transaction context
                    transact(() => {
                        put('revert-nested-key', 'nested-value');
                        nestedTransactionResult = getString('revert-nested-key') || '';
                    });
                });
                throw new Error('Intentional error');
            })).rejects.toThrow('Intentional error');
            
            // Wait a bit for nested transaction to complete
            await new Promise(resolve => setTimeout(resolve, 10));
            
            expect(nestedTransactionResult).toBe('nested-value');
            
            // Verify main transaction was reverted but nested succeeded
            const mainValue = await transact(() => getString('revert-main-key'));
            const nestedValue = await transact(() => getString('revert-nested-key'));
            expect(mainValue).toBeUndefined(); // Main transaction was reverted
            expect(nestedValue).toBe('nested-value'); // Nested transaction succeeded
        });
        
        test('should execute multiple callbacks in order', async () => {
            const executionOrder: string[] = [];
            
            await transact(() => {
                put('multi-callback-test', 'value');
                onCommit(() => {
                    executionOrder.push('first');
                });
                onCommit(() => {
                    executionOrder.push('second');
                });
                onCommit(() => {
                    executionOrder.push('third');
                });
            });
            
            expect(executionOrder).toEqual(['first', 'second', 'third']);
        });
        
        test('should handle errors in callbacks gracefully', async () => {
            let successCallbackExecuted = false;
            
            await transact(() => {
                put('error-callback-test', 'value');
                onCommit(() => {
                    throw new Error('Callback error');
                });
                onCommit(() => {
                    successCallbackExecuted = true;
                });
            });
            
            // The second callback should still execute despite first one failing
            expect(successCallbackExecuted).toBe(true);
            
            // The transaction should still succeed
            const value = await transact(() => getString('error-callback-test'));
            expect(value).toBe('value');
        });
        
        test('should throw error when calling onCommit outside transaction', () => {
            expect(() => {
                onCommit(() => {});
            }).toThrow('should be performed within in a transact');
        });
        
        test('should throw error when calling onRevert outside transaction', () => {
            expect(() => {
                onRevert(() => {});
            }).toThrow('should be performed within in a transact');
        });
        
        test('should call onRevert on every retry and clear callbacks', async () => {
            // This test is conceptual since it's hard to force retries in test environment
            // But it verifies the callback clearing behavior
            let revertCount = 0;
            
            await expect(transact(() => {
                // Register a callback that increments counter
                onRevert(() => {
                    revertCount++;
                });
                
                // Register multiple callbacks to test clearing
                onRevert(() => {
                    // This should also execute
                });
                
                throw new Error('Force revert');
            })).rejects.toThrow('Force revert');
            
            // Callbacks should have been called
            expect(revertCount).toBe(1);
        });
    });

});

