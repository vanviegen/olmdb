/**
 * Example usage of onRevert() and onCommit() callbacks
 * 
 * This demonstrates the API but won't run due to missing native library.
 * It shows how the callbacks would be used in practice.
 */

import { transact, put, get, getString, onRevert, onCommit } from './dist/olmdb.js';

// Example 1: Basic callback usage
async function example1() {
    console.log('Example 1: Basic callback usage');
    
    await transact(() => {
        put('user:123', JSON.stringify({ name: 'Alice', email: 'alice@example.com' }));
        
        // Register callbacks
        onCommit(() => {
            console.log('✅ User was successfully saved to database');
        });
        
        onRevert(() => {
            console.log('❌ Failed to save user to database');
        });
    });
}

// Example 2: Error handling with callbacks
async function example2() {
    console.log('Example 2: Error handling with callbacks');
    
    try {
        await transact(() => {
            put('test-key', 'test-value');
            
            onRevert(() => {
                console.log('🔄 Cleaning up after transaction failure');
                // Could notify monitoring systems, clean up resources, etc.
            });
            
            // Simulate an error
            throw new Error('Something went wrong');
        });
    } catch (err) {
        console.log('Caught error:', err.message);
    }
}

// Example 3: Nested transactions in callbacks
async function example3() {
    console.log('Example 3: Nested transactions in callbacks');
    
    await transact(() => {
        put('order:456', JSON.stringify({ 
            userId: 123, 
            total: 99.99,
            items: ['item1', 'item2'] 
        }));
        
        onCommit(() => {
            // Update user statistics in a separate transaction
            transact(() => {
                const userStats = get('user:123:stats');
                const stats = userStats ? JSON.parse(new TextDecoder().decode(userStats)) : { orderCount: 0, totalSpent: 0 };
                stats.orderCount++;
                stats.totalSpent += 99.99;
                put('user:123:stats', JSON.stringify(stats));
            });
            
            // Send confirmation email (outside DB)
            console.log('📧 Sending order confirmation email...');
        });
        
        onRevert(() => {
            // Log failed order attempt
            transact(() => {
                put('failed_orders:' + Date.now(), JSON.stringify({
                    userId: 123,
                    timestamp: new Date().toISOString(),
                    reason: 'Transaction failed'
                }));
            });
        });
    });
}

// Example 4: Multiple callbacks
async function example4() {
    console.log('Example 4: Multiple callbacks');
    
    await transact(() => {
        put('config:feature_flag', 'enabled');
        
        // Multiple commit callbacks execute in order
        onCommit(() => {
            console.log('1️⃣ First callback: clearing cache');
        });
        
        onCommit(() => {
            console.log('2️⃣ Second callback: notifying services');
        });
        
        onCommit(() => {
            console.log('3️⃣ Third callback: updating metrics');
        });
    });
}

// Export examples for documentation
export { example1, example2, example3, example4 };

console.log('✅ Callback API examples loaded successfully');
console.log('💡 These examples show proper usage of onRevert() and onCommit() functions');