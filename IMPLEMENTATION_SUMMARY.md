# onRevert() and onCommit() Implementation Summary

## Overview
Added `onRevert(callback)` and `onCommit(callback)` functions to the olmdb transaction system. These functions allow registering callbacks that execute when transactions are reverted (due to errors) or committed successfully.

## Key Features

### ✅ Requirements Met
- **Callback Registration**: Functions add callbacks to lists attached to the current transaction
- **Error Context**: onRevert callbacks execute when transactions throw errors
- **Success Context**: onCommit callbacks execute when transactions commit successfully
- **Non-Transaction Context**: Callbacks run outside transaction context
- **Nested Transactions**: Users can call `transact()` within callbacks
- **Proper Exports**: Functions are exported and available for import

### 🔧 Implementation Details

#### 1. Extended Transaction Interface
```typescript
interface Transaction {
    fn: () => any;
    resolve: (value: any) => void;
    reject: (reason: any) => void;
    id: number;
    result?: any;
    retryCount: number;
    onCommitCallbacks: Array<() => void>;  // ✨ New
    onRevertCallbacks: Array<() => void>;  // ✨ New
}
```

#### 2. Callback Registration Functions
```typescript
export function onRevert(callback: () => void): void {
    const transaction = getTransaction();
    transaction.onRevertCallbacks.push(callback);
}

export function onCommit(callback: () => void): void {
    const transaction = getTransaction();
    transaction.onCommitCallbacks.push(callback);
}
```

#### 3. Callback Execution Points

**onRevert callbacks execute when:**
- Transaction function throws an error
- Transaction fails after maximum retries (racing)

**onCommit callbacks execute when:**
- Read-only transactions complete immediately
- Write transactions commit successfully (async)

#### 4. Error Handling
- Callback errors are caught and logged as warnings
- Callback failures don't affect transaction outcome
- Multiple callbacks execute in registration order

## Usage Examples

### Basic Success Callback
```javascript
await transact(() => {
    put('user:123', userData);
    onCommit(() => {
        console.log('User saved successfully!');
    });
});
```

### Error Handling
```javascript
try {
    await transact(() => {
        put('data', value);
        onRevert(() => {
            console.log('Transaction failed, cleaning up...');
        });
        throw new Error('Something went wrong');
    });
} catch (err) {
    // onRevert callback already executed
}
```

### Nested Transactions
```javascript
await transact(() => {
    put('order', orderData);
    onCommit(() => {
        // This works - callbacks run outside transaction context
        transact(() => {
            put('audit_log', logEntry);
        });
    });
});
```

## Files Modified
- **src/olmdb.ts**: Core implementation
- **tests/olmdb.test.ts**: Comprehensive test suite
- **dist/**: Compiled JavaScript and TypeScript declarations

## Testing
Comprehensive test suite covers:
- ✅ Basic callback execution
- ✅ Error scenarios
- ✅ Nested transactions in callbacks
- ✅ Multiple callbacks
- ✅ Error handling in callbacks
- ✅ Outside-transaction-context validation

## API Compatibility
- ✅ Fully backward compatible
- ✅ No breaking changes to existing API
- ✅ Minimal code changes
- ✅ Proper TypeScript definitions