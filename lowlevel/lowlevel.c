#include "lmdb.h"
#include <node_api.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <sys/stat.h>
#include <errno.h>
#include <stdatomic.h>
#include <time.h>
#include <stddef.h>
#include <assert.h>

// Logging and error macros
#define LOG_INTERNAL_ERROR(message, ...) \
    fprintf(stderr, "OLMDB: %s:%d " message "\n", __FILE__, __LINE__, ##__VA_ARGS__)

#define ASSERT(condition) \
    do { \
        if (!(condition)) { \
            LOG_INTERNAL_ERROR("Assertion failed: " #condition); \
        } \
    } while(0)

#define ASSERT_OR_RETURN(condition, returnValue) \
    do { \
        if (!(condition)) { \
            LOG_INTERNAL_ERROR("Assertion failed: " #condition); \
            return returnValue; \
        } \
    } while(0)

// Error throwing macros
#define THROW_DB_ERROR(env, returnValue, code, message, ...) \
    do { \
        char _error_msg[512]; \
        snprintf(_error_msg, sizeof(_error_msg), message, ##__VA_ARGS__); \
        throw_db_error(env, code, _error_msg); \
        return (returnValue); \
    } while(0)

#define THROW_TYPE_ERROR(env, returnValue, code, message, ...) \
    do { \
        char _error_msg[512]; \
        snprintf(_error_msg, sizeof(_error_msg), message, ##__VA_ARGS__); \
        napi_throw_type_error(env, code, _error_msg); \
        return (returnValue); \
    } while(0)

#define THROW_LMDB_ERROR(env, returnValue, error_code) \
    do { \
        char _code[16]; \
        snprintf(_code, sizeof(_code), "LMDB%d", error_code); \
        throw_db_error(env, _code, mdb_strerror(error_code)); \
        return (returnValue); \
    } while(0)

// DatabaseError class reference
static napi_ref database_error_constructor_ref = NULL;

// Helper to create DatabaseError instances
static napi_value throw_db_error(napi_env env, const char *code, const char *message) {
    if (!database_error_constructor_ref) {
        LOG_INTERNAL_ERROR("DatabaseError constructor not initialized");
        napi_throw_error(env, code, message);
        return NULL;
    }
    
    napi_value constructor;
    if (napi_get_reference_value(env, database_error_constructor_ref, &constructor) != napi_ok) {
        LOG_INTERNAL_ERROR("Failed to get DatabaseError constructor reference");
        napi_throw_error(env, code, message);
        return NULL;
    }
    
    napi_value message_val, code_val;
    if (napi_create_string_utf8(env, message, NAPI_AUTO_LENGTH, &message_val) != napi_ok ||
        napi_create_string_utf8(env, code, NAPI_AUTO_LENGTH, &code_val) != napi_ok) {
        LOG_INTERNAL_ERROR("Failed to create string values for DatabaseError");
        napi_throw_error(env, code, message);
        return NULL;
    }
    
    napi_value argv[] = { message_val, code_val };
    napi_value error_instance;
    if (napi_new_instance(env, constructor, 2, argv, &error_instance) != napi_ok) {
        LOG_INTERNAL_ERROR("Failed to create DatabaseError instance");
        napi_throw_error(env, code, message);
        return NULL;
    }
    
    napi_throw(env, error_instance);
    return NULL;
}

// Get the pointer to a struct from a pointer to one of its members
#define container_of(ptr, type, member) ((type *)((char *)(ptr) - offsetof(type, member)))

// Configuration constants
#define MAX_TRANSACTIONS 8192
#define MAX_ITERATORS 16384
#define DEFAULT_LOG_BUFFER_SIZE (32 * 1024 - sizeof(log_buffer_t))
#define MAX_KEY_LENGTH 511
#define SKIPLIST_DEPTH 4

// For use by our checksum hash function (simple FNV-1a)
#define CHECKSUM_INITIAL 0xcbf29ce484222325ULL
#define CHECKSUM_PRIME 0x100000001b3ULL

typedef struct {
    union read_log_struct *tagged_next_ptr; // tagged as 0 in the lowest 3 bits
    uint64_t checksum;
    uint16_t key_size;
    char key_data[];  // Variable length
} get_log_t;

typedef struct {
    union read_log_struct *tagged_next_ptr; // bit 0 is 1. bit 1 is 0 for forwards, 1 for backwards.
    uint64_t checksum;
    uint32_t row_count; // Number of additional rows (excluding the initial one) that this iterator has read, and have been included in checksum
    uint16_t key_size;
    char key_data[];  // Variable length
} iterate_log_t;

// Union containing all entry types
typedef union read_log_struct {
    // Base structure for all entries
    union read_log_struct *tagged_next_ptr; // ignore the lowest 3 bits
    uintptr_t tag;  // Type info in the lowest 3 bits
    get_log_t get;
    iterate_log_t iterate;
} __attribute__((aligned(8))) read_log_t;

typedef struct update_log_struct {
    struct update_log_struct *next_ptrs[SKIPLIST_DEPTH];
    struct update_log_struct *prev_ptr;
    uint32_t value_size;
    uint16_t key_size;
    char data[]; // key data + value data
} __attribute__((aligned(8))) update_log_t;

// Buffer structure using flexible array member
typedef struct log_buffer_struct {
    struct log_buffer_struct *next;
    uint32_t size;
    char data[];  // flexible array member
} log_buffer_t;

#define TRANSACTION_FREE 0
#define TRANSACTION_OPEN 1
#define TRANSACTION_COMMITTING 2
#define TRANSACTION_RACED 3
#define TRANSACTION_SUCCEEDED 4
#define TRANSACTION_FAILED 5

// Transaction structure
typedef struct transaction_struct {
    uint16_t nonce;
    uint8_t state; // TRANSACTION_*
    uint8_t has_writes;
    MDB_txn *read_txn;
    // Buffer for storing commands (first/main buffer)
    log_buffer_t *first_log_buffer;
    log_buffer_t *last_log_buffer;  // Points to buffer being written to
    char *log_end_ptr; // Next write pos within current_buffer
    read_log_t *first_read_log; // Chronologically first read entry
    read_log_t *last_read_log; // Chronologically last read entry
    update_log_t *update_log_skiplist_ptrs[SKIPLIST_DEPTH]; // Skiplist for write entries, 4 pointers for 4 levels
    
    // Queue management for worker thread and free list
    struct transaction_struct *next;
    struct iterator_struct *first_iterator; // Linked list of iterators for this transaction
    
    // Callback for async completion
    napi_ref callback_ref;
    napi_env env;
} transaction_t;

typedef struct iterator_struct {
    struct iterator_struct *next; // Next iterator in the free list, or within the transaction
    MDB_cursor *lmdb_cursor;
    MDB_val lmdb_key; // when mv_data is NULL, it means the cursor is at the end of the database
    MDB_val lmdb_value;
    update_log_t *current_update_log; // Current position within uncommitted records
    iterate_log_t *iterate_log; // Log entry about this iterator
    const char *end_key_data; // If set, the cursor will stop at this key
    int transaction_id; // Set to -1 when free
    uint16_t end_key_size;
    uint16_t nonce;
} iterator_t;

// Global state
// LMDB environment
static MDB_env *dbenv = NULL;
static MDB_dbi dbi;

// Transactions
static transaction_t transactions[MAX_TRANSACTIONS];
static transaction_t *first_free_transaction = NULL; // Start of linked-list of free transactions
static int next_unused_transaction = 0; // Index of next never-used transaction
static transaction_t *first_queued_transaction = NULL; // Queue of transactions waiting for processing
static transaction_t *last_queued_transaction = NULL;

// Log buffers
static log_buffer_t *first_free_log_buffer = NULL; // Start of linked-list of free log buffers of DEFAULT_LOG_BUFFER_SIZE

// Iterators
static iterator_t iterators[MAX_ITERATORS];
static iterator_t *first_free_iterator = NULL; // Start of linked-list of free iterators
static int next_unused_iterator = 0; // Index of next never-used iterator

// Worker
int worker_running = 0; // Flag to indicate if a worker is currently processing transactions
napi_async_work worker_work;

// Random number generator
uint64_t rng_state = 0;

// Prototypes for internal functions
static uint64_t checksum(const char *data, size_t len, uint64_t initial_checksum);
static void *allocate_log_space(transaction_t *txn, size_t needed_space);
static void start_worker();
static void on_worker_ready(napi_env env, napi_status status, void *data);
static void process_write_commits(napi_env env, void *transactions);
static int validate_reads(MDB_txn *write_txn, transaction_t *txn, MDB_cursor *validation_cursor);
static void reset_transaction(transaction_t *txn);
static transaction_t *claim_available_transaction(void);
static transaction_t *id_to_open_transaction(napi_env env, int transaction_id);
static int transaction_to_id(transaction_t *txn);
static update_log_t *find_update_log(transaction_t *txn, uint16_t key_size, const char *key_data, int allow_before);
static void delete_update_log(transaction_t *txn, update_log_t *log);
static int place_cursor(MDB_cursor *cursor, MDB_val *key, MDB_val *value, int reverse);
static uint32_t get_random_number();
int open_lmdb(napi_env env, const char *db_dir);

static inline int max(int a, int b) { return a > b ? a : b; }
static inline int min(int a, int b) { return a < b ? a : b; }

static inline int get_ptr_tag(void *ptr) {
    return (uintptr_t)ptr & 7; // Get the lower 3 bits
}

static inline void *strip_ptr_tag(void *ptr) {
    return (void *)((uintptr_t)ptr & ~7); // Clear the lower 3 bits
}

static inline void *make_tag_ptr(void *ptr, int tag) {
    ASSERT(((uintptr_t)ptr & 7) == 0); // Ensure ptr is aligned to 8 bytes
    return (void *)((uintptr_t)ptr | (tag & 7)); // Set the lower 3 bits to tag
}

static inline int compare_keys(uint16_t key1_size, const char *key1, uint16_t key2_size, const char *key2) {
    int res = memcmp(key1, key2, min(key1_size, key2_size));
    if (res == 0) res = (int)key1_size - (int)key2_size;
    return res;
}

__attribute__((constructor))
static void olmdb_init(void) {
}

static uint32_t get_random_number() {
    if (rng_state == 0) rng_state = (uint64_t)time(NULL);
    rng_state ^= rng_state << 13;
    rng_state ^= rng_state >> 7;
    rng_state ^= rng_state << 17;
    return (uint32_t)rng_state;
}

// A simple and very fast hash function for checksums
static uint64_t checksum(const char *data, size_t len, uint64_t val) {
    val ^= len;
    val *= CHECKSUM_PRIME;
    for (size_t i = 0; i < len; i++) {
        val ^= (uint8_t)data[i];
        val *= CHECKSUM_PRIME;
    }
    return val;
}

// Ensure buffer has enough space, growing if necessary
static void *allocate_log_space(transaction_t *txn, size_t needed_space) {
    // Align buffer position to 8 bytes
    txn->log_end_ptr = (void *)(((uintptr_t)txn->log_end_ptr + 7) & ~7);
    
    if (!txn->last_log_buffer || txn->log_end_ptr + needed_space > txn->last_log_buffer->data + txn->last_log_buffer->size) {
        size_t new_size = max(DEFAULT_LOG_BUFFER_SIZE, needed_space + 7 /* for alignment */);
        
        log_buffer_t *new_buf;
        if (new_size == DEFAULT_LOG_BUFFER_SIZE && first_free_log_buffer) {
            new_buf = first_free_log_buffer;
            first_free_log_buffer = first_free_log_buffer->next;
        } else {
            // Allocate new buffer with data area included
            size_t alloc_size = sizeof(log_buffer_t) + new_size;
            new_buf = malloc(alloc_size);
            if (!new_buf) {
                LOG_INTERNAL_ERROR("Failed to allocate transaction buffer of size %zu", alloc_size);
                return NULL;
            }
            new_buf->size = new_size;
        }
        
        new_buf->next = NULL;
        if (txn->last_log_buffer) {
            txn->last_log_buffer->next = new_buf;
        } else {
            txn->first_log_buffer = new_buf;
        }
        txn->last_log_buffer = new_buf;
        // Align buffer position to 8 bytes
        txn->log_end_ptr = (void *)(((uintptr_t)new_buf->data + 7) & ~7);
    }
    
    void *result = txn->log_end_ptr;
    txn->log_end_ptr += needed_space;
    return result;
}

static read_log_t *create_read_log(transaction_t *txn, size_t size, uint8_t tag) {    
    read_log_t *read_log = allocate_log_space(txn, size);
    ASSERT_OR_RETURN(read_log != NULL, NULL);
    ASSERT(get_ptr_tag(read_log) == 0);
    
    // Initialize the read log entry
    read_log->tagged_next_ptr = make_tag_ptr(NULL, tag);
    
    if (txn->last_read_log) {
        // Keep tag on previous log's next pointer
        txn->last_read_log->tagged_next_ptr = make_tag_ptr(read_log, get_ptr_tag(txn->last_read_log->tagged_next_ptr));
    } else {
        txn->first_read_log = read_log;
    }
    txn->last_read_log = read_log;
    return read_log;
}

// Reset transaction state and put it back into the free list
static void reset_transaction(transaction_t *txn) {        
    // Free all except the first log buffer (which is always DEFAULT_LOG_BUFFER_SIZE bytes)
    log_buffer_t *lb = txn->first_log_buffer;
    txn->first_log_buffer = NULL;
    while(lb) {
        log_buffer_t *next = lb->next;
        if (lb->size == DEFAULT_LOG_BUFFER_SIZE) {
            // Default size, put in free list
            lb->next = first_free_log_buffer;
            first_free_log_buffer = lb;
        } else {
            // Custom size, free it
            free(lb);
        }
        lb = next;
    }
    
    // Reset transaction state
    txn->first_read_log = NULL;
    txn->last_read_log = NULL;
    for (int i = 0; i < SKIPLIST_DEPTH; i++) {
        txn->update_log_skiplist_ptrs[i] = NULL;
    }
    
    txn->has_writes = 0;
    txn->next = NULL;
    
    // Clean up callback reference
    if (txn->callback_ref) {
        napi_status status = napi_delete_reference(txn->env, txn->callback_ref);
        if (status != napi_ok) {
            LOG_INTERNAL_ERROR("Failed to delete callback reference for transaction %d", transaction_to_id(txn));
        }
        txn->callback_ref = NULL;
    }
    txn->env = NULL;
    
    // Move iterators to free list
    iterator_t *it = txn->first_iterator;
    while(it) {
        it->transaction_id = -1;
        iterator_t *next = it->next;
        it->next = NULL;
        it = next;
    }
    txn->first_iterator = NULL;
    
    // Return transaction to free list
    txn->state = TRANSACTION_FREE;
    txn->next = first_free_transaction;
    first_free_transaction = txn;
}

static transaction_t *id_to_open_transaction(napi_env env, int transaction_id) {
    int idx = transaction_id >> 16;
    uint16_t nonce = transaction_id & 0xffff;
    
    if (idx >= MAX_TRANSACTIONS) {
        THROW_DB_ERROR(env, NULL, "INVALID_TRANSACTION", "Transaction index %d exceeds maximum %d", idx, MAX_TRANSACTIONS - 1);
    }
    
    transaction_t *txn = &transactions[idx];
    if (txn->state != TRANSACTION_OPEN || txn->nonce != nonce) {
        THROW_DB_ERROR(env, NULL, "INVALID_TRANSACTION", "Transaction ID %d not found or already closed (index=%d, nonce=%u, state=%d, expected_nonce=%u)", 
                      transaction_id, idx, nonce, txn->state, txn->nonce);
    }
    
    return txn;
}

static int transaction_to_id(transaction_t *txn) {
    int idx = txn - transactions;
    ASSERT(idx >= 0 && idx < MAX_TRANSACTIONS);
    return (idx << 16) | txn->nonce;
}

// Find an available transaction slot
static transaction_t *claim_available_transaction(void) {
    transaction_t *txn;
    if (first_free_transaction) {
        txn = first_free_transaction;
        first_free_transaction = txn->next;
    } else if (next_unused_transaction < MAX_TRANSACTIONS) {
        txn = &transactions[next_unused_transaction++];
    } else {
        return NULL;
    }
    txn->nonce = get_random_number() & 0xffff; // Generate a random nonce
    txn->state = TRANSACTION_OPEN;
    return txn;
}

static iterator_t *id_to_open_iterator(napi_env env, int iterator_id) {
    int idx = iterator_id >> 16;
    uint16_t nonce = iterator_id & 0xffff;
    
    if (idx >= MAX_ITERATORS) {
        THROW_DB_ERROR(env, NULL, "INVALID_ITERATOR", "Iterator index %d exceeds maximum %d", idx, MAX_ITERATORS - 1);
    }
    
    iterator_t *iterator = &iterators[idx];
    if (iterator->transaction_id < 0 || iterator->nonce != nonce) {
        THROW_DB_ERROR(env, NULL, "INVALID_ITERATOR", "Iterator ID %d not found or already closed (index=%d, nonce=%u, txn_id=%d, expected_nonce=%u)", 
                      iterator_id, idx, nonce, iterator->transaction_id, iterator->nonce);
    }
    
    return iterator;
}

static int iterator_to_id(iterator_t *iterator) {
    int idx = iterator - iterators;
    ASSERT(idx >= 0 && idx < MAX_ITERATORS);
    return (idx << 16) | iterator->nonce;
}

static iterator_t *claim_available_iterator(transaction_t *txn) {
    iterator_t *iterator;
    if (first_free_iterator) {
        iterator = first_free_iterator;
        ASSERT(iterator->transaction_id < 0); // Should be free
        first_free_iterator = iterator->next;
        int rc = mdb_cursor_renew(txn->read_txn, iterator->lmdb_cursor);
        ASSERT_OR_RETURN(rc == MDB_SUCCESS, NULL);
    } else if (next_unused_iterator < MAX_ITERATORS) {
        iterator = &iterators[next_unused_iterator++];
        int rc = mdb_cursor_open(txn->read_txn, dbi, &iterator->lmdb_cursor);
        ASSERT_OR_RETURN(rc == MDB_SUCCESS, NULL);
    } else {
        LOG_INTERNAL_ERROR("No free iterator slots available (max: %d)", MAX_ITERATORS);
        return NULL;
    }
    iterator->transaction_id = transaction_to_id(txn);
    iterator->nonce = get_random_number() & 0xffff; // Generate a random nonce
    return iterator;
}

// if_not_found: 0=returning nothing 1=return next 2=return previous
static update_log_t *find_update_log(transaction_t *txn, uint16_t key_size, const char *key_data, int if_not_found) {
    int level = SKIPLIST_DEPTH - 1;
    update_log_t **next_ptrs = txn->update_log_skiplist_ptrs;
    
    while(1) {
        update_log_t *next = next_ptrs[level];
        if (next) {
            int cmp = compare_keys(next->key_size, next->data, key_size, key_data);
            if (!cmp) return next;
            if (cmp < 0) {
                next_ptrs = next->next_ptrs;
                continue;
            }
        }
        // No more entries at this depth, go to next depth
        if (level <= 0) break;
        level--;
    }
    
    // No exact match
    if (if_not_found == 1) {
        // Return the first entry that is greater than key
        return next_ptrs[0];
    } else if (if_not_found == 2) {
        // Return the last entry that is less than key
        // This cast happens to be right, as the next_ptrs are at the start of the record_t
        return next_ptrs==txn->update_log_skiplist_ptrs ? NULL : container_of(next_ptrs, update_log_t, next_ptrs);
    }
    return NULL;
}

static update_log_t *find_last_update_log(transaction_t *txn) {
    int level = SKIPLIST_DEPTH - 1;
    update_log_t **next_ptrs = txn->update_log_skiplist_ptrs;
    
    while(1) {
        update_log_t *next = next_ptrs[level];
        if (next) {
            next_ptrs = next->next_ptrs;
        } else {
            // No more entries at this depth, go to next depth
            if (level <= 0) break;
            level--;
        }
    }
    
    if (next_ptrs == txn->update_log_skiplist_ptrs) return NULL; // No entries at all
    // This cast happens to be right, as the next_ptrs are at the start of the record_t
    return (update_log_t *)next_ptrs;
}

static void add_update_log(transaction_t *txn, uint16_t key_size, const char *key_data, uint32_t value_size, const char *value_data) {
    int size = sizeof(update_log_t) + (int)key_size + value_size;
    update_log_t *new_log = allocate_log_space(txn, size);
    if (!new_log) return;
    
    new_log->key_size = key_size;
    new_log->value_size = value_size;
    memcpy(new_log->data, key_data, key_size);
    if (value_size > 0) memcpy(new_log->data + key_size, value_data, value_size);
    
    // Create a random skiplist insert level, where each higher level has a 75% chance of being skipped
    uint32_t rnd = get_random_number();
    int insert_level = 0;
    while ((rnd & 0x3)==0 && insert_level < SKIPLIST_DEPTH - 1) {
        insert_level++;
        rnd >>= 2;
    }
    
    // Find insertion point, and set forward references on new_log and its predecessors on the various levels
    int level = SKIPLIST_DEPTH - 1;
    update_log_t **current_ptrs = txn->update_log_skiplist_ptrs;
    while (level >= 0) {
        update_log_t *next = current_ptrs[level];
        if (next && compare_keys(next->key_size, next->data, key_size, key_data) < 0) {
            current_ptrs = next->next_ptrs;
        } else {
            if (level <= insert_level) {
                new_log->next_ptrs[level] = current_ptrs[level];
                current_ptrs[level] = new_log;
            }
            level--;
        }
    }
    
    // current_ptrs now points at the ->next_ptrs of the item right before new_log (at level 0), or still
    // at txn->update_log_skiplist_ptrs, if no predecessors were found
    // Set prev_ptr for reverse iteration
    new_log->prev_ptr = current_ptrs == txn->update_log_skiplist_ptrs ? NULL : container_of(current_ptrs, update_log_t, next_ptrs);
    
    // Update next record's prev_ptr if it exists
    if (new_log->next_ptrs[0]) {
        new_log->next_ptrs[0]->prev_ptr = new_log;
    }
    
    // Clear unused skiplist pointers
    for (int i = insert_level + 1; i < SKIPLIST_DEPTH; i++) {
        new_log->next_ptrs[i] = NULL;
    }
    
    if (new_log->next_ptrs[0] && compare_keys(new_log->next_ptrs[0]->key_size, new_log->next_ptrs[0]->data, key_size, key_data) == 0) {
        // If we found an exact match, we need to delete the pre-existing log entry.
        // (We can't just replace the old item, as the value length may have increased.)
        delete_update_log(txn, new_log->next_ptrs[0]);
    }
}

static void delete_update_log(transaction_t *txn, update_log_t *log) {
    // Find all predecessors that point to this log entry across all levels
    int level = SKIPLIST_DEPTH - 1;
    update_log_t **current_ptrs = txn->update_log_skiplist_ptrs;
    
    while (level >= 0) {
        update_log_t *next = current_ptrs[level];
        if (next == log) {
            // Found a pointer to the log entry at this level, update it
            current_ptrs[level] = log->next_ptrs[level];
            level--;
        }
        else if (next && compare_keys(next->key_size, next->data, log->key_size, log->data) <= 0) {
            // Move forward at this level
            current_ptrs = next->next_ptrs;
        } else {
            // Go to next level down
            level--;
        }
    }
    
    // Update the prev_ptr of the next node (if it exists)
    if (log->next_ptrs[0]) {
        log->next_ptrs[0]->prev_ptr = log->prev_ptr;
    }
    
    // Optional: Clear the deleted node's pointers (for debugging/safety)
    log->prev_ptr = NULL;
    for (int i = 0; i < SKIPLIST_DEPTH; i++) {
        log->next_ptrs[i] = NULL;
    }
}

static int place_cursor(MDB_cursor *cursor, MDB_val *key, MDB_val *value, int reverse) {
    MDB_val in_key;
    memcpy(&in_key, key, sizeof(MDB_val));
    
    int cursor_mode = in_key.mv_size > 0 ? MDB_SET_RANGE : (reverse ? MDB_LAST : MDB_FIRST);
    int rc = mdb_cursor_get(cursor, key, value, cursor_mode);
    
    if (in_key.mv_size > 0 && reverse) {
        if (rc == MDB_NOTFOUND) {
            // If no next item was found, then the key we're looking for is the last item in the database
            rc = mdb_cursor_get(cursor, key, value, MDB_LAST);
        } else if (rc == 0 && compare_keys(key->mv_size, key->mv_data, in_key.mv_size, in_key.mv_data) != 0) {
            // If the key is not exactly matched, we need to move the cursor to the item that came *before*
            rc = mdb_cursor_get(cursor, key, value, MDB_PREV);            
        }
    }
    
    if (rc == MDB_NOTFOUND) {
        key->mv_data = NULL; // mark cursor as at the end of the database
        key->mv_size = 0;
        value->mv_data = NULL;
        value->mv_size = 0; 
        rc = 0; // This is not an error, just means no more items to read
    }
    return rc;
}

// Start worker for async processing
static void start_worker() {
    if (worker_running) return; // Already running
    transaction_t *transactions = first_queued_transaction;
    if (!transactions) return; // Nothing to process
    first_queued_transaction = last_queued_transaction = NULL;
    
    worker_running = 1;
    napi_value resource_name;
    ASSERT_OR_RETURN(napi_create_string_utf8(transactions->env, "commit_work", NAPI_AUTO_LENGTH, &resource_name) == napi_ok,);
    
    napi_status status = napi_create_async_work(
        transactions->env, NULL, resource_name,
        process_write_commits, on_worker_ready,
        transactions, &worker_work
    );
    
    if (status != napi_ok) {
        LOG_INTERNAL_ERROR("Failed to create async work");
        worker_running = 0;
        return;
    }
    
    status = napi_queue_async_work(transactions->env, worker_work);
    if (status != napi_ok) {
        LOG_INTERNAL_ERROR("Failed to queue async work");
        napi_delete_async_work(transactions->env, worker_work);
        worker_running = 0;
    }
}

// Worker completion function (main thread callback)
static void on_worker_ready(napi_env env, napi_status status, void *data) {
    (void)status; // Unused parameter
    transaction_t *transactions = (transaction_t *)data;
    
    // Call callbacks for each completed transaction
    transaction_t *txn = transactions;
    while (txn) {
        transaction_t *next = txn->next;
        
        if (txn->callback_ref) {
            napi_value callback;
            if (napi_get_reference_value(env, txn->callback_ref, &callback) == napi_ok) {
                napi_value global;
                if (napi_get_global(env, &global) != napi_ok) {
                    LOG_INTERNAL_ERROR("Failed to get global object for callback");
                } else {
                    napi_value txnId;
                    napi_value result;
                    
                    if (napi_create_int32(env, transaction_to_id(txn), &txnId) != napi_ok ||
                        napi_create_int32(env, txn->state, &result) != napi_ok) {
                        LOG_INTERNAL_ERROR("Failed to create callback arguments for transaction %d", transaction_to_id(txn));
                    } else {
                        napi_value argv[] = { txnId, result };
                        napi_value callback_result;
                        if (napi_call_function(env, global, callback, 2, argv, &callback_result) != napi_ok) {
                            LOG_INTERNAL_ERROR("Failed to call callback for transaction %d", transaction_to_id(txn));
                        }
                    }
                }
            } else {
                LOG_INTERNAL_ERROR("Could not obtain callback value for transaction %d", transaction_to_id(txn));
            }
        }
        
        reset_transaction(txn);
        txn = next;
    }
    
    // Clean up async work
    if (napi_delete_async_work(env, worker_work) != napi_ok) {
        LOG_INTERNAL_ERROR("Failed to delete async work");
    }
    worker_running = 0;
    
    // Start new worker if there are queued transactions
    start_worker();
}

// Process all write commands in a single LMDB transaction
static void process_write_commits(napi_env env, void *data) {
    (void)env; // Unused parameter
    transaction_t *transactions = (transaction_t *)data;
    
    MDB_txn *write_txn = NULL;
    int rc = mdb_txn_begin(dbenv, NULL, 0, &write_txn);
    if (rc != MDB_SUCCESS) {
        LOG_INTERNAL_ERROR("Failed to begin write transaction: %s", mdb_strerror(rc));
        // Mark all transactions as failed
        for(transaction_t *trx = transactions; trx; trx = trx->next) {
            trx->state = TRANSACTION_FAILED;
        }
        return;
    }
    
    // Cursors within read/write transactions will be closed automatically
    // when the transaction ends
    int failed = 0;
    MDB_cursor *validation_cursor;
    rc = mdb_cursor_open(write_txn, dbi, &validation_cursor);
    if (rc != MDB_SUCCESS) {
        LOG_INTERNAL_ERROR("Failed to open cursor for validation: %s", mdb_strerror(rc));
        failed = 1;
    }
    
    for(transaction_t *trx = transactions; trx && !failed; trx = trx->next) {
        // Validate all reads for this transaction
        trx->state = validate_reads(write_txn, trx, validation_cursor);
        
        if (trx->state == TRANSACTION_RACED) continue; // Skip processing if transaction has been raced
        if (trx->state == TRANSACTION_FAILED) break; // Stop processing further transactions
        
        // Process all write entries in skiplist order
        update_log_t *update = trx->update_log_skiplist_ptrs[0];
        
        while (update && !failed) {
            MDB_val key;
            key.mv_data = update->data;
            key.mv_size = update->key_size;
            if (update->value_size == 0) {
                // Delete operation
                rc = mdb_del(write_txn, dbi, &key, NULL);
                if (rc != MDB_SUCCESS && rc != MDB_NOTFOUND) {
                    LOG_INTERNAL_ERROR("Failed to delete key %.*s: %s", (int)update->key_size, update->data, mdb_strerror(rc));
                    failed = 1;
                }
            } else {
                // Put operation
                MDB_val value;
                value.mv_data = update->data + update->key_size;
                value.mv_size = update->value_size;
                
                rc = mdb_put(write_txn, dbi, &key, &value, 0);
                if (rc != MDB_SUCCESS) {
                    LOG_INTERNAL_ERROR("Failed to put key %.*s: %s", (int)update->key_size, update->data, mdb_strerror(rc));
                    failed = 1;
                }
            }
            update = update->next_ptrs[0];
        }
    }
    
    if (failed) {
        // If any transaction/update failed, we need to abort all
        mdb_txn_abort(write_txn);
        // Mark remaining transactions as failed
        for(transaction_t *trx = transactions; trx; trx = trx->next) {
            if (trx->state == TRANSACTION_COMMITTING) {
                trx->state = TRANSACTION_FAILED;
            }
        }
    } else {
        rc = mdb_txn_commit(write_txn);
        if (rc != MDB_SUCCESS) {
            LOG_INTERNAL_ERROR("Failed to commit write transaction: %s", mdb_strerror(rc));
            for(transaction_t *trx = transactions; trx; trx = trx->next) {
                if (trx->state == TRANSACTION_COMMITTING) {
                    trx->state = TRANSACTION_FAILED;
                }
            }
        }
    }
}

// Validate that all read values haven't changed: returns TRANSACTION_*
static int validate_reads(MDB_txn *write_txn, transaction_t *txn, MDB_cursor *validation_cursor) {
    read_log_t *current = txn->first_read_log;
    MDB_val key, value;
    
    while (current) {
        if (current->tag & 1) { // Iterator
            int reverse = current->tag & 2;
            
            key.mv_size = current->iterate.key_size;
            key.mv_data = current->iterate.key_data;
            int rc = place_cursor(validation_cursor, &key, &value, reverse);
            if (rc != MDB_SUCCESS) {
                LOG_INTERNAL_ERROR("Failed to place cursor for iterator validation: %s", mdb_strerror(rc));
                return TRANSACTION_FAILED;
            }
            
            uint64_t cs = CHECKSUM_INITIAL;
            cs = checksum(key.mv_data, key.mv_size, cs);
            cs = checksum(value.mv_data, value.mv_size, cs);
            
            for(int row_count = current->iterate.row_count; row_count > 0; row_count--) {
                // Read the next item in the LMDB cursor
                int rc = mdb_cursor_get(validation_cursor, &key, &value, reverse ? MDB_PREV : MDB_NEXT);
                if (rc == MDB_NOTFOUND) {
                    key.mv_data = value.mv_data = NULL;
                    key.mv_size = value.mv_size = 0;
                } else if (rc != MDB_SUCCESS) {
                    LOG_INTERNAL_ERROR("Failed to read next item in cursor validation: %s", mdb_strerror(rc));
                    return TRANSACTION_FAILED;
                }
                cs = checksum(key.mv_data, key.mv_size, cs);
                cs = checksum(value.mv_data, value.mv_size, cs);
            }
            
            if (cs != current->iterate.checksum) {
                return TRANSACTION_RACED;
            }
        } else { // Regular read
            key.mv_data = current->get.key_data;
            key.mv_size = current->get.key_size;
            int rc = mdb_get(write_txn, dbi, &key, &value);
            uint64_t cs = CHECKSUM_INITIAL;
            if (rc == MDB_NOTFOUND) {
                cs = 0;
            } else if (rc == MDB_SUCCESS) {
                cs = checksum((char *)value.mv_data, value.mv_size, cs);
            } else {
                LOG_INTERNAL_ERROR("Failed to read key %.*s during validation: %s", (int)key.mv_size, (char *)key.mv_data, mdb_strerror(rc));
                return TRANSACTION_FAILED;
            }
            if (cs != current->get.checksum) {
                return TRANSACTION_RACED;
            }
        }
        current = strip_ptr_tag(current->tagged_next_ptr);
    }
    
    return TRANSACTION_SUCCEEDED; // All reads validated successfully
}

__attribute__((destructor)) 
static void cleanup_olmdb() {
    // Cleanup on exit - worker threads will be cleaned up automatically by Node.js
}

// Returns 1 on success, 0 on failure (error already thrown)
int open_lmdb(napi_env env, const char *db_dir) {
    if (dbenv) {
        THROW_DB_ERROR(env, 0, "ALREADY_OPEN", "Database is already open");
    }
    
    if (!db_dir || !db_dir[0]) {
        // Read $OLMDB_DIR environment variable or use default './.olmdb'
        db_dir = getenv("OLMDB_DIR");
        if (!db_dir || !db_dir[0]) {
            db_dir = "./.olmdb"; // Default directory
        }
    }
    
    // Create database directory if it doesn't exist
    if (mkdir(db_dir, 0755) != 0 && errno != EEXIST) {
        THROW_DB_ERROR(env, 0, "CREATE_DIR_FAILED", "Failed to create/open database directory '%s': %s", 
                      db_dir, strerror(errno));
    }
    
    // Initialize LMDB environment
    int rc = mdb_env_create(&dbenv);
    if (rc != MDB_SUCCESS) goto open_fail_base;
    
    rc = mdb_env_set_mapsize(dbenv, 1024UL * 1024UL * 1024UL * 1024UL * 16UL); // 16TB
    if (rc != MDB_SUCCESS) goto open_fail_env;
    
    rc = mdb_env_set_maxreaders(dbenv, MAX_TRANSACTIONS);
    if (rc != MDB_SUCCESS) goto open_fail_env;
    
    rc = mdb_env_open(dbenv, db_dir, MDB_NOTLS, 0664);
    if (rc != MDB_SUCCESS) goto open_fail_env;
    
    // Open the database (within a transaction)
    MDB_txn *txn;
    rc = mdb_txn_begin(dbenv, NULL, 0, &txn);
    if (rc != MDB_SUCCESS) goto open_fail_env;
    
    rc = mdb_dbi_open(txn, NULL, 0, &dbi);
    if (rc != MDB_SUCCESS) goto open_fail_txn;
    
    rc = mdb_txn_commit(txn);
    if (rc != MDB_SUCCESS) goto open_fail_env;
    
    return 1;
    
    open_fail_txn:
    mdb_txn_abort(txn);
    open_fail_env:
    mdb_env_close(dbenv);
    dbenv = NULL;
    open_fail_base:
    THROW_LMDB_ERROR(env, 0, rc);
}

napi_value js_start_transaction(napi_env env, napi_callback_info info) {
    // Auto-open the database if needed
    if (!dbenv && !open_lmdb(env, NULL)) return NULL; // Initialization failed, error already thrown
    
    transaction_t *txn = claim_available_transaction();
    if (!txn) {
        THROW_DB_ERROR(env, NULL, "TXN_LIMIT", "Transaction limit reached - no available transaction slots (max: %d)", MAX_TRANSACTIONS);
    }
    
    // Start/Renew LMDB read transaction
    int rc = txn->read_txn ? mdb_txn_renew(txn->read_txn) : mdb_txn_begin(dbenv, NULL, MDB_RDONLY, &txn->read_txn);
    if (rc != MDB_SUCCESS) {
        LOG_INTERNAL_ERROR("Failed to start read transaction: %s", mdb_strerror(rc));
        txn->state = TRANSACTION_FREE;
        txn->next = first_free_transaction;
        first_free_transaction = txn;
        THROW_LMDB_ERROR(env, NULL, rc);
    }
    
    // Combine index and nonce into transaction ID
    napi_value result;
    ASSERT_OR_RETURN(napi_create_int32(env, transaction_to_id(txn), &result) == napi_ok, NULL);
    return result;
}

napi_value js_commit_transaction(napi_env env, napi_callback_info info) {
    napi_status status;
    size_t argc = 2;
    napi_value argv[2];
    
    status = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
    if (status != napi_ok || argc < 1) {
        THROW_TYPE_ERROR(env, NULL, NULL, "Expected at least one argument (transaction ID), got %zu", argc);
    }
    
    int32_t transaction_id;
    status = napi_get_value_int32(env, argv[0], &transaction_id);
    if (status != napi_ok) {
        THROW_TYPE_ERROR(env, NULL, NULL, "Expected transaction ID as integer, got invalid type");
    }
    
    transaction_t *txn = id_to_open_transaction(env, transaction_id);
    if (!txn) return NULL; // Error already thrown
    
    mdb_txn_reset(txn->read_txn);
    
    if (!txn->has_writes) {
        // Read-only transaction, commit immediately
        reset_transaction(txn);
        napi_value result;
        ASSERT_OR_RETURN(napi_get_boolean(env, false, &result) == napi_ok, NULL);
        return result;
    }
    
    // Transaction has writes, prepare for async processing
    txn->state = TRANSACTION_COMMITTING;
    txn->env = env;
    
    // Store callback reference
    napi_valuetype valuetype;
    if (argc > 1 && napi_typeof(env, argv[1], &valuetype) == napi_ok && valuetype == napi_function) {
        status = napi_create_reference(env, argv[1], 1, &txn->callback_ref);
        if (status != napi_ok) {
            LOG_INTERNAL_ERROR("Failed to create callback reference for transaction %d", transaction_id);
            txn->callback_ref = NULL;
        }
    } else {
        txn->callback_ref = NULL; // No callback provided
    }
    
    // Add transaction to the queue
    txn->next = NULL;
    if (last_queued_transaction) {
        last_queued_transaction->next = txn;
    } else {
        first_queued_transaction = txn;
    }
    last_queued_transaction = txn;
    
    // This will do nothing if a worker is already running
    start_worker();
    
    napi_value result;
    ASSERT_OR_RETURN(napi_get_boolean(env, true, &result) == napi_ok, NULL);
    return result;
}

napi_value js_abort_transaction(napi_env env, napi_callback_info info) {
    napi_status status;
    size_t argc = 1;
    napi_value argv[1];
    
    status = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
    if (status != napi_ok || argc < 1) {
        THROW_TYPE_ERROR(env, NULL, NULL, "Expected transaction ID, got %zu arguments", argc);
    }
    
    int32_t transaction_id;
    status = napi_get_value_int32(env, argv[0], &transaction_id);
    if (status != napi_ok) {
        THROW_TYPE_ERROR(env, NULL, NULL, "Expected transaction ID as integer");
    }
    
    transaction_t *txn = id_to_open_transaction(env, transaction_id);
    if (!txn) return NULL; // Error already thrown
    
    mdb_txn_reset(txn->read_txn);
    reset_transaction(txn);
    
    napi_value result;
    ASSERT_OR_RETURN(napi_get_undefined(env, &result) == napi_ok, NULL);
    return result;
}

napi_value js_get(napi_env env, napi_callback_info info) {
    napi_status status;
    size_t argc = 2;
    napi_value argv[2];
    
    status = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
    if (status != napi_ok || argc < 2) {
        THROW_TYPE_ERROR(env, NULL, NULL, "Expected transaction ID and key, got %zu arguments", argc);
    }
    
    int32_t transaction_id;
    status = napi_get_value_int32(env, argv[0], &transaction_id);
    if (status != napi_ok) {
        THROW_TYPE_ERROR(env, NULL, NULL, "Expected transaction ID as integer");
    }
    
    char *key_data;
    size_t key_size;
    status = napi_get_arraybuffer_info(env, argv[1], (void**)&key_data, &key_size);
    if (status != napi_ok) {
        THROW_TYPE_ERROR(env, NULL, NULL, "Expected key as ArrayBuffer");
    }
    
    if (key_size > MAX_KEY_LENGTH) {
        THROW_DB_ERROR(env, NULL, "KEY_TOO_LONG", "Key size %zu exceeds maximum allowed length %d", 
                      key_size, MAX_KEY_LENGTH);
    }
    
    transaction_t *txn = id_to_open_transaction(env, transaction_id);
    if (!txn) return NULL; // Error already thrown
    
    // First check if we have any PUT/DEL operations for this key in our buffer
    update_log_t *update_log = find_update_log(txn, key_size, key_data, 0);
    if (update_log) {
        // Found in buffer, return the value
        if (update_log->value_size == 0) {
            // It's a delete operation
            napi_value result;
            ASSERT_OR_RETURN(napi_get_undefined(env, &result) == napi_ok, NULL);
            return result;
        }
        
        // Create ArrayBuffer for the value
        napi_value result_buffer;
        ASSERT_OR_RETURN(napi_create_external_arraybuffer(env, update_log->data + update_log->key_size, 
                        update_log->value_size, NULL, NULL, &result_buffer) == napi_ok, NULL);
        return result_buffer;
    }
    
    // Not found in buffer, do LMDB lookup
    MDB_val key, value;
    key.mv_data = key_data;
    key.mv_size = key_size;
    int rc = mdb_get(txn->read_txn, dbi, &key, &value);
    
    if (rc == MDB_SUCCESS || rc == MDB_NOTFOUND) {
        read_log_t *read_log = create_read_log(txn, sizeof(get_log_t) + key_size, 0);
        if (read_log) {
            read_log->get.checksum = (rc == MDB_NOTFOUND) ? 0 : checksum((char *)value.mv_data, value.mv_size, CHECKSUM_INITIAL);
            read_log->get.key_size = key_size;
            memcpy(read_log->get.key_data, key_data, key_size);
        }
    }
    
    if (rc == MDB_NOTFOUND) {
        napi_value result;
        ASSERT_OR_RETURN(napi_get_undefined(env, &result) == napi_ok, NULL);
        return result;
    }
    
    if (rc != MDB_SUCCESS) {
        THROW_LMDB_ERROR(env, NULL, rc);
    }
    
    // Create ArrayBuffer for the value
    napi_value result_buffer;
    ASSERT_OR_RETURN(napi_create_external_arraybuffer(env, value.mv_data, value.mv_size, NULL, NULL, &result_buffer) == napi_ok, NULL);
    return result_buffer;
}

napi_value js_put(napi_env env, napi_callback_info info) {
    napi_status status;
    size_t argc = 3;
    napi_value argv[3];
    
    status = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
    if (status != napi_ok || argc < 3) {
        THROW_TYPE_ERROR(env, NULL, NULL, "Expected transaction ID, key, and value, got %zu arguments", argc);
    }
    
    int32_t transaction_id;
    status = napi_get_value_int32(env, argv[0], &transaction_id);
    if (status != napi_ok) {
        THROW_TYPE_ERROR(env, NULL, NULL, "Expected transaction ID as integer");
    }
    
    char *key_data;
    size_t key_size;
    status = napi_get_arraybuffer_info(env, argv[1], (void**)&key_data, &key_size);
    if (status != napi_ok) {
        THROW_TYPE_ERROR(env, NULL, NULL, "Expected key as ArrayBuffer");
    }
    
    char *value_data;
    size_t value_size;
    status = napi_get_arraybuffer_info(env, argv[2], (void**)&value_data, &value_size);
    if (status != napi_ok) {
        THROW_TYPE_ERROR(env, NULL, NULL, "Expected value as ArrayBuffer");
    }
    
    if (key_size > MAX_KEY_LENGTH) {
        THROW_DB_ERROR(env, NULL, "KEY_TOO_LONG", "Key size %zu exceeds maximum allowed length %d", 
                      key_size, MAX_KEY_LENGTH);
    }
    
    transaction_t *txn = id_to_open_transaction(env, transaction_id);
    if (!txn) return NULL; // Error already thrown
    
    add_update_log(txn, key_size, key_data, value_size, value_data);
    txn->has_writes = 1;
    
    napi_value result;
    ASSERT_OR_RETURN(napi_get_undefined(env, &result) == napi_ok, NULL);
    return result;
}

napi_value js_del(napi_env env, napi_callback_info info) {
    napi_status status;
    size_t argc = 2;
    napi_value argv[2];
    
    status = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
    if (status != napi_ok || argc < 2) {
        THROW_TYPE_ERROR(env, NULL, NULL, "Expected transaction ID and key, got %zu arguments", argc);
    }
    
    int32_t transaction_id;
    status = napi_get_value_int32(env, argv[0], &transaction_id);
    if (status != napi_ok) {
        THROW_TYPE_ERROR(env, NULL, NULL, "Expected transaction ID as integer");
    }
    
    char *key_data;
    size_t key_size;
    status = napi_get_arraybuffer_info(env, argv[1], (void**)&key_data, &key_size);
    if (status != napi_ok) {
        THROW_TYPE_ERROR(env, NULL, NULL, "Expected key as ArrayBuffer");
    }
    
    if (key_size > MAX_KEY_LENGTH) {
        THROW_DB_ERROR(env, NULL, "KEY_TOO_LONG", "Key size %zu exceeds maximum allowed length %d", 
                      key_size, MAX_KEY_LENGTH);
    }
    
    transaction_t *txn = id_to_open_transaction(env, transaction_id);
    if (!txn) return NULL; // Error already thrown
    
    add_update_log(txn, key_size, key_data, 0, NULL);
    txn->has_writes = 1;
    
    napi_value result;
    ASSERT_OR_RETURN(napi_get_undefined(env, &result) == napi_ok, NULL);
    return result;
}

napi_value js_create_iterator(napi_env env, napi_callback_info info) {
    size_t argc = 4;
    napi_value argv[4];
    
    if (napi_get_cb_info(env, info, &argc, argv, NULL, NULL) != napi_ok || argc < 1) {
        THROW_TYPE_ERROR(env, NULL, NULL, "Expected at least transaction ID, got %zu arguments", argc);
    }
    
    int32_t transaction_id;
    if (napi_get_value_int32(env, argv[0], &transaction_id) != napi_ok) {
        THROW_TYPE_ERROR(env, NULL, NULL, "Expected transaction ID as integer");
    }
    
    // Fetch the transaction
    transaction_t *txn = id_to_open_transaction(env, transaction_id);
    if (!txn) return NULL; // Error already thrown
    
    napi_valuetype key_type;
    
    // Start key
    const char *start_key_data = NULL;
    size_t start_key_size = 0;
    if (argc >= 2 && napi_typeof(env, argv[1], &key_type) == napi_ok && key_type != napi_null && key_type != napi_undefined) {
        if (napi_get_arraybuffer_info(env, argv[1], (void**)&start_key_data, &start_key_size) != napi_ok) {
            THROW_TYPE_ERROR(env, NULL, NULL, "Expected start key as ArrayBuffer or null/undefined");
        }
    }
    
    // End key
    const char *end_key_data = NULL;
    size_t end_key_size = 0;
    if (argc >= 3 && napi_typeof(env, argv[2], &key_type) == napi_ok && key_type != napi_null && key_type != napi_undefined) {
        if (napi_get_arraybuffer_info(env, argv[2], (void**)&end_key_data, &end_key_size) != napi_ok) {
            THROW_TYPE_ERROR(env, NULL, NULL, "Expected end key as ArrayBuffer or null/undefined");
        }
    }
    
    if (start_key_size > MAX_KEY_LENGTH) {
        THROW_DB_ERROR(env, NULL, "KEY_TOO_LONG", "Start key size %zu exceeds maximum allowed length %d", 
                      start_key_size, MAX_KEY_LENGTH);
    }
    
    if (end_key_size > MAX_KEY_LENGTH) {
        THROW_DB_ERROR(env, NULL, "KEY_TOO_LONG", "End key size %zu exceeds maximum allowed length %d", 
                      end_key_size, MAX_KEY_LENGTH);
    }
    
    // Reverse flag
    bool reverse = false;
    if (argc >= 4 && napi_get_value_bool(env, argv[3], &reverse) != napi_ok) {
        THROW_TYPE_ERROR(env, NULL, NULL, "Expected reverse flag as boolean");
    }
    
    read_log_t *read_log = create_read_log(txn, sizeof(iterate_log_t) + end_key_size, reverse ? 1|2 : 1);
    ASSERT_OR_RETURN(read_log != NULL, NULL);
    
    iterator_t *it = claim_available_iterator(txn);
    ASSERT_OR_RETURN(it != NULL, NULL);
    
    it->end_key_size = (uint16_t)end_key_size;
    it->end_key_data = end_key_data;
    it->next = txn->first_iterator;
    txn->first_iterator = it;    
    it->iterate_log = &read_log->iterate;
    
    // Tag in iterate_log->tagged_next_ptr can be left at 0
    it->iterate_log->key_size = start_key_size;
    it->iterate_log->row_count = 0; // Reset row count for this iterator
    if (start_key_size > 0) {
        memcpy(it->iterate_log->key_data, start_key_data, start_key_size);
    }
    
    if (start_key_size > 0) {
        it->current_update_log = find_update_log(txn, start_key_size, start_key_data, reverse ? 2 : 1);
        it->lmdb_key.mv_data = (char *)start_key_data; // non-const, but LMDB doesn't modify it
        it->lmdb_key.mv_size = start_key_size;
    } else {
        // start at the first/last record
        it->current_update_log = reverse ? find_last_update_log(txn) : txn->update_log_skiplist_ptrs[0];
        it->lmdb_key.mv_data = NULL;
        it->lmdb_key.mv_size = 0;
    }
    
    int rc = place_cursor(it->lmdb_cursor, &it->lmdb_key, &it->lmdb_value, reverse);
    if (rc != MDB_SUCCESS) {
        THROW_LMDB_ERROR(env, NULL, rc);
    }
    
    uint64_t cs = CHECKSUM_INITIAL;
    cs = checksum((const char*)it->lmdb_key.mv_data, it->lmdb_key.mv_size, cs);
    cs = checksum((const char*)it->lmdb_value.mv_data, it->lmdb_value.mv_size, cs);
    it->iterate_log->checksum = cs;
    
    napi_value result;
    ASSERT_OR_RETURN(napi_create_int32(env, iterator_to_id(it), &result) == napi_ok, NULL);
    return result;
}

napi_value js_read_iterator(napi_env env, napi_callback_info info) {
    napi_status status;
    size_t argc = 1;
    napi_value argv[1];
    
    status = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
    if (status != napi_ok || argc < 1) {
        THROW_TYPE_ERROR(env, NULL, NULL, "Expected iterator ID, got %zu arguments", argc);
    }
    
    int32_t iterator_id;
    status = napi_get_value_int32(env, argv[0], &iterator_id);
    if (status != napi_ok) {
        THROW_TYPE_ERROR(env, NULL, NULL, "Expected iterator ID as integer");
    }
    
    iterator_t *it = id_to_open_iterator(env, iterator_id);
    if (!it) return NULL; // Error already thrown
    
    transaction_t *txn = id_to_open_transaction(env, it->transaction_id);
    if (!txn) return NULL; // Error already thrown
    
    // The reverse flag is stored in the iterate_log->tagged_next_ptr
    int reverse = get_ptr_tag(it->iterate_log->tagged_next_ptr) & 2;
    
    restart_read_iterator:
    int cmp;
    if (it->current_update_log) {
        if (it->lmdb_key.mv_data) {
            // Both are present.. we need to merge!
            cmp = compare_keys(it->lmdb_key.mv_size, (const char*)it->lmdb_key.mv_data, it->current_update_log->key_size, it->current_update_log->data);
            // When equal (0): take from both, but return update_log value
            if (reverse) cmp = -cmp; // Reverse comparison for backwards iteration
        } else {
            cmp = +1; // Take from update_log
        }
    } else {
        if (it->lmdb_key.mv_data) {
            cmp = -1; // Take from lmdb
        } else { // No more items to read
            napi_value result_undefined;
            ASSERT_OR_RETURN(napi_get_undefined(env, &result_undefined) == napi_ok, NULL);
            return result_undefined;
        }
    }
    
    MDB_val key, val;
    
    if (cmp <= 0) { // Take from lmdb
        key.mv_data = it->lmdb_key.mv_data;
        key.mv_size = it->lmdb_key.mv_size;
        val.mv_data = it->lmdb_value.mv_data;
        val.mv_size = it->lmdb_value.mv_size;
        
        // Read the next item in the LMDB cursor
        int rc = mdb_cursor_get(it->lmdb_cursor, &it->lmdb_key, &it->lmdb_value, reverse ? MDB_PREV : MDB_NEXT);
        if (rc == MDB_NOTFOUND) {
            it->lmdb_key.mv_data = it->lmdb_value.mv_data = NULL; // mark cursor as at the end of the database
            it->lmdb_key.mv_size = it->lmdb_value.mv_size = 0;
        } else if (rc != MDB_SUCCESS) {
            THROW_LMDB_ERROR(env, NULL, rc);
        }
        
        uint64_t cs = it->iterate_log->checksum;
        cs = checksum((const char*)it->lmdb_key.mv_data, it->lmdb_key.mv_size, cs);
        cs = checksum((const char*)it->lmdb_value.mv_data, it->lmdb_value.mv_size, cs);
        it->iterate_log->checksum = cs;
        
        it->iterate_log->row_count++;
    }
    
    if (cmp >= 0) { // Take from update_log (can be in addition to lmdb above, when key matches)
        key.mv_data = it->current_update_log->data;
        key.mv_size = it->current_update_log->key_size;
        val.mv_data = it->current_update_log->data + it->current_update_log->key_size;
        val.mv_size = it->current_update_log->value_size;
        
        // Move to next update log entry
        it->current_update_log = reverse ? it->current_update_log->prev_ptr : it->current_update_log->next_ptrs[0];
        
        if (val.mv_size == 0) {
            // It's a delete.. we'll skip this
            goto restart_read_iterator;
        }
    }
    
    if (it->end_key_data) {
        cmp = compare_keys(key.mv_size, key.mv_data, it->end_key_size, it->end_key_data);
        if (reverse ? cmp <= 0 : cmp >= 0) {
            // We're at or past end key. Mark iterator as done and return undefined.
            it->current_update_log = NULL; // Mark log iterator as done
            it->lmdb_key.mv_data = NULL; // Mark LMDB cursor as done
            it->lmdb_key.mv_size = 0;
            napi_value result_undefined;
            ASSERT_OR_RETURN(napi_get_undefined(env, &result_undefined) == napi_ok, NULL);
            return result_undefined;
        }
    }
    
    // Create result object with key and value ArrayBuffers
    napi_value result_obj;
    ASSERT_OR_RETURN(napi_create_object(env, &result_obj) == napi_ok, NULL);
    
    // Create key ArrayBuffer
    napi_value key_buffer;
    ASSERT_OR_RETURN(napi_create_external_arraybuffer(env, key.mv_data, key.mv_size, NULL, NULL, &key_buffer) == napi_ok, NULL);
    
    // Create value ArrayBuffer
    napi_value val_buffer;
    ASSERT_OR_RETURN(napi_create_external_arraybuffer(env, val.mv_data, val.mv_size, NULL, NULL, &val_buffer) == napi_ok, NULL);
    
    // Set properties
    ASSERT_OR_RETURN(napi_set_named_property(env, result_obj, "key", key_buffer) == napi_ok, NULL);
    ASSERT_OR_RETURN(napi_set_named_property(env, result_obj, "value", val_buffer) == napi_ok, NULL);
    
    return result_obj;
}

napi_value js_close_iterator(napi_env env, napi_callback_info info) {
    napi_status status;
    size_t argc = 1;
    napi_value argv[1];
    
    status = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
    if (status != napi_ok || argc < 1) {
        THROW_TYPE_ERROR(env, NULL, NULL, "Expected iterator ID, got %zu arguments", argc);
    }
    
    int32_t iterator_id;
    status = napi_get_value_int32(env, argv[0], &iterator_id);
    if (status != napi_ok) {
        THROW_TYPE_ERROR(env, NULL, NULL, "Expected iterator ID as integer");
    }
    
    // Fetch the iterator
    iterator_t *iterator = id_to_open_iterator(env, iterator_id);
    if (!iterator) return NULL; // Error already thrown
    
    transaction_t *txn = id_to_open_transaction(env, iterator->transaction_id);
    if (!txn) return NULL; // Error already thrown
    
    // Remove iterator from the transaction's linked list    
    if (iterator == txn->first_iterator) {
        txn->first_iterator = iterator->next;
    } else {
        iterator_t *prev = txn->first_iterator;
        while (prev) {
            if (prev->next == iterator) {
                prev->next = iterator->next; // Remove from list
                break;
            }
            prev = prev->next;
        }
    }
    
    iterator->transaction_id = -1;
    // Add to free list
    iterator->next = first_free_iterator;
    first_free_iterator = iterator;
    
    napi_value result;
    ASSERT_OR_RETURN(napi_get_undefined(env, &result) == napi_ok, NULL);
    return result;
}

napi_value js_open_lmdb(napi_env env, napi_callback_info info) {
    // Get optional db_dir argument
    size_t argc = 1;
    napi_value argv[1];
    char *db_dir = NULL;
    char db_dir_buffer[512];
    
    if (napi_get_cb_info(env, info, &argc, argv, NULL, NULL) == napi_ok && argc >= 1) {
        // Get the database directory argument
        if (napi_get_value_string_utf8(env, argv[0], db_dir_buffer, sizeof(db_dir_buffer), NULL) != napi_ok) {
            THROW_TYPE_ERROR(env, NULL, NULL, "Expected database path as string");
        }
        db_dir = db_dir_buffer;
    }
    
    if (!open_lmdb(env, db_dir)) return NULL; // Error already thrown
    
    napi_value result;
    ASSERT_OR_RETURN(napi_get_undefined(env, &result) == napi_ok, NULL);
    return result;
}

// DatabaseError constructor
napi_value database_error_constructor(napi_env env, napi_callback_info info) {
    size_t argc = 2;
    napi_value args[2];
    napi_value js_this;

    ASSERT_OR_RETURN(napi_get_cb_info(env, info, &argc, args, &js_this, NULL) == napi_ok, NULL);

    if (argc < 1) {
        THROW_TYPE_ERROR(env, NULL, NULL, "DatabaseError requires at least a message argument");
    }

    // Set message property
    ASSERT_OR_RETURN(napi_set_named_property(env, js_this, "message", args[0]) == napi_ok, NULL);

    // Set code property if provided
    if (argc >= 2) {
        ASSERT_OR_RETURN(napi_set_named_property(env, js_this, "code", args[1]) == napi_ok, NULL);
    } else {
        napi_value undefined;
        ASSERT_OR_RETURN(napi_get_undefined(env, &undefined) == napi_ok, NULL);
        ASSERT_OR_RETURN(napi_set_named_property(env, js_this, "code", undefined) == napi_ok, NULL);
    }

    // Set name property
    napi_value name;
    ASSERT_OR_RETURN(napi_create_string_utf8(env, "DatabaseError", NAPI_AUTO_LENGTH, &name) == napi_ok, NULL);
    ASSERT_OR_RETURN(napi_set_named_property(env, js_this, "name", name) == napi_ok, NULL);

    return js_this;
}

// Module initialization
napi_value Init(napi_env env, napi_value exports) {
    napi_status status;
    napi_value fn;

    // Create DatabaseError constructor
    napi_value database_error_ctor;
    status = napi_create_function(env, "DatabaseError", NAPI_AUTO_LENGTH, database_error_constructor, NULL, &database_error_ctor);
    ASSERT_OR_RETURN(status == napi_ok, NULL);

    // Get Error constructor to inherit from
    napi_value global;
    ASSERT_OR_RETURN(napi_get_global(env, &global) == napi_ok, NULL);
    
    napi_value error_ctor;
    ASSERT_OR_RETURN(napi_get_named_property(env, global, "Error", &error_ctor) == napi_ok, NULL);

    // Set up prototype chain
    napi_value error_prototype;
    ASSERT_OR_RETURN(napi_get_named_property(env, error_ctor, "prototype", &error_prototype) == napi_ok, NULL);

    napi_value database_error_prototype;
    ASSERT_OR_RETURN(napi_create_object(env, &database_error_prototype) == napi_ok, NULL);

    // Set DatabaseError.prototype.__proto__ = Error.prototype
    ASSERT_OR_RETURN(napi_set_named_property(env, database_error_prototype, "__proto__", error_prototype) == napi_ok, NULL);

    // Set DatabaseError.prototype
    ASSERT_OR_RETURN(napi_set_named_property(env, database_error_ctor, "prototype", database_error_prototype) == napi_ok, NULL);

    // Export DatabaseError
    ASSERT_OR_RETURN(napi_set_named_property(env, exports, "DatabaseError", database_error_ctor) == napi_ok, NULL);

    // Store reference for later use
    ASSERT_OR_RETURN(napi_create_reference(env, database_error_ctor, 1, &database_error_constructor_ref) == napi_ok, NULL);

    // Function exports table
    struct {
        const char* name;
        napi_callback callback;
    } functions[] = {
        {"open", js_open_lmdb},
        {"startTransaction", js_start_transaction},
        {"commitTransaction", js_commit_transaction},
        {"abortTransaction", js_abort_transaction},
        {"get", js_get},
        {"put", js_put},
        {"del", js_del},
        {"createIterator", js_create_iterator},
        {"readIterator", js_read_iterator},
        {"closeIterator", js_close_iterator}
    };
    
    // Export functions
    for (size_t i = 0; i < sizeof(functions) / sizeof(functions[0]); i++) {
        status = napi_create_function(env, NULL, 0, functions[i].callback, NULL, &fn);
        ASSERT_OR_RETURN(status == napi_ok, NULL);
        ASSERT_OR_RETURN(napi_set_named_property(env, exports, functions[i].name, fn) == napi_ok, NULL);
    }
    
    // Export state constants
    struct {
        const char* name;
        int value;
    } constants[] = {
        {"TRANSACTION_RACED", TRANSACTION_RACED},
        {"TRANSACTION_SUCCEEDED", TRANSACTION_SUCCEEDED},
        {"TRANSACTION_FAILED", TRANSACTION_FAILED}
    };
    
    // Export constants
    for (size_t i = 0; i < sizeof(constants) / sizeof(constants[0]); i++) {
        napi_value constant_value;
        ASSERT_OR_RETURN(napi_create_int32(env, constants[i].value, &constant_value) == napi_ok, NULL);
        ASSERT_OR_RETURN(napi_set_named_property(env, exports, constants[i].name, constant_value) == napi_ok, NULL);
    }
    
    return exports;
}

NAPI_MODULE(NODE_GYP_MODULE_NAME, Init)