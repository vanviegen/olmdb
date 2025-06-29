/**
 * See the architecture section in README.md before delving into this code.
 * 
 * Import abbreviations:
 * - ltxn: An OLMDB logical transaction, which is what our user works with
 * - rtxn: An LMDB read-only transaction, used during the initial phase of one or more logical transactions
 * - wtxn: An LMDB read/write transaction, used in the commit phase of one or more logical transaction
 */

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

#include <stdio.h>
#include <time.h>

#ifdef _WIN32
    #include <windows.h>
    #include <sys/timeb.h>
#else
    #include <sys/time.h>
#endif


// Configuration constants
#define MAX_LTXNS 0x100000
#define MAX_ITERATORS 0x10000
#define DEFAULT_LOG_BUFFER_SIZE (32 * 1024 - sizeof(log_buffer_t))
#define MAX_KEY_LENGTH 511
#define SKIPLIST_DEPTH 4
#define MAX_RTXNS 254

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

typedef struct rtxn_wrapper_struct {
    MDB_txn *rtxn;
    union {
        int ref_count;
        struct rtxn_wrapper_struct *next_free; // Free list
    };
} rtxn_wrapper_t;

#define TRANSACTION_FREE 0
#define TRANSACTION_OPEN 1
#define TRANSACTION_COMMITTING 2
#define TRANSACTION_RACED 3
#define TRANSACTION_SUCCEEDED 4
#define TRANSACTION_FAILED 5

// Transaction structure
typedef struct ltxn_struct {
    uint16_t nonce;
    uint8_t state; // TRANSACTION_*
    uint8_t has_writes;

    rtxn_wrapper_t *rtxn_wrapper; // Pointer to LMDB 
    // Buffer for storing commands (first/main buffer)
    log_buffer_t *first_log_buffer;
    log_buffer_t *last_log_buffer;  // Points to buffer being written to
    char *log_end_ptr; // Next write pos within current_buffer
    read_log_t *first_read_log; // Chronologically first read entry
    read_log_t *last_read_log; // Chronologically last read entry
    update_log_t *update_log_skiplist_ptrs[SKIPLIST_DEPTH]; // Skiplist for write entries, 4 pointers for 4 levels
    
    struct ltxn_struct *next; // Queue management for worker thread and free list
    struct iterator_struct *first_iterator; // Linked list of iterators for this logical transaction
    
    // Callback for async completion
    napi_ref callback_ref;
    napi_env env;
} ltxn_t;

typedef struct iterator_struct {
    struct iterator_struct *next; // Next iterator in the free list, or within the transaction
    MDB_cursor *cursor;
    MDB_val lmdb_key; // when mv_data is NULL, it means the cursor is at the end of the database
    MDB_val lmdb_value;
    update_log_t *current_update_log; // Current position within uncommitted records
    iterate_log_t *iterate_log; // Log entry about this iterator
    const char *end_key_data; // If set, the cursor will stop at this key
    int ltxn_id; // Set to -1 when free
    uint16_t end_key_size;
    uint16_t nonce;
} iterator_t;



// Global state
// LMDB environment
static MDB_env *dbenv = NULL;
static MDB_dbi dbi;

// Transactions
static ltxn_t ltxns[MAX_LTXNS]; // This is quite large, but Linux will allocate pages lazily
static ltxn_t *first_free_ltxn = NULL; // Start of linked-list of free transactions
static int next_unused_ltxn = 0; // Index of next never-used transaction
static ltxn_t *first_queued_ltxn = NULL; // Queue of transactions waiting for processing
static ltxn_t *last_queued_ltxn = NULL;

// Log buffers
static log_buffer_t *first_free_log_buffer = NULL; // Start of linked-list of free log buffers of DEFAULT_LOG_BUFFER_SIZE

// Iterators
static iterator_t iterators[MAX_ITERATORS]; // This is quite large, but Linux will allocate pages lazily
static iterator_t *first_free_iterator = NULL; // Start of linked-list of free iterators
static int next_unused_iterator = 0; // Index of next never-used iterator

// Read transaction wrappers
rtxn_wrapper_t rtxn_wrappers[MAX_RTXNS];
rtxn_wrapper_t *first_free_rtxn_wrapper = NULL;
int next_unused_rtxn_wrapper = 0;

rtxn_wrapper_t *current_rtxn_wrapper = NULL;
long long current_rtxn_expire_time = 0;

// Worker
int worker_running = 0; // Flag to indicate if a worker is currently processing transactions
napi_async_work worker_work;

// Random number generator
uint64_t rng_state = 0;

// DatabaseError class reference
static napi_ref database_error_constructor_ref = NULL;

// Prototypes for internal functions
static uint64_t checksum(const char *data, size_t len, uint64_t initial_checksum);
static void *allocate_log_space(ltxn_t *ltxn, size_t needed_space);
static void start_worker();
static void on_worker_ready(napi_env env, napi_status status, void *data);
static void process_write_commits(napi_env env, void *ltxns);
static int validate_reads(MDB_txn *wtxn, ltxn_t *ltxn, MDB_cursor *validation_cursor);
static void release_ltxn(ltxn_t *ltxn);
static ltxn_t *allocate_ltxn();
static ltxn_t *id_to_open_ltxn(napi_env env, int transaction_id);
static int ltxn_to_id(ltxn_t *ltxn);
static update_log_t *find_update_log(ltxn_t *ltxn, uint16_t key_size, const char *key_data, int allow_before);
static void delete_update_log(ltxn_t *ltxn, update_log_t *log);
static int place_cursor(MDB_cursor *cursor, MDB_val *key, MDB_val *value, int reverse);
static uint32_t get_random_number();
int open_lmdb(napi_env env, const char *db_dir);

// Logging and error macros
#define LOG_INTERNAL_ERROR(message, ...) \
    fprintf(stderr, "OLMDB: %s:%d " message "\n", __FILE__, __LINE__, ##__VA_ARGS__)

#define ASSERT(condition) \
    do { \
        if (!(condition)) { \
            LOG_INTERNAL_ERROR("Assertion failed: " #condition); \
        } \
    } while(0)

#define ASSERT_OR_RETURN(condition, return_value) \
    do { \
        if (!(condition)) { \
            LOG_INTERNAL_ERROR("Assertion failed: " #condition); \
            return return_value; \
        } \
    } while(0)

// Error throwing macros
#define THROW_DB_ERROR(env, return_value, code, message, ...) \
    do { \
        char _error_msg[512]; \
        snprintf(_error_msg, sizeof(_error_msg), message, ##__VA_ARGS__); \
        throw_db_error(env, code, _error_msg); \
        return (return_value); \
    } while(0)

#define THROW_TYPE_ERROR(env, return_value, code, message, ...) \
    do { \
        char _error_msg[512]; \
        snprintf(_error_msg, sizeof(_error_msg), message, ##__VA_ARGS__); \
        napi_throw_type_error(env, code, _error_msg); \
        return (return_value); \
    } while(0)

#define THROW_LMDB_ERROR(env, return_value, message, error_code) \
    do { \
        char _code[16]; \
        snprintf(_code, sizeof(_code), "LMDB%d", error_code); \
        THROW_DB_ERROR(env, return_value, _code, message " (%s)", mdb_strerror(error_code)); \
    } while(0)


// Get the pointer to a struct from a pointer to one of its members
#define CONTAINER_OF(ptr, type, member) ((type *)((char *)(ptr) - offsetof(type, member)))

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

static int ltxn_to_id(ltxn_t *ltxn) {
    int idx = ltxn - ltxns;
    ASSERT(idx >= 0 && idx < MAX_LTXNS);
    return (idx << 12) | (ltxn->nonce & 0xfff); // Use the lower 12 bits for nonce
}

// __attribute__((constructor))
// static void olmdb_init(void) {
// }

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

long long get_time_ms() {
#ifdef _WIN32
    struct _timeb tb;
    _ftime_s(&tb);
    return (long long)tb.time * 1000 + tb.millitm;
#else
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (long long)tv.tv_sec * 1000 + tv.tv_usec / 1000;
#endif
}

// Ensure buffer has enough space, growing if necessary
static void *allocate_log_space(ltxn_t *ltxn, size_t needed_space) {
    // Align buffer position to 8 bytes
    ltxn->log_end_ptr = (void *)(((uintptr_t)ltxn->log_end_ptr + 7) & ~7);
    
    if (!ltxn->last_log_buffer || ltxn->log_end_ptr + needed_space > ltxn->last_log_buffer->data + ltxn->last_log_buffer->size) {
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
        if (ltxn->last_log_buffer) {
            ltxn->last_log_buffer->next = new_buf;
        } else {
            ltxn->first_log_buffer = new_buf;
        }
        ltxn->last_log_buffer = new_buf;
        // Align buffer position to 8 bytes
        ltxn->log_end_ptr = (void *)(((uintptr_t)new_buf->data + 7) & ~7);
    }
    
    void *result = ltxn->log_end_ptr;
    ltxn->log_end_ptr += needed_space;
    return result;
}

static read_log_t *create_read_log(ltxn_t *ltxn, size_t size, uint8_t tag) {    
    read_log_t *read_log = allocate_log_space(ltxn, size);
    ASSERT_OR_RETURN(read_log != NULL, NULL);
    ASSERT(get_ptr_tag(read_log) == 0);
    
    // Initialize the read log entry
    read_log->tagged_next_ptr = make_tag_ptr(NULL, tag);
    
    if (ltxn->last_read_log) {
        // Keep tag on previous log's next pointer
        ltxn->last_read_log->tagged_next_ptr = make_tag_ptr(read_log, get_ptr_tag(ltxn->last_read_log->tagged_next_ptr));
    } else {
        ltxn->first_read_log = read_log;
    }
    ltxn->last_read_log = read_log;
    return read_log;
}

// Reset transaction state and put it back into the free list
static void release_ltxn(ltxn_t *ltxn) {        
    // Free all except the first log buffer (which is always DEFAULT_LOG_BUFFER_SIZE bytes)
    log_buffer_t *lb = ltxn->first_log_buffer;
    ltxn->first_log_buffer = NULL;
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
    ltxn->first_read_log = NULL;
    ltxn->last_read_log = NULL;
    for (int i = 0; i < SKIPLIST_DEPTH; i++) {
        ltxn->update_log_skiplist_ptrs[i] = NULL;
    }
    
    ltxn->has_writes = 0;
    ltxn->next = NULL;
    
    // Clean up callback reference
    if (ltxn->callback_ref) {
        napi_status status = napi_delete_reference(ltxn->env, ltxn->callback_ref);
        if (status != napi_ok) {
            LOG_INTERNAL_ERROR("Failed to delete callback reference for transaction %d", ltxn_to_id(ltxn));
        }
        ltxn->callback_ref = NULL;
    }
    ltxn->env = NULL;
    
    // Move iterators to free list
    iterator_t *it = ltxn->first_iterator;
    while(it) {
        it->ltxn_id = -1;
        iterator_t *next = it->next;
        it->next = NULL;
        it = next;
    }
    ltxn->first_iterator = NULL;
    
    // Return transaction to free list
    ltxn->state = TRANSACTION_FREE;
    ltxn->next = first_free_ltxn;
    first_free_ltxn = ltxn;
}

static ltxn_t *id_to_open_ltxn(napi_env env, int ltxn_id) {
    int idx = ltxn_id >> 12;
    uint16_t nonce = ltxn_id & 0xfff;
    
    if (idx >= MAX_LTXNS) {
        THROW_DB_ERROR(env, NULL, "INVALID_TRANSACTION", "Transaction index %d exceeds maximum %d", idx, MAX_LTXNS - 1);
    }
    
    ltxn_t *ltxn = &ltxns[idx];
    if (ltxn->state != TRANSACTION_OPEN || ltxn->nonce != nonce) {
        THROW_DB_ERROR(env, NULL, "INVALID_TRANSACTION", "Transaction ID %d not found or already closed (index=%d, nonce=%u, state=%d, expected_nonce=%u)", 
                      ltxn_id, idx, nonce, ltxn->state, ltxn->nonce);
    }
    
    return ltxn;
}

void assign_rtxn_wrapper(ltxn_t *ltxn) {
    ASSERT(ltxn->rtxn_wrapper == NULL);
    long long time = get_time_ms();
    if (time < current_rtxn_expire_time) {
        current_rtxn_wrapper->ref_count++;
        ltxn->rtxn_wrapper = current_rtxn_wrapper;
        return;
    }

    rtxn_wrapper_t *rtxn_wrapper = first_free_rtxn_wrapper;
    if (rtxn_wrapper) {
        first_free_rtxn_wrapper = rtxn_wrapper->next_free;
        mdb_txn_renew(rtxn_wrapper->rtxn);
    } else {
        if (next_unused_rtxn_wrapper >= MAX_RTXNS) {
            LOG_INTERNAL_ERROR("Exceeded maximum read transactions");
            ltxn->rtxn_wrapper = current_rtxn_wrapper; // do *something*?!
            return;
        }
        rtxn_wrapper = &rtxn_wrappers[next_unused_rtxn_wrapper++];
        mdb_txn_begin(dbenv, NULL, MDB_RDONLY, &rtxn_wrapper->rtxn);
    }
    rtxn_wrapper->ref_count = 1;
    current_rtxn_wrapper = rtxn_wrapper;
    current_rtxn_expire_time = time+100; // Share this read transaction with all logical transactions started within the next 100 ms
    ltxn->rtxn_wrapper = rtxn_wrapper;
}

void release_rtxn_wrapper(ltxn_t *ltxn) {
    rtxn_wrapper_t *rtxn_wrapper = ltxn->rtxn_wrapper;
    ASSERT_OR_RETURN(rtxn_wrapper != NULL,);
    if (--rtxn_wrapper->ref_count <= 0) {
        // Return to free list
        rtxn_wrapper->next_free = first_free_rtxn_wrapper;
        first_free_rtxn_wrapper = rtxn_wrapper;
        mdb_txn_reset(rtxn_wrapper->rtxn);
        if (current_rtxn_wrapper == rtxn_wrapper) {
            // If no other ltxns are using this, we might as well reset it
            current_rtxn_wrapper = NULL;
            current_rtxn_expire_time = 0;
        }
    }
    ltxn->rtxn_wrapper = NULL;
}


// Find an available transaction slot
static ltxn_t *allocate_ltxn() {
    ltxn_t *ltxn;
    if (first_free_ltxn) {
        ltxn = first_free_ltxn;
        first_free_ltxn = ltxn->next;
    } else if (next_unused_ltxn < MAX_LTXNS) {
        ltxn = &ltxns[next_unused_ltxn++];
    } else {
        return NULL;
    }
    ltxn->nonce = get_random_number() & 0xfff; // Generate a random nonce
    ltxn->state = TRANSACTION_OPEN;
    return ltxn;
}

static iterator_t *id_to_open_iterator(napi_env env, int iterator_id) {
    int idx = iterator_id >> 12;
    uint16_t nonce = iterator_id & 0xfff;
    
    if (idx >= MAX_ITERATORS) {
        THROW_DB_ERROR(env, NULL, "INVALID_ITERATOR", "Iterator index %d exceeds maximum %d", idx, MAX_ITERATORS - 1);
    }
    
    iterator_t *iterator = &iterators[idx];
    if (iterator->ltxn_id < 0 || iterator->nonce != nonce) {
        THROW_DB_ERROR(env, NULL, "INVALID_ITERATOR", "Iterator ID %d not found or already closed (index=%d, nonce=%u, txn_id=%d, expected_nonce=%u)", 
                      iterator_id, idx, nonce, iterator->ltxn_id, iterator->nonce);
    }
    
    return iterator;
}

static int iterator_to_id(iterator_t *iterator) {
    int idx = iterator - iterators;
    ASSERT(idx >= 0 && idx < MAX_ITERATORS);
    return (idx << 12) | (iterator->nonce & 0xfff);
}

static iterator_t *allocate_iterator(ltxn_t *ltxn) {
    iterator_t *iterator;
    if (first_free_iterator) {
        iterator = first_free_iterator;
        ASSERT(iterator->ltxn_id < 0); // Should be free
        first_free_iterator = iterator->next;
        ASSERT_OR_RETURN(mdb_cursor_renew(ltxn->rtxn_wrapper->rtxn, iterator->cursor) == MDB_SUCCESS, NULL);
    } else if (next_unused_iterator < MAX_ITERATORS) {
        iterator = &iterators[next_unused_iterator++];
        ASSERT_OR_RETURN(mdb_cursor_open(ltxn->rtxn_wrapper->rtxn, dbi, &iterator->cursor) == MDB_SUCCESS, NULL);
    } else {
        LOG_INTERNAL_ERROR("No free iterator slots available (max: %d)", MAX_ITERATORS);
        return NULL;
    }
    iterator->ltxn_id = ltxn_to_id(ltxn);
    iterator->nonce = get_random_number() & 0xfff; // Generate a random nonce
    return iterator;
}

// if_not_found: 0=returning nothing 1=return next 2=return previous
static update_log_t *find_update_log(ltxn_t *ltxn, uint16_t key_size, const char *key_data, int if_not_found) {
    int level = SKIPLIST_DEPTH - 1;
    update_log_t **next_ptrs = ltxn->update_log_skiplist_ptrs;
    
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
        return next_ptrs==ltxn->update_log_skiplist_ptrs ? NULL : CONTAINER_OF(next_ptrs, update_log_t, next_ptrs);
    }
    return NULL;
}

static update_log_t *find_last_update_log(ltxn_t *ltxn) {
    int level = SKIPLIST_DEPTH - 1;
    update_log_t **next_ptrs = ltxn->update_log_skiplist_ptrs;
    
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
    
    if (next_ptrs == ltxn->update_log_skiplist_ptrs) return NULL; // No entries at all
    // This cast happens to be right, as the next_ptrs are at the start of the record_t
    return (update_log_t *)next_ptrs;
}

static void add_update_log(ltxn_t *ltxn, uint16_t key_size, const char *key_data, uint32_t value_size, const char *value_data) {
    int size = sizeof(update_log_t) + (int)key_size + value_size;
    update_log_t *new_log = allocate_log_space(ltxn, size);
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
    update_log_t **current_ptrs = ltxn->update_log_skiplist_ptrs;
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
    // at ltxn->update_log_skiplist_ptrs, if no predecessors were found
    // Set prev_ptr for reverse iteration
    new_log->prev_ptr = current_ptrs == ltxn->update_log_skiplist_ptrs ? NULL : CONTAINER_OF(current_ptrs, update_log_t, next_ptrs);
    
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
        delete_update_log(ltxn, new_log->next_ptrs[0]);
    }
}

static void delete_update_log(ltxn_t *ltxn, update_log_t *log) {
    // Find all predecessors that point to this log entry across all levels
    int level = SKIPLIST_DEPTH - 1;
    update_log_t **current_ptrs = ltxn->update_log_skiplist_ptrs;
    
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
    ltxn_t *ltxns = first_queued_ltxn;
    if (!ltxns) return; // Nothing to process
    first_queued_ltxn = last_queued_ltxn = NULL;
    
    worker_running = 1;
    napi_value resource_name;
    ASSERT_OR_RETURN(napi_create_string_utf8(ltxns->env, "commit_work", NAPI_AUTO_LENGTH, &resource_name) == napi_ok,);
    
    napi_status status = napi_create_async_work(
        ltxns->env, NULL, resource_name,
        process_write_commits, on_worker_ready,
        ltxns, &worker_work
    );
    
    if (status != napi_ok) {
        LOG_INTERNAL_ERROR("Failed to create async work");
        worker_running = 0;
        return;
    }
    
    status = napi_queue_async_work(ltxns->env, worker_work);
    if (status != napi_ok) {
        LOG_INTERNAL_ERROR("Failed to queue async work");
        napi_delete_async_work(ltxns->env, worker_work);
        worker_running = 0;
    }
}

// Worker completion function (main thread callback)
static void on_worker_ready(napi_env env, napi_status status, void *data) {
    (void)status; // Unused parameter
    ltxn_t *ltxns = (ltxn_t *)data;
    
    // Call callbacks for each completed transaction
    ltxn_t *ltxn = ltxns;
    while (ltxn) {
        ltxn_t *next = ltxn->next;
        
        if (ltxn->callback_ref) {
            napi_value callback;
            if (napi_get_reference_value(env, ltxn->callback_ref, &callback) == napi_ok) {
                napi_value global;
                if (napi_get_global(env, &global) != napi_ok) {
                    LOG_INTERNAL_ERROR("Failed to get global object for callback");
                } else {
                    napi_value ltxn_id;
                    napi_value status;
                    
                    if (napi_create_int32(env, ltxn_to_id(ltxn), &ltxn_id) != napi_ok ||
                        napi_create_int32(env, ltxn->state, &status) != napi_ok) {
                        LOG_INTERNAL_ERROR("Failed to create callback arguments for transaction %d", ltxn_to_id(ltxn));
                    } else {
                        napi_value argv[] = { ltxn_id, status };
                        napi_value callback_result;
                        if (napi_call_function(env, global, callback, 2, argv, &callback_result) != napi_ok) {
                            LOG_INTERNAL_ERROR("Failed to call callback for transaction %d", ltxn_to_id(ltxn));
                        }
                    }
                }
            } else {
                LOG_INTERNAL_ERROR("Could not obtain callback value for transaction %d", ltxn_to_id(ltxn));
            }
        }
        
        release_ltxn(ltxn);
        ltxn = next;
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
    ltxn_t *ltxns = (ltxn_t *)data;
    
    MDB_txn *wtxn = NULL;
    int rc = mdb_txn_begin(dbenv, NULL, 0, &wtxn);
    if (rc != MDB_SUCCESS) {
        LOG_INTERNAL_ERROR("Failed to begin write transaction: %s", mdb_strerror(rc));
        // Mark all transactions as failed
        for(ltxn_t *ltxn = ltxns; ltxn; ltxn = ltxn->next) {
            ltxn->state = TRANSACTION_FAILED;
        }
        return;
    }
    
    // Cursors within read/write transactions will be closed automatically
    // when the transaction ends
    int failed = 0;
    MDB_cursor *validation_cursor;
    rc = mdb_cursor_open(wtxn, dbi, &validation_cursor);
    if (rc != MDB_SUCCESS) {
        LOG_INTERNAL_ERROR("Failed to open cursor for validation: %s", mdb_strerror(rc));
        failed = 1;
    }
    
    for(ltxn_t *ltxn = ltxns; ltxn && !failed; ltxn = ltxn->next) {
        // Validate all reads for this transaction
        ltxn->state = validate_reads(wtxn, ltxn, validation_cursor);
        
        if (ltxn->state == TRANSACTION_RACED) continue; // Skip processing if transaction has been raced
        if (ltxn->state == TRANSACTION_FAILED) break; // Stop processing further transactions
        
        // Process all write entries in skiplist order
        update_log_t *update = ltxn->update_log_skiplist_ptrs[0];
        
        while (update && !failed) {
            MDB_val key;
            key.mv_data = update->data;
            key.mv_size = update->key_size;
            if (update->value_size == 0) {
                // Delete operation
                rc = mdb_del(wtxn, dbi, &key, NULL);
                if (rc != MDB_SUCCESS && rc != MDB_NOTFOUND) {
                    LOG_INTERNAL_ERROR("Failed to delete key %.*s: %s", (int)update->key_size, update->data, mdb_strerror(rc));
                    failed = 1;
                }
            } else {
                // Put operation
                MDB_val value;
                value.mv_data = update->data + update->key_size;
                value.mv_size = update->value_size;
                
                rc = mdb_put(wtxn, dbi, &key, &value, 0);
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
        mdb_txn_abort(wtxn);
        // Mark remaining transactions as failed
        for(ltxn_t *ltxn = ltxns; ltxn; ltxn = ltxn->next) {
            if (ltxn->state == TRANSACTION_COMMITTING) {
                ltxn->state = TRANSACTION_FAILED;
            }
        }
    } else {
        rc = mdb_txn_commit(wtxn);
        if (rc != MDB_SUCCESS) {
            LOG_INTERNAL_ERROR("Failed to commit write transaction: %s", mdb_strerror(rc));
            for(ltxn_t *ltxn = ltxns; ltxn; ltxn = ltxn->next) {
                if (ltxn->state == TRANSACTION_COMMITTING) {
                    ltxn->state = TRANSACTION_FAILED;
                }
            }
        }
    }
}

// Validate that all read values haven't changed: returns TRANSACTION_*
static int validate_reads(MDB_txn *wtxn, ltxn_t *ltxn, MDB_cursor *validation_cursor) {
    read_log_t *current = ltxn->first_read_log;
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
            int rc = mdb_get(wtxn, dbi, &key, &value);
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
    
    rc = mdb_env_set_maxreaders(dbenv, MAX_LTXNS);
    if (rc != MDB_SUCCESS) goto open_fail_env;
    
    rc = mdb_env_open(dbenv, db_dir, MDB_NOTLS, 0664);
    if (rc != MDB_SUCCESS) goto open_fail_env;
    
    // Open the database (within a transaction)
    MDB_txn *wtxn;
    rc = mdb_txn_begin(dbenv, NULL, 0, &wtxn);
    if (rc != MDB_SUCCESS) goto open_fail_env;
    
    rc = mdb_dbi_open(wtxn, NULL, 0, &dbi);
    if (rc != MDB_SUCCESS) goto open_fail_txn;
    
    rc = mdb_txn_commit(wtxn);
    if (rc != MDB_SUCCESS) goto open_fail_env;
    
    return 1;
    
open_fail_txn:
    mdb_txn_abort(wtxn);
open_fail_env:
    mdb_env_close(dbenv);
    dbenv = NULL;
open_fail_base:
    THROW_LMDB_ERROR(env, 0, "Open failed", rc);
}

napi_value js_start_transaction(napi_env env, napi_callback_info info) {
    // Auto-open the database if needed
    if (!dbenv && !open_lmdb(env, NULL)) return NULL; // Initialization failed, error already thrown
    
    ltxn_t *ltxn = allocate_ltxn();
    if (!ltxn) {
        THROW_DB_ERROR(env, NULL, "TXN_LIMIT", "Transaction limit reached - no available transaction slots (max: %d)", MAX_LTXNS);
    }

    assign_rtxn_wrapper(ltxn);
    
    // Combine index and nonce into transaction ID
    napi_value result;
    ASSERT_OR_RETURN(napi_create_int32(env, ltxn_to_id(ltxn), &result) == napi_ok, NULL);
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
    
    int32_t ltxn_id;
    status = napi_get_value_int32(env, argv[0], &ltxn_id);
    if (status != napi_ok) {
        THROW_TYPE_ERROR(env, NULL, NULL, "Expected transaction ID as integer, got invalid type");
    }
    
    ltxn_t *ltxn = id_to_open_ltxn(env, ltxn_id);
    if (!ltxn) return NULL; // Error already thrown
    
    release_rtxn_wrapper(ltxn);
    
    if (!ltxn->has_writes) {
        // Read-only transaction, commit immediately
        release_ltxn(ltxn);
        napi_value result;
        ASSERT_OR_RETURN(napi_get_boolean(env, false, &result) == napi_ok, NULL);
        return result;
    }
    
    // Transaction has writes, prepare for async processing
    ltxn->state = TRANSACTION_COMMITTING;
    ltxn->env = env;
    
    // Store callback reference
    napi_valuetype valuetype;
    if (argc > 1 && napi_typeof(env, argv[1], &valuetype) == napi_ok && valuetype == napi_function) {
        status = napi_create_reference(env, argv[1], 1, &ltxn->callback_ref);
        if (status != napi_ok) {
            LOG_INTERNAL_ERROR("Failed to create callback reference for transaction %d", ltxn_id);
            ltxn->callback_ref = NULL;
        }
    } else {
        ltxn->callback_ref = NULL; // No callback provided
    }
    
    // Add transaction to the queue
    ltxn->next = NULL;
    if (last_queued_ltxn) {
        last_queued_ltxn->next = ltxn;
    } else {
        first_queued_ltxn = ltxn;
    }
    last_queued_ltxn = ltxn;
    
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
    
    int32_t ltxn_id;
    status = napi_get_value_int32(env, argv[0], &ltxn_id);
    if (status != napi_ok) {
        THROW_TYPE_ERROR(env, NULL, NULL, "Expected transaction ID as integer");
    }
    
    ltxn_t *ltxn = id_to_open_ltxn(env, ltxn_id);
    if (!ltxn) return NULL; // Error already thrown
    
    release_rtxn_wrapper(ltxn);
    release_ltxn(ltxn);
    
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
    
    int32_t ltxn_id;
    status = napi_get_value_int32(env, argv[0], &ltxn_id);
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
    
    ltxn_t *ltxn = id_to_open_ltxn(env, ltxn_id);
    if (!ltxn) return NULL; // Error already thrown
    
    // First check if we have any PUT/DEL operations for this key in our buffer
    update_log_t *update_log = find_update_log(ltxn, key_size, key_data, 0);
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
    int rc = mdb_get(ltxn->rtxn_wrapper->rtxn, dbi, &key, &value);
    
    if (rc == MDB_SUCCESS || rc == MDB_NOTFOUND) {
        read_log_t *read_log = create_read_log(ltxn, sizeof(get_log_t) + key_size, 0);
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
        THROW_LMDB_ERROR(env, NULL, "Get failed", rc);
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
    
    int32_t ltxn_id;
    status = napi_get_value_int32(env, argv[0], &ltxn_id);
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
    
    ltxn_t *ltxn = id_to_open_ltxn(env, ltxn_id);
    if (!ltxn) return NULL; // Error already thrown
    
    add_update_log(ltxn, key_size, key_data, value_size, value_data);
    ltxn->has_writes = 1;
    
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
    
    int32_t ltxn_id;
    status = napi_get_value_int32(env, argv[0], &ltxn_id);
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
    
    ltxn_t *ltxn = id_to_open_ltxn(env, ltxn_id);
    if (!ltxn) return NULL; // Error already thrown
    
    add_update_log(ltxn, key_size, key_data, 0, NULL);
    ltxn->has_writes = 1;
    
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
    
    int32_t ltxn_id;
    if (napi_get_value_int32(env, argv[0], &ltxn_id) != napi_ok) {
        THROW_TYPE_ERROR(env, NULL, NULL, "Expected transaction ID as integer");
    }
    
    // Fetch the transaction
    ltxn_t *ltxn = id_to_open_ltxn(env, ltxn_id);
    if (!ltxn) return NULL; // Error already thrown
    
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
    
    read_log_t *read_log = create_read_log(ltxn, sizeof(iterate_log_t) + end_key_size, reverse ? 1|2 : 1);
    ASSERT_OR_RETURN(read_log != NULL, NULL);
    
    iterator_t *it = allocate_iterator(ltxn);
    ASSERT_OR_RETURN(it != NULL, NULL);
    
    it->end_key_size = (uint16_t)end_key_size;
    it->end_key_data = end_key_data;
    it->next = ltxn->first_iterator;
    ltxn->first_iterator = it;    
    it->iterate_log = &read_log->iterate;
    
    // Tag in iterate_log->tagged_next_ptr can be left at 0
    it->iterate_log->key_size = start_key_size;
    it->iterate_log->row_count = 0; // Reset row count for this iterator
    if (start_key_size > 0) {
        memcpy(it->iterate_log->key_data, start_key_data, start_key_size);
    }
    
    if (start_key_size > 0) {
        it->current_update_log = find_update_log(ltxn, start_key_size, start_key_data, reverse ? 2 : 1);
        it->lmdb_key.mv_data = (char *)start_key_data; // non-const, but LMDB doesn't modify it
        it->lmdb_key.mv_size = start_key_size;
    } else {
        // start at the first/last record
        it->current_update_log = reverse ? find_last_update_log(ltxn) : ltxn->update_log_skiplist_ptrs[0];
        it->lmdb_key.mv_data = NULL;
        it->lmdb_key.mv_size = 0;
    }
    
    int rc = place_cursor(it->cursor, &it->lmdb_key, &it->lmdb_value, reverse);
    if (rc != MDB_SUCCESS) {
        THROW_LMDB_ERROR(env, NULL, "Place cursor failed", rc);
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
    
    ltxn_t *ltxn = id_to_open_ltxn(env, it->ltxn_id);
    if (!ltxn) return NULL; // Error already thrown
    
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
        int rc = mdb_cursor_get(it->cursor, &it->lmdb_key, &it->lmdb_value, reverse ? MDB_PREV : MDB_NEXT);
        if (rc == MDB_NOTFOUND) {
            it->lmdb_key.mv_data = it->lmdb_value.mv_data = NULL; // mark cursor as at the end of the database
            it->lmdb_key.mv_size = it->lmdb_value.mv_size = 0;
        } else if (rc != MDB_SUCCESS) {
            THROW_LMDB_ERROR(env, NULL, "Cursor next failed", rc);
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
    
    ltxn_t *ltxn = id_to_open_ltxn(env, iterator->ltxn_id);
    if (!ltxn) return NULL; // Error already thrown
    
    // Remove iterator from the transaction's linked list    
    if (iterator == ltxn->first_iterator) {
        ltxn->first_iterator = iterator->next;
    } else {
        iterator_t *prev = ltxn->first_iterator;
        while (prev) {
            if (prev->next == iterator) {
                prev->next = iterator->next; // Remove from list
                break;
            }
            prev = prev->next;
        }
    }
    
    iterator->ltxn_id = -1;
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