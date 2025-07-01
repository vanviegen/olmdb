#ifndef LOWLEVEL_INTERNAL_H
#define LOWLEVEL_INTERNAL_H

#include <stdint.h>
#include "lmdb.h"
#include "lowlevel.h"

#define SKIPLIST_DEPTH 4 // Number of levels in the skiplist
#define LOG_BUFFER_BLOCK_SIZE (64 * 1024)

// For use by our checksum hash function (simple FNV-1a)
#define CHECKSUM_INITIAL 0xcbf29ce484222325ULL
#define CHECKSUM_PRIME 0x100000001b3ULL

// Transaction states
#define TRANSACTION_FREE 0
#define TRANSACTION_OPEN 1
#define TRANSACTION_COMMITTING 2
#define TRANSACTION_RACED 3
#define TRANSACTION_SUCCEEDED 4
#define TRANSACTION_FAILED 5

extern int log_fd;

#define LOG(fmt, ...) \
    do { \
        if (log_fd == -1) { \
            dprintf(2, "LMDB: " fmt "\n", ##__VA_ARGS__); \
        } else if (log_fd >= 0) { \
            dprintf(log_fd, fmt "\n", ##__VA_ARGS__); \
        } \
    } while (0)

#define LOG_INTERNAL_ERROR(message, ...) \
    LOG("%s:%d " message, __FILE__, __LINE__, ##__VA_ARGS__)

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

typedef struct {
    char type; // 'i' for init
    uintptr_t mmap_ptr; // Shared memory base within js process
    size_t mmap_size;
    // The mmap fd is sent as ancillary data
} init_command_t;

typedef struct {
    char type; // 'c' for commit
    struct ltxn_struct *ltxn; // Js process-relative pointer to the ltxn_t structure
} commit_command_t;

typedef struct read_log_struct {
    struct read_log_struct *next_ptr;
    uint64_t checksum;
    int32_t row_count; // 0 for just a get, >0 for forward iteration, <0 for backward iteration
    uint16_t key_size;
    char key_data[];  // Variable length
} __attribute__((aligned(8))) read_log_t;

typedef struct update_log_struct {
    struct update_log_struct *next_ptrs[SKIPLIST_DEPTH];
    struct update_log_struct *prev_ptr;
    uint32_t value_size;
    uint16_t key_size;
    char data[]; // key data + value data
} __attribute__((aligned(4))) update_log_t;


// Buffer structure using flexible array member
typedef struct log_buffer_struct {
    struct log_buffer_struct *next;
    uint16_t blocks; // Allocation size in blocks of LOG_BUFFER_BLOCK_SIZE (including this header)
    char free; // 1 if this buffer is free, 0 if it is in use
    char data[];
} log_buffer_t;

typedef struct rtxn_wrapper_struct {
    MDB_txn *rtxn;
    union {
        int ref_count;
        struct rtxn_wrapper_struct *next_free; // Free list
    };
} rtxn_wrapper_t;

// Transaction structure
typedef struct ltxn_struct {
    uint16_t nonce;
    uint8_t state; // TRANSACTION_*
    uint8_t has_writes;

    // Log buffer indices, point to memory within the shared memory area
    log_buffer_t *first_log_buffer;
    char *log_write_ptr; // Next write pos within current_buffer
    char *log_end_ptr; // End of current buffer, used to check if we need to allocate a new buffer
    read_log_t *first_read_log; // Chronologically first read entry
    read_log_t *last_read_log; // Chronologically last read entry
    update_log_t *update_log_skiplist_ptrs[SKIPLIST_DEPTH]; // Skiplist for write entries, 4 pointers for 4 levels

    // These point to memory outside the shared memory area
    rtxn_wrapper_t *rtxn_wrapper; // Read-only transaction wrapper
    struct iterator_struct *first_iterator; // Linked list of iterators for this logical transaction
    struct ltxn_struct *next; // Next transaction, in commit list or in free list
} ltxn_t;

typedef struct iterator_struct {
    struct iterator_struct *next; // Next iterator in the free list, or within the transaction
    MDB_cursor *cursor;
    MDB_val lmdb_key; // when mv_data is NULL, it means the cursor is at the end of the database
    MDB_val lmdb_value;
    update_log_t *current_update_log; // Current position within uncommitted records
    read_log_t *iterate_log; // Log entry about this iterator
    const char *end_key_data; // If set, the cursor will stop at this key
    int ltxn_id; // Set to -1 when free
    uint16_t end_key_size;
    uint16_t nonce;
} iterator_t;

// Shared functions and data structures

extern MDB_env *dbenv;
extern MDB_dbi dbi;
int init_lmdb();
int place_cursor(MDB_cursor *cursor, MDB_val *key, MDB_val *value, int reverse);
uint64_t checksum(const char *data, size_t len, uint64_t val);

#endif // LOWLEVEL_INTERNAL_H
