/**
 * See the architecture section in README.md before delving into this code.
 */

#define _GNU_SOURCE // For memfd_create
#include <errno.h>
#include <fcntl.h>
#include <linux/limits.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <sys/mman.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#ifdef _WIN32
    #include <windows.h>
    #include <sys/timeb.h>
#else
    #include <sys/time.h>
#endif

#include "common.h"
#include "transaction_client.h"


// Configuration constants
#define SHARED_MEMORY_SIZE (4ULL * 1024 * 1024 * 1024) // 4 GB shared memory size
#define LOG_BUFFER_BLOCK_SIZE (64 * 1024)
#define MAX_LTXNS 0x100000
#define MAX_ITERATORS 0x100000
#define MAX_KEY_LENGTH 511
#define SKIPLIST_DEPTH 4
#define RTXN_SPAN_TIME_MS 100 // Read transactions are shared for 100 ms
#define MAX_RTXNS 8 // At most 8 read transactions can be active at the same time

// Macro's

// Get the pointer to a struct from a pointer to one of its members
#define CONTAINER_OF(ptr, type, member) ((type *)((char *)(ptr) - offsetof(type, member)))

#define LOG(fmt, ...) \
    do { \
        fprintf(stderr, "OLMDB: " fmt "\n", ##__VA_ARGS__); \
    } while (0)

#define SET_ERROR(code_str, msg, ...) \
    do { \
        snprintf(error_code, sizeof(error_code), "%s", code_str); \
        snprintf(error_message, sizeof(error_message), msg, ##__VA_ARGS__); \
    } while(0)


// We must include this instead of linking it, because of the error handling and logging macros
#include "common.c"


// Global state

// Transactions
static ltxn_t *ltxns; // This will be allocated within shared memory
static ltxn_t *first_free_ltxn = NULL; // Start of linked-list of free transactions
static int next_unused_ltxn = 0; // Index of next never-used transaction
static ltxn_t *ltxn_commit_queue_head = NULL; // List of ltxns handed to the commit worker for processing

// Log buffers
static log_buffer_t *first_free_log_buffer = NULL; // Start of linked-list of free single-block buffers

// Iterators
static iterator_t iterators[MAX_ITERATORS]; // This is quite large, but Linux will allocate pages lazily
static iterator_t *first_free_iterator = NULL; // Start of linked-list of free iterators
static int next_unused_iterator = 0; // Index of next never-used iterator

// Read transaction wrappers
static rtxn_wrapper_t rtxn_wrappers[MAX_RTXNS];
static rtxn_wrapper_t *first_free_rtxn_wrapper = NULL;
static int next_unused_rtxn_wrapper = 0;
static rtxn_wrapper_t *current_rtxn_wrapper = NULL;
static long long current_rtxn_expire_time = 0;

// Random number generator
static uint64_t rng_state = 0;

// Worker communication
static char *shared_memory;
static char *shared_memory_unused_start;
static char *shared_memory_unused_end;
static int commit_worker_fd = -1;
static int mmap_fd = -1;
char db_dir[PATH_MAX];
char *commit_worker_bin;
void (*set_signal_fd_callback)(int fd) = NULL;

// Prototypes for internal functions
static void *allocate_log_space(ltxn_t *ltxn, size_t needed_space);
static void release_ltxn(ltxn_t *ltxn);
static ltxn_t *allocate_ltxn();
static ltxn_t *id_to_open_ltxn(int transaction_id);
static int ltxn_to_id(ltxn_t *ltxn);
static update_log_t *find_update_log(ltxn_t *ltxn, uint16_t key_size, const char *key_data, int allow_before);
static void delete_update_log(ltxn_t *ltxn, update_log_t *log);
static uint32_t get_random_number();

// Error state
char error_message[2048];
char error_code[32];

// Inline functions

static int ltxn_to_id(ltxn_t *ltxn) {
    int idx = ltxn - ltxns;
    ASSERT(idx >= 0 && idx < MAX_LTXNS);
    return (idx << 12) | (ltxn->nonce & 0xfff); // Use the lower 12 bits for nonce
}

static uint32_t get_random_number() {
    if (rng_state == 0) rng_state = (uint64_t)time(NULL);
    rng_state ^= rng_state << 13;
    rng_state ^= rng_state >> 7;
    rng_state ^= rng_state << 17;
    return (uint32_t)rng_state;
}

static long long get_time_ms() {
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

log_buffer_t *allocate_shared_memory_blocks(uint32_t blocks) {
    if (blocks == 1) {
        // Special case for single block allocation
        if (first_free_log_buffer) {
            // Take a (recently) freed buffer from the free list
            log_buffer_t *new_buf = first_free_log_buffer;
            first_free_log_buffer = first_free_log_buffer->next;
            new_buf->next = 0;
            return new_buf;
        } else {
            // Allocate a new single block buffer at the end of the shared memory
            shared_memory_unused_end -= LOG_BUFFER_BLOCK_SIZE;
            log_buffer_t *new_buf = (log_buffer_t *)shared_memory_unused_end;
            new_buf->blocks = 1;
            new_buf->free = 0;
            return new_buf;
        }
    }

    // Scan all multi-page allocations from the start of the buffer until we find a free one
    // that is large enough
    log_buffer_t *current = (log_buffer_t *)shared_memory;
    while((char *)current < shared_memory_unused_start && current->blocks > 0) {
        log_buffer_t *next = (log_buffer_t *)((char *)current + LOG_BUFFER_BLOCK_SIZE * current->blocks);
        if (current->free) {
            // Merge with subsequent buffers if they're also free
            while ((char *)next < shared_memory_unused_start && next->blocks > 0 && next->free) {
                current->blocks += next->blocks;
                next = (log_buffer_t *)((char *)current + LOG_BUFFER_BLOCK_SIZE * current->blocks);
            }
            if (current->blocks >= blocks) {
                // We can use this buffer
                current->free = 0; // Mark as in use
                if (blocks < current->blocks) {
                    // If we have more blocks than needed, split the buffer
                    log_buffer_t *next_buf = (log_buffer_t *)((char *)current + LOG_BUFFER_BLOCK_SIZE * blocks);
                    next_buf->free = 1;
                    next_buf->blocks = current->blocks - blocks;
                    current->blocks = blocks;
                }
                return current;
            }
        }
        current = next;
    }
    // No suitable free buffer found, allocate a new one at the start of the unused memory area
    log_buffer_t *new_buf = (log_buffer_t *)shared_memory_unused_start;
    shared_memory_unused_start += LOG_BUFFER_BLOCK_SIZE * blocks;
    if (shared_memory_unused_start > shared_memory_unused_end) {
        SET_ERROR("OOM", "Not enough shared memory for %u blocks of size %d bytes", blocks, LOG_BUFFER_BLOCK_SIZE);
        return NULL;
    }
    new_buf->blocks = blocks;
    new_buf->free = 0;
    return new_buf;
}

void release_shared_memory_blocks(log_buffer_t *buf) {
    if (buf->blocks == 1) {
        // Single block buffer, just return it to the free list
        buf->next = first_free_log_buffer;
        first_free_log_buffer = buf;
        // Never marked as free, as we don't want to reuse it in multi-block allocations
        return;
    }

    // Multi-block buffer, just mark it as free
    buf->next = NULL;
    buf->free = 1;
}

// Ensure buffer has enough space, growing if necessary
static void *allocate_log_space(ltxn_t *ltxn, size_t needed_space) {
    // Align buffer position to 8 bytes
    ltxn->log_write_ptr = (char *)(((uintptr_t)ltxn->log_write_ptr + 7) & ~7);
    if (!ltxn->first_log_buffer || ltxn->log_write_ptr + needed_space > ltxn->log_end_ptr) {
        uint32_t blocks = (needed_space + 7 /* for alignment */ + sizeof(log_buffer_t) + LOG_BUFFER_BLOCK_SIZE - 1) / LOG_BUFFER_BLOCK_SIZE;
        
        log_buffer_t *new_buf = allocate_shared_memory_blocks(blocks);

        new_buf->next = ltxn->first_log_buffer;
        ltxn->first_log_buffer = new_buf;
        
        // Align buffer position to 8 bytes
        ltxn->log_write_ptr = (char *)(((uintptr_t)new_buf->data + 7) & ~7);
        ltxn->log_end_ptr = (char *)new_buf + blocks * LOG_BUFFER_BLOCK_SIZE;
    }
    
    void *result = ltxn->log_write_ptr;
    ltxn->log_write_ptr += needed_space;
    return result;
}

static read_log_t *create_read_log(ltxn_t *ltxn, size_t size, int32_t row_count) {    
    read_log_t *read_log = allocate_log_space(ltxn, size);
    ASSERT_OR_RETURN(read_log != NULL, NULL);
    
    // Initialize the read log entry
    read_log->next_ptr = NULL;
    read_log->row_count = row_count;
    
    if (ltxn->last_read_log) {
        ltxn->last_read_log->next_ptr = read_log;
    } else {
        ltxn->first_read_log = read_log;
    }
    ltxn->last_read_log = read_log;
    return read_log;
}

static void release_ltxn_logs(ltxn_t *ltxn) {
    // Free all except the first log buffer (which is always LOG_BUFFER_BLOCK_SIZE bytes)
    log_buffer_t *lb = ltxn->first_log_buffer;
    ltxn->first_log_buffer = NULL;
    while(lb) {
        log_buffer_t *next = lb->next;
        release_shared_memory_blocks(lb);
        lb = next;
    }
    
    // Reset transaction state
    ltxn->first_read_log = NULL;
    ltxn->last_read_log = NULL;
    for (int i = 0; i < SKIPLIST_DEPTH; i++) {
        ltxn->update_log_skiplist_ptrs[i] = NULL;
    }
    
}

static void release_ltxn_iterators(ltxn_t *ltxn) {
    iterator_t *it = ltxn->first_iterator;
    while(it) {
        it->ltxn_id = -1;
        iterator_t *next = it->next;
        it->next = first_free_iterator;
        first_free_iterator = it;
        it = next;
    }
    ltxn->first_iterator = NULL;
}

// Reset transaction state and put it back into the free list
static void release_ltxn(ltxn_t *ltxn) {        
    release_ltxn_logs(ltxn);
    release_ltxn_iterators(ltxn);
    
    ltxn->has_writes = 0;
    
    // Return transaction to free list
    ltxn->state = TRANSACTION_FREE;
    ltxn->next = first_free_ltxn;
    first_free_ltxn = ltxn;
}

static ltxn_t *id_to_open_ltxn(int ltxn_id) {
    int idx = ltxn_id >> 12;
    uint16_t nonce = ltxn_id & 0xfff;
    
    if (idx < 0 || idx >= MAX_LTXNS) {
        SET_ERROR("INVALID_TRANSACTION", "Transaction index %d not within valid range 0..%d", idx, MAX_LTXNS - 1);
        return NULL;
    }
    
    ltxn_t *ltxn = &ltxns[idx];
    if (ltxn->state != TRANSACTION_OPEN || ltxn->nonce != nonce) {
        SET_ERROR("INVALID_TRANSACTION", "Transaction ID %d not found or already closed (index=%d, nonce=%u, state=%d, expected_nonce=%u)", 
                  ltxn_id, idx, nonce, ltxn->state, ltxn->nonce);
        return NULL;
    }
    
    return ltxn;
}

static void assign_rtxn_wrapper(ltxn_t *ltxn) {
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
        int rc = mdb_txn_begin(dbenv, NULL, MDB_RDONLY, &rtxn_wrapper->rtxn);
        if (rc != MDB_SUCCESS) {
            LOG_INTERNAL_ERROR("Failed to create read transaction (%s)", mdb_strerror(rc));
            next_unused_rtxn_wrapper--;
            ltxn->rtxn_wrapper = current_rtxn_wrapper; // do *something*?!
            return;
        }
    }
    rtxn_wrapper->ref_count = 1;
    // Our read-only commit seq will be 1 lower than the next read/write transaction to commit
    rtxn_wrapper->commit_seq = (mdb_txn_id(rtxn_wrapper->rtxn)+1) * MAX_BATCHED_COMMITS;
    current_rtxn_wrapper = rtxn_wrapper;
    current_rtxn_expire_time = time + RTXN_SPAN_TIME_MS; // Share this read transaction with all logical transactions started within the next 100 ms
    ltxn->rtxn_wrapper = rtxn_wrapper;
}

static void release_rtxn_wrapper(ltxn_t *ltxn) {
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
        SET_ERROR("TXN_LIMIT", "Transaction limit reached - no available transaction slots (max: %d)", MAX_LTXNS);
        return NULL;
    }
    ltxn->nonce = (ltxn->nonce + 1) & 0xfff; // Prevent rapid reuse of transaction IDs
    ltxn->state = TRANSACTION_OPEN;
    return ltxn;
}

static iterator_t *id_to_open_iterator(int iterator_id) {
    int idx = iterator_id >> 12;
    uint16_t nonce = iterator_id & 0xfff;
    
    if (idx >= MAX_ITERATORS) {
        SET_ERROR("INVALID_ITERATOR", "Iterator index %d exceeds maximum %d", idx, MAX_ITERATORS - 1);
        return NULL;
    }
    
    iterator_t *iterator = &iterators[idx];
    if (iterator->ltxn_id < 0 || iterator->nonce != nonce) {
        SET_ERROR("INVALID_ITERATOR", "Iterator ID %d not found or already closed (index=%d, nonce=%u, txn_id=%d, expected_nonce=%u)", 
                  iterator_id, idx, nonce, iterator->ltxn_id, iterator->nonce);
        return NULL;
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
        SET_ERROR("INVALID_ITERATOR", "No free iterator slots available (max: %d)", MAX_ITERATORS);
        return NULL;
    }
    iterator->ltxn_id = ltxn_to_id(ltxn);
    iterator->nonce = (iterator->nonce + 1) & 0xfff; // Prevent rapid reuse of iterator IDs
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
    // Align to 4 bytes for update_log_t as per the new alignment requirement
    size = (size + 3) & ~3;
    
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

static int send_fd(int sockfd, int fd)
{
    /* Allocate a char array of suitable size to hold the ancillary data.
       However, since this buffer is in reality a 'struct cmsghdr', use a
       union to ensure that it is aligned as required for that structure.
       Alternatively, we could allocate the buffer using malloc(), which
       returns a buffer that satisfies the strictest alignment requirements
       of any type. However, if we employ that approach, we must ensure
       that we free() the buffer on all return paths from this function. */
    union {
        char   buf[CMSG_SPACE(sizeof(int))];
                        /* Space large enough to hold an 'int' */
        struct cmsghdr align;
    } controlMsg;

    /* The 'msg_name' field can be used to specify the address of the
       destination socket when sending a datagram. However, we do not need
       to use this field because we presume that 'sockfd' is a connected
       socket. */

    struct msghdr msgh;
    msgh.msg_name = NULL;
    msgh.msg_namelen = 0;

    /* On Linux, we must transmit at least one byte of real data in order to
       send ancillary data. We transmit an arbitrary integer whose value is
       ignored by recv_fd(). */

    struct iovec iov;
    int data;

    data = 12345;
    iov.iov_base = &data;
    iov.iov_len = sizeof(int);
    msgh.msg_iov = &iov;
    msgh.msg_iovlen = 1;

    /* Set 'msghdr' fields that describe ancillary data. */

    msgh.msg_control = controlMsg.buf;
    msgh.msg_controllen = sizeof(controlMsg.buf);

    /* Set up ancillary data describing file descriptor to send. */

    struct cmsghdr *cmsgp;
    cmsgp = CMSG_FIRSTHDR(&msgh);
    cmsgp->cmsg_level = SOL_SOCKET;
    cmsgp->cmsg_type = SCM_RIGHTS;
    cmsgp->cmsg_len = CMSG_LEN(sizeof(int));
    memcpy(CMSG_DATA(cmsgp), &fd, sizeof(int));

    /* Send real plus ancillary data. */

    if (sendmsg(sockfd, &msgh, 0) == -1)
        return -1;

    return 0;
}

static void start_commit_worker(int socket_fd, const char *db_dir) {
    pid_t pid = fork();
    if (pid < 0) {
        LOG_INTERNAL_ERROR("Failed to fork server process: %s", strerror(errno));
        return;
    }
    if (pid > 0) {
        // Parent process, wait for the child to exit (as it's doing its second fork to daemonize), and then return
        waitpid(pid, NULL, 0);
        return;
    }

    // Close all file descriptors except the socket
    for (int fd = 0; fd < 1024; fd++) {
        if (fd != socket_fd) {
            close(fd);
        }
    }
    
    if (socket_fd != 0) {
        // We'll pass the socket to the commit worker as stdin
        dup2(socket_fd, 0);
        close(socket_fd);
    }

    execv(commit_worker_bin, (char *[]){"commit_worker", (char *)db_dir, NULL});
    // If execv fails, log the error and exit
    LOG_INTERNAL_ERROR("Failed to exec commit worker: %s", strerror(errno));
    _exit(1);
}

static int connect_to_commit_worker() {
    // LOG("Connecting to commit worker...");
    
    // Get the inode of data.mdb to uniquely identify this database
    char data_mdb_path[PATH_MAX];
    snprintf(data_mdb_path, sizeof(data_mdb_path), "%s/data.mdb", db_dir);
    
    // Ensure directory exists
    mkdir(db_dir, 0755);
    
    // Ensure data.mdb exists (create empty file if needed) and get its inode
    int data_fd = open(data_mdb_path, O_RDWR | O_CREAT, 0644);
    struct stat st;
    if (data_fd < 0 || fstat(data_fd, &st) < 0) {
        if (data_fd >= 0) close(data_fd);
        SET_ERROR("NO_COMMIT_WORKER", "Failed to access data.mdb: %s", strerror(errno));
        return -1;
    }
    close(data_fd);
    
    for(int retry_count = 0; retry_count < 10; retry_count++) {
        int fd = socket(AF_UNIX, SOCK_SEQPACKET, 0);
        if (fd == -1) {
            SET_ERROR("NO_COMMIT_WORKER", "Failed to create socket: %s", strerror(errno));
            return -1;
        }
        
        struct sockaddr_un addr;
        memset(&addr, 0, sizeof(addr));
        addr.sun_family = AF_UNIX;
        // The 0 byte indicates were using the Abstract Socket Namespace.
        // We use device+inode to uniquely identify this database instance.
        // If the database is deleted and recreated, it gets a new inode.
        // We prefer this over an actual socket file, as it avoids issues with stale socket files.
        // Also, we avoid the max 108 bytes path length issue for regular unix sockets.
        // This is Linux-specific though!
        snprintf(addr.sun_path, sizeof(addr.sun_path), "%colmdb-%lu-%lu", 0, (unsigned long)st.st_dev, (unsigned long)st.st_ino);        

        // First try to bind, see if we can become the server
        if (bind(fd, (struct sockaddr*)&addr, sizeof(addr)) == 0) {
            start_commit_worker(fd, db_dir);
            usleep(50000); // 50ms to allow the server to start (if that's not enough the retry will come to the rescue)
            close(fd);
            continue; // Now try to connect to the server
        } else if (errno != EADDRINUSE) {
            LOG_INTERNAL_ERROR("Failed to bind to socket '%s': %s", addr.sun_path, strerror(errno));
        }
        
        // Try to become client instead
        if (connect(fd, (struct sockaddr*)&addr, sizeof(addr)) == -1) {
            LOG_INTERNAL_ERROR("Failed to connect to worker socket: %s", strerror(errno));
            goto delayed_retry_connect;
        }

        init_command_t init_command = {
            .type = 'i',
            .mmap_ptr = (uintptr_t)shared_memory,
            .mmap_size = SHARED_MEMORY_SIZE,
            .pid = getpid(),
        };

        if (send(fd, &init_command, sizeof(init_command), 0) < 0) {
            LOG_INTERNAL_ERROR("Failed to send init command to commit worker: %s", strerror(errno));
            goto delayed_retry_connect;
        }
        if (send_fd(fd, mmap_fd)) {
            LOG_INTERNAL_ERROR("Failed to send shared memory file descriptor to commit worker: %s", strerror(errno));
            goto delayed_retry_connect;
        }
        // LOG("Connected to commit worker");

        if (set_signal_fd_callback) {
            set_signal_fd_callback(fd);
        }
        return fd;
    delayed_retry_connect:
        close(fd);
        usleep(50000 + (get_random_number()%150000)); // 50ms ~ 200ms before trying again
    }

    SET_ERROR("NO_COMMIT_WORKER", "Failed to connect to commit worker after multiple attempts");
    return -1;
}

void reconnect_commit_worker() {
    // If there was a commit queue, we will mark all transactions as raced, so
    // JavaScript will rerun them. In case one of these transactions was causing the
    // problem, hopefully this will resolve it. (And otherwise there will be 
    // a retry limit, so we'll eventually give up.)
    for (ltxn_t *current = ltxn_commit_queue_head; current; current = current->next) {
        if (current->state == TRANSACTION_COMMITTING) {
            current->state = TRANSACTION_RACED;
        }
    }

    close(commit_worker_fd);
    commit_worker_fd = -1;
    commit_worker_fd = connect_to_commit_worker();
}

int init(const char *_db_dir, const char *_commit_worker_bin, void (*set_signal_fd)(int fd)) {
    if (dbenv) {
        SET_ERROR("DUP_INIT", "Database is already init");
        return -1;
    }

    set_signal_fd_callback = set_signal_fd;
    
    if (!_db_dir || !_db_dir[0]) {
        // Read $OLMDB_DIR environment variable or use default './.olmdb'
        _db_dir = getenv("OLMDB_DIR");
        if (!_db_dir || !_db_dir[0]) {
            _db_dir = "./.olmdb"; // Default directory
        }
    }

    // Create database directory if it doesn't exist
    if (mkdir(_db_dir, 0755) != 0 && errno != EEXIST) {
        SET_ERROR("CREATE_DIR_FAILED", "Failed to create/open database directory '%.512s': %s", _db_dir, strerror(errno));
        return -1;
    }

    if (realpath(_db_dir, db_dir)==NULL) {
        SET_ERROR("CREATE_DIR_FAILED", "Failed to resolve database directory '%s': %s", _db_dir, strerror(errno));
        return -1;
    }

    if (access(_commit_worker_bin, X_OK) != 0) {
        SET_ERROR("NO_COMMIT_WORKER", "Commit worker binary '%s' is not executable: %s", _commit_worker_bin, strerror(errno));
        return -1;
    }

    if (init_lmdb(db_dir) < 0) return -1; // Error already set

    mmap_fd = memfd_create("client_shared_mem", 0);
    if (mmap_fd < 0) {
        SET_ERROR("OOM", "Failed to create shared memory: %s", strerror(errno));
        return -1;
    }
    if (ftruncate(mmap_fd, SHARED_MEMORY_SIZE) == -1) {
        SET_ERROR("OOM", "Failed to set size of shared memory: %s", strerror(errno));
        close(mmap_fd);
        return -1;
    }
    shared_memory = mmap(NULL, SHARED_MEMORY_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, mmap_fd, 0);
    if (shared_memory == MAP_FAILED) {
        SET_ERROR("OOM", "Failed to map shared memory: %s", strerror(errno));
        close(mmap_fd);
        shared_memory = NULL;
        return -1;
    }

    shared_memory_unused_start = shared_memory;

    // Allocate ltxns at the end of the shared memory
    ltxns = (ltxn_t *)((uintptr_t)(shared_memory + SHARED_MEMORY_SIZE - sizeof(ltxn_t) * MAX_LTXNS) & ~4095); // Align to 4kb
    shared_memory_unused_end = (char *)ltxns;

    commit_worker_bin = strdup(_commit_worker_bin);
    commit_worker_fd = connect_to_commit_worker();
    if (commit_worker_fd < 0) return -1; // Error already set

    return 0;
}

int start_transaction() {
    if (!dbenv) {
        SET_ERROR("NOT_INIT", "Database is not init");
        return -1;
    }

    ltxn_t *ltxn = allocate_ltxn();
    if (!ltxn) return -1; // Error already set
    
    assign_rtxn_wrapper(ltxn);
    
    // Combine index and nonce into transaction ID
    return ltxn_to_id(ltxn);
}

int put(int ltxn_id, const void *key_data, size_t key_size, const void *value_data, size_t value_size) {
    if (key_size < 1) {
        SET_ERROR("EMPTY_KEY", "Key must be at least 1 byte");
        return -1;
    }
    if (key_size > MAX_KEY_LENGTH) {
        SET_ERROR("KEY_TOO_LONG", "Key size %zu exceeds maximum allowed length %d", 
                  key_size, MAX_KEY_LENGTH);
        return -1;
    }
    
    ltxn_t *ltxn = id_to_open_ltxn(ltxn_id);
    if (!ltxn) return -1; // Error already set
    
    add_update_log(ltxn, key_size, key_data, value_size, value_data);
    ltxn->has_writes = 1;
    
    return 0;
}

int del(int ltxn_id, const void *key_data, size_t key_size) {
    if (key_size < 1) {
        SET_ERROR("EMPTY_KEY", "Key must be at least 1 byte");
        return -1;
    }
    if (key_size > MAX_KEY_LENGTH) {
        SET_ERROR("KEY_TOO_LONG", "Key size %zu exceeds maximum allowed length %d", 
                  key_size, MAX_KEY_LENGTH);
        return -1;
    }
    
    ltxn_t *ltxn = id_to_open_ltxn(ltxn_id);
    if (!ltxn) return -1; // Error already set
    
    add_update_log(ltxn, key_size, key_data, 0, NULL);
    ltxn->has_writes = 1;
    
    return 0;
}

int get(int ltxn_id, const void *key_data, size_t key_size, void **value_data, size_t *value_size) {
    if (key_size < 1) {
        SET_ERROR("EMPTY_KEY", "Key must be at least 1 byte");
        return -1;
    }
    if (key_size > MAX_KEY_LENGTH) {
        SET_ERROR("KEY_TOO_LONG", "Key size %zu exceeds maximum allowed length %d", 
                  key_size, MAX_KEY_LENGTH);
        return -1;
    }
    
    ltxn_t *ltxn = id_to_open_ltxn(ltxn_id);
    if (!ltxn) return -1; // Error already set
    
    // First check if we have any PUT/DEL operations for this key in our buffer
    update_log_t *update_log = find_update_log(ltxn, key_size, key_data, 0);
    if (update_log) {
        // Found in buffer, return the value
        if (update_log->value_size == 0) {
            // It's a delete operation
            *value_data = NULL;
            *value_size = 0;
            return 0; // Key not found (was deleted)
        }
        
        *value_data = update_log->data + update_log->key_size;
        *value_size = update_log->value_size;
        return 1;
    }
    
    // Not found in buffer, do LMDB lookup
    MDB_val key, value;
    key.mv_data = (void*)key_data;
    key.mv_size = key_size;
    ASSERT_OR_RETURN(ltxn->rtxn_wrapper->rtxn != NULL, -1);
    int rc = mdb_get(ltxn->rtxn_wrapper->rtxn, dbi, &key, &value);

    if (rc == MDB_SUCCESS || rc == MDB_NOTFOUND) {
        read_log_t *read_log = create_read_log(ltxn, sizeof(read_log_t) + key_size, 0);
        ASSERT(read_log);
        if (read_log) {
            read_log->checksum = (rc == MDB_NOTFOUND) ? 0 : checksum(value.mv_data, value.mv_size, CHECKSUM_INITIAL);
            read_log->key_size = key_size;
            memcpy(read_log->key_data, key_data, key_size);
        }
    }

    if (rc == EINVAL) {
        SET_ERROR("EINVAL", "Key length %zu, key '%.*s', first char '%c'", key_size, (int)key_size, (char*)key_data, ((char *)key_data)[0]);
        return -1; // Invalid key size
    }
    
    if (rc == MDB_NOTFOUND) {
        *value_data = NULL;
        *value_size = 0;
        return 0; // Key not found
    }
    
    if (rc != MDB_SUCCESS) {
        SET_LMDB_ERROR("mdb_get", rc);
        return -1; // Error occurred
    }
    
    *value_data = value.mv_data;
    *value_size = value.mv_size;
    return 1;
}

int create_iterator(int ltxn_id, 
                         const void *start_key_data, size_t start_key_size, 
                         const void *end_key_data, size_t end_key_size, 
                         int reverse) {
    if (start_key_size > MAX_KEY_LENGTH) {
        SET_ERROR("KEY_TOO_LONG", "Start key size %zu exceeds maximum allowed length %d", 
                  start_key_size, MAX_KEY_LENGTH);
        return -1;
    }
    
    if (end_key_size > MAX_KEY_LENGTH) {
        SET_ERROR("KEY_TOO_LONG", "End key size %zu exceeds maximum allowed length %d", 
                  end_key_size, MAX_KEY_LENGTH);
        return -1;
    }
    
    // Fetch the transaction
    ltxn_t *ltxn = id_to_open_ltxn(ltxn_id);
    if (!ltxn) return -1; // Error already set

    iterator_t *it = allocate_iterator(ltxn);
    if (!it) return -1; // Error already set

    it->end_key_size = (uint16_t)end_key_size;
    it->end_key_data = end_key_data;
    it->next = ltxn->first_iterator;
    ltxn->first_iterator = it;    

    if (start_key_size > 0) {
        it->current_update_log = find_update_log(ltxn, start_key_size, start_key_data, reverse ? 2 : 1);
        it->lmdb_key.mv_data = (void*)start_key_data; // non-const, but LMDB doesn't modify it
        it->lmdb_key.mv_size = start_key_size;
    } else {
        // start at the first/last record
        it->current_update_log = reverse ? find_last_update_log(ltxn) : ltxn->update_log_skiplist_ptrs[0];
        it->lmdb_key.mv_data = NULL;
        it->lmdb_key.mv_size = 0;
    }
    
    int rc = place_cursor(it->cursor, &it->lmdb_key, &it->lmdb_value, reverse);
    if (rc != MDB_SUCCESS) {
        SET_LMDB_ERROR("place_cursor", rc);
        return -1;
    }

    // Create read log with row_count indicating direction (positive for forward, negative for backward)
    read_log_t *read_log = create_read_log(ltxn, sizeof(read_log_t) + start_key_size, reverse ? -1 : 1);
    if (!read_log) {
        SET_ERROR("OOM", "Failed to allocate memory for read log");
        return -1;
    }
        
    read_log->key_size = start_key_size;
    if (start_key_size > 0) {
        memcpy(read_log->key_data, start_key_data, start_key_size);
    }   
    it->iterate_log = read_log;
    
    uint64_t cs = CHECKSUM_INITIAL;
    cs = checksum(it->lmdb_key.mv_data, it->lmdb_key.mv_size, cs);
    cs = checksum(it->lmdb_value.mv_data, it->lmdb_value.mv_size, cs);
    read_log->checksum = cs;
    
    return iterator_to_id(it);
}

int read_iterator(int iterator_id, void **key_data, size_t *key_size, void **value_data, size_t *value_size) {
    iterator_t *it = id_to_open_iterator(iterator_id);
    if (!it) return -1; // Error already set
    
    ltxn_t *ltxn = id_to_open_ltxn(it->ltxn_id);
    if (!ltxn) return -1; // Error already set
    
    // The reverse flag is determined by the sign of row_count in the iterate_log
    int reverse = it->iterate_log->row_count < 0;
    
restart_read_iterator:
    int cmp;
    if (it->current_update_log) {
        if (it->lmdb_key.mv_data) {
            // Both are present.. we need to merge!
            cmp = compare_keys(it->lmdb_key.mv_size, (const char*)it->lmdb_key.mv_data, 
                               it->current_update_log->key_size, it->current_update_log->data);
            // When equal (0): take from both, but return update_log value
            if (reverse) cmp = -cmp; // Reverse comparison for backwards iteration
        } else {
            cmp = +1; // Take from update_log
        }
    } else {
        if (it->lmdb_key.mv_data) {
            cmp = -1; // Take from lmdb
        } else { // No more items to read
            *key_data = NULL;
            *key_size = 0;
            *value_data = NULL;
            *value_size = 0;
            return 0; // No more items
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
            SET_LMDB_ERROR("mdb_cursor_get next", rc);
            return -1;
        }
        
        uint64_t cs = it->iterate_log->checksum;
        cs = checksum((const char*)it->lmdb_key.mv_data, it->lmdb_key.mv_size, cs);
        cs = checksum((const char*)it->lmdb_value.mv_data, it->lmdb_value.mv_size, cs);
        it->iterate_log->checksum = cs;
        
        // Update row count: increment for forward, decrement for reverse
        it->iterate_log->row_count += reverse ? -1 : 1;
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
            // We're at or past end key. Mark iterator as done and return NULL.
            it->current_update_log = NULL; // Mark log iterator as done
            it->lmdb_key.mv_data = NULL; // Mark LMDB cursor as done
            it->lmdb_key.mv_size = 0;
            *key_data = NULL;
            *key_size = 0;
            *value_data = NULL;
            *value_size = 0;
            return 0; // No more items
        }
    }
    
    *key_data = key.mv_data;
    *key_size = key.mv_size;
    *value_data = val.mv_data;
    *value_size = val.mv_size;
    return 1;
}

int close_iterator(int iterator_id) {
    // Fetch the iterator
    iterator_t *iterator = id_to_open_iterator(iterator_id);
    if (!iterator) return -1; // Error already set
    
    ltxn_t *ltxn = id_to_open_ltxn(iterator->ltxn_id);
    if (!ltxn) return -1; // Error already set
    
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
    
    return 0;
}

size_t commit_transaction(int ltxn_id) {
    ltxn_t *ltxn = id_to_open_ltxn(ltxn_id);
    if (!ltxn) return -1; // Error already set

    size_t commit_seq = ltxn->rtxn_wrapper->commit_seq;

    release_rtxn_wrapper(ltxn);
    
    if (!ltxn->has_writes) {
        // Read-only transaction, commit immediately
        release_ltxn(ltxn);
        return commit_seq; // No async work needed
    }

    // Allow the read iterators to be recycled
    release_ltxn_iterators(ltxn);
    
    // Transaction has writes, prepare for async processing
    ltxn->state = TRANSACTION_COMMITTING;
    
    // Add transaction to the queue
    ltxn->next = ltxn_commit_queue_head;
    ltxn_commit_queue_head = ltxn;

    // Notify the commit worker that there's work to do
    commit_command_t commit_command = {
        .type = 'c',
        .ltxn = ltxn
    };
    if (send(commit_worker_fd, &commit_command, sizeof(commit_command), 0) < 0) {
        LOG_INTERNAL_ERROR("Failed to send commit command: %s", strerror(errno));
        reconnect_commit_worker();
    }

    return 0; // Async work queued
}

int drain_signal_fd(int blocking) {
    int result = 0;
    while (1) {
        char d;
        int rc = recv(commit_worker_fd, &d, 1, (blocking && !result) ? 0 : MSG_DONTWAIT);
        if (rc < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break; // Drained!
            if (errno == EINTR) continue; // Interrupted, try again
            LOG("Error receiving data from commit worker: %s", strerror(errno));
        }
        else if (rc > 0) {
            result = 1;
            continue; // Data received
        }
        // An error or EOF
        reconnect_commit_worker();
        if (commit_worker_fd < 0) break;
    }
    return result;
}

int get_commit_results(commit_result_t *results, int *result_count) {
    ltxn_t *new_head = NULL;
    ltxn_t *ltxn = ltxn_commit_queue_head;

    int max_results = *result_count;
    int index = 0;
    int return_value = 0;
    while (ltxn) {
        ltxn_t *next = ltxn->next;
        if (index >= max_results) {
            // Output buffer full, keep in queue
            ltxn->next = new_head;
            new_head = ltxn;
            return_value = 2;
        } else if (ltxn->state == TRANSACTION_COMMITTING) {
            // Not done yet, keep in queue
            ltxn->next = new_head;
            new_head = ltxn;
            return_value = max(return_value, 1);
        } else {
            results[index].ltxn_id = ltxn_to_id(ltxn);
            results[index].commit_seq = (ltxn->state == TRANSACTION_SUCCEEDED) ? ltxn->commit_seq : 0;
            index++;
            release_ltxn(ltxn);
        }
        ltxn = next;
    }
    ltxn_commit_queue_head = new_head;
    *result_count = index;

    return return_value;
}

int abort_transaction(int ltxn_id) {
    ltxn_t *ltxn = id_to_open_ltxn(ltxn_id);
    if (!ltxn) return -1; // Error already set
    
    release_rtxn_wrapper(ltxn);
    release_ltxn(ltxn);
    
    return 0;
}
