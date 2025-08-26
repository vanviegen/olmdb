#ifndef TRANSACTION_CLIENT_H
#define TRANSACTION_CLIENT_H

#include <stdint.h>
#include <stddef.h>

/**
 * @struct commit_result_t
 * @brief Structure containing the result of a transaction commit operation.
 * 
 * @field commit_seq  0 for race/failure, commit sequence number for success.
 * @field ltxn_id     The transaction ID that was committed.
 */
typedef struct {
    size_t commit_seq;
    int ltxn_id;
} commit_result_t;

/**
 * @brief Initialize the database with the specified directory.
 *
 * @param db_dir Path to the database directory, or NULL to use environment variable OLMDB_DIR or default "./.olmdb". Data from this pointer will be copied.
 * @param commit_worker_bin Path to the commit worker binary. Usually `<base_dir>/build/release/commit_worker`. Data from this pointer will be copied.
 * @param set_signal_fd Optional callback function to set and change the file descriptor for the commit worker connection.
 *   When data becomes available on this file descriptor, that's a signal that get_commit_results() should be called.
 * 
 * @return 0 on success.
 *         -1 on failure and sets error_code/error_message.
 * 
 * @error DUP_INIT           Database is already initialized.
 * @error DIR_TOO_LONG       Database directory path exceeds maximum length.
 * @error CREATE_DIR_FAILED  Failed to create or open database directory.
 * @error OOM                Failed to allocate shared memory.
 * @error NO_COMMIT_WORKER          Failed to connect to commit worker after multiple attempts (see log file in db dir).
 * @error LMDB*              Various LMDB errors (where * is the negative LMDB error code).
 */
int init(const char *_db_dir, const char *_commit_worker_bin, void (*set_signal_fd)(int fd));

/**
 * @brief Start a new transaction.
 * 
 * @return >0: A transaction ID on success.
 *         -1: On failure, see error_code/error_message for details.
 * 
 * @error NOT_INIT   Database is not initialized.
 * @error TXN_LIMIT  Transaction limit reached.
 */
int start_transaction();

/**
 * @brief Commit a transaction.
 * 
 * @param ltxn_id Transaction ID to commit.
 * 
 * @return 0: The transaction has writes and was queued for asynchronous commit.
 *         >0: The transaction was read-only and was committed immediately, with this commit sequence number.
 * 
 * @error INVALID_TRANSACTION Transaction ID not found or already closed.
 */
size_t commit_transaction(int ltxn_id);

/**
 * @brief Abort a transaction.
 * 
 * @param ltxn_id Transaction ID to abort.
 * 
 * @return 0: On success.
 *         -1: On failure, see error_code/error_message for details.
 * 
 * @error INVALID_TRANSACTION Transaction ID not found or already closed.
 */
int abort_transaction(int ltxn_id);

/**
 * @brief Get value for a key within a transaction.
 * 
 * @param ltxn_id     Transaction ID.
 * @param key_data    Pointer to key data.
 * @param key_size    Size of key data in bytes.
 * @param value_data  Pointer to store the value pointer (output parameter). This will point within the
 *   LMDB memory map, so it should not and cannot be modified or freed. This data is guaranteed to be valid
 *   until the transaction ends.
 * @param value_size  Pointer to store the size of retrieved value (output parameter).
 * 
 * @return 1: Key was found.
 *         0: Key was not found (not a failure; value_data will be NULL and value_size will be 0).
 *         -1: On failure, see error_code/error_message for details.
 * 
 * @error INVALID_TRANSACTION  Transaction ID not found or already closed.
 * @error KEY_TOO_LONG         Key size exceeds maximum allowed length.
 * @error OOM                  Failed to allocate memory for the operation.
 * @error LMDB*                Various LMDB errors.
 */
int get(int ltxn_id, const void *key_data, size_t key_size, void **value_data, size_t *value_size);

/**
 * @brief Put a key-value pair within a transaction.
 * 
 * @param ltxn_id     Transaction ID.
 * @param key_data    Pointer to key data.
 * @param key_size    Size of key data in bytes.
 * @param value_data  Pointer to value data.
 * @param value_size  Size of value data in bytes.
 * 
 * @return 0: On success.
 *         -1: On failure, see error_code/error_message for details.
 * 
 * @error INVALID_TRANSACTION  Transaction ID not found or already closed.
 * @error KEY_TOO_LONG         Key size exceeds maximum allowed length.
 * @error OOM                  Failed to allocate memory for the operation.
 * @error LMDB*                Various LMDB errors.
 */
int put(int ltxn_id, const void *key_data, size_t key_size, const void *value_data, size_t value_size);

/**
 * @brief Delete a key within a transaction.
 * 
 * @param ltxn_id     Transaction ID.
 * @param key_data    Pointer to key data.
 * @param key_size    Size of key data in bytes.
 * 
 * @return 1: On success.
 *         0: On failure, see error_code/error_message for details.
 * 
 * @error INVALID_TRANSACTION  Transaction ID not found or already closed.
 * @error KEY_TOO_LONG         Key size exceeds maximum allowed length.
 * @error OOM                  Failed to allocate memory for the operation.
 * @error LMDB*                Various LMDB errors.
 */
int del(int ltxn_id, const void *key_data, size_t key_size);

/**
 * @brief Create an iterator for a range of keys.
 * 
 * @param ltxn_id         Transaction ID.
 * @param start_key_data  Pointer to start key data, or NULL to start from the beginning.
 * @param start_key_size  Size of start key data in bytes.
 * @param end_key_data    Pointer to end key data, or NULL for no upper bound.
 * @param end_key_size    Size of end key data in bytes.
 * @param reverse         If non-zero, iterate in reverse order.
 * 
 * @return >0: An iterator ID, on success.
 *         -1: On failure, see error_code/error_message for details.
 * 
 * @error INVALID_TRANSACTION  Transaction ID not found or already closed.
 * @error KEY_TOO_LONG         Start or end key size exceeds maximum allowed length.
 * @error OOM                  Failed to allocate memory for the iterator.
 * @error LMDB*                Various LMDB errors.
 */
int create_iterator(int ltxn_id, 
                    const void *start_key_data, size_t start_key_size, 
                    const void *end_key_data, size_t end_key_size, 
                    int reverse);

/**
 * @brief Read next key-value pair from an iterator.
 * 
 * @param iterator_id  Iterator ID.
 * @param key_data     Pointer to store the key pointer (output parameter). This will point within the
 *   LMDB memory map, so it should not and cannot be modified or freed. This data is guaranteed to be valid
 *   until the transaction ends.
 * @param key_size     Pointer to store the key size (output parameter).
 * @param value_data   Pointer to store the value pointer (output parameter). This will point within the
 *   LMDB memory map, so it should not and cannot be modified or freed. This data is guaranteed to be valid
 *   until the transaction ends.
 * @param value_size   Pointer to store the value size (output parameter).
 * 
 * @return 1: A key-value pair was read.
 *         0: Iterator has no more items (not a failure; data pointers will be NULL and sizes will be 0).
 *         -1: On failure, see error_code/error_message for details.
 * 
 * @error INVALID_ITERATOR  Iterator ID not found or already closed.
 * @error LMDB*             Various LMDB errors.
 */
int read_iterator(int iterator_id, 
                  void **key_data, size_t *key_size,
                  void **value_data, size_t *value_size);

/**
 * @brief Close an iterator.
 * 
 * @param iterator_id  Iterator ID to close.
 * 
 * @return 0: On success.
 *         -1: On failure, see error_code/error_message for details.
 * 
 * @error INVALID_ITERATOR    Iterator ID not found or already closed.
 * @error INVALID_TRANSACTION Transaction ID associated with iterator not found or closed.
 */
int close_iterator(int iterator_id);

/**
 * @brief Receive and discard all messages available on the commit worker file descriptor.
 * 
 * When using something like `poll()` or `select()` to monitor the commit worker file descriptor,
 * this function should be called to drain the file descriptor and to make sure the worker
 * connection is still healthy (and reconnect if necessary).
 * 
 * Alternatively, this function can be used in blocking mode (presumably in a secondary thread)
 * to wait until data is available on the commit worker file descriptor.
 * 
 * When this functions signals that data has been read, you would normally call `get_commit_results()`
 * to retrieve the results of completed transaction commits.
 *
 * @param blocking  If non-zero, block until results are available. Otherwise, return immediately if
 *   no results are available.
 * @return 1: Data was read from the commit worker file descriptor.
 *         0: No data was available for immediate read in non-blocking mode, or the commit worker
 *   connection was lost and a new connection could not be established.
 */

int drain_signal_fd(int blocking);

/**
 * @brief Get results of completed asynchronous transaction commits.
 *
 * This function should be called periodically or when data is available on the commit worker file descriptor
 * to retrieve the status of completed transaction commits.
 * 
 * @param results      Array to store commit results.
 * @param result_count Input: Maximum number of results to return. Output: Actual number of results returned.
 * 
 * @return  0: No more transactions are being committed.
 *          1: Call again later, as more transactions are being committed.
 *          2: Call again now, as there are more results ready than fit in the buffer.
 */
int get_commit_results(commit_result_t *results, int *result_count);

/**
 * Global error message buffer, set when functions return failure codes.
 * Contains a human-readable description of the last error.
 */
extern char error_message[2048];

/**
 * Global error code buffer, set when functions return failure codes.
 * Contains a short string identifier for the last error type.
 * This is useful for programmatic error handling.
 */
extern char error_code[32];

#endif // TRANSACTION_CLIENT_H