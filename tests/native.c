/**
 * Native C Test Suite for OLMDB
 * 
 * This test file provides comprehensive testing of the OLMDB transaction_client API
 * directly from C code. It includes:
 * 
 * - Basic database initialization
 * - Transaction operations (start, commit, abort)
 * - Key-value operations (put, get, delete)
 * - Iterator operations (forward/backward iteration)
 * - Performance testing with 100,000 sequential read/write operations
 * - Performance testing with 100,000 reads of the same key in a single transaction
 * 
 * To run: npm run test:native
 * 
 * The test suite validates both correctness and performance of the low-level C API.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/time.h>
#include "../lowlevel/transaction_client.h"

// ANSI color codes for pretty output
#define COLOR_GREEN "\033[32m"
#define COLOR_RED "\033[31m"
#define COLOR_YELLOW "\033[33m"
#define COLOR_RESET "\033[0m"

// Test statistics
static int tests_passed = 0;
static int tests_failed = 0;

// Utility function to get current time in milliseconds
static double get_time_ms() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec * 1000.0 + tv.tv_usec / 1000.0;
}

// Assert macro for tests
#define TEST_ASSERT(condition, message) \
    do { \
        if (!(condition)) { \
            printf(COLOR_RED "✗ FAILED: %s\n" COLOR_RESET, message); \
            printf("  Error: %s (%s)\n", error_message, error_code); \
            tests_failed++; \
            return -1; \
        } \
    } while(0)

#define TEST_PASS(message) \
    do { \
        printf(COLOR_GREEN "✓ PASSED: %s\n" COLOR_RESET, message); \
        tests_passed++; \
    } while(0)

// Signal handler for commit notifications (optional)
static volatile int signal_fd_value = -1;
static void set_signal_fd(int fd) {
    signal_fd_value = fd;
}

// Test 1: Basic initialization
int test_init() {
    printf("\n" COLOR_YELLOW "Test 1: Database Initialization\n" COLOR_RESET);
    unlink("./.olmdb_native/data.mdb");
    unlink("./.olmdb_native/lock.mdb");
    
    int rc = init("./.olmdb_native", "./build/Debug/commit_worker", set_signal_fd);
    TEST_ASSERT(rc == 0, "Initialize database");
    
    // Test double init with same parameters (should succeed)
    rc = init("./.olmdb_native", "./build/Debug/commit_worker", set_signal_fd);
    TEST_ASSERT(rc == 0, "Re-initialize with same parameters");
    
    TEST_PASS("Database initialization");
    return 0;
}

// Test 2: Basic transaction operations
int test_basic_transaction() {
    printf("\n" COLOR_YELLOW "Test 2: Basic Transaction Operations\n" COLOR_RESET);
    
    // Start a transaction
    int txn_id = start_transaction();
    TEST_ASSERT(txn_id > 0, "Start transaction");
    
    // Put a key-value pair
    const char *key = "test_key";
    const char *value = "test_value";
    int rc = put(txn_id, key, strlen(key), value, strlen(value));
    TEST_ASSERT(rc == 0, "Put key-value pair");
    
    // Get the value back (should get from write buffer)
    void *retrieved_value = NULL;
    size_t retrieved_size = 0;
    rc = get(txn_id, key, strlen(key), &retrieved_value, &retrieved_size);
    TEST_ASSERT(rc == 1, "Get key-value pair from transaction");
    TEST_ASSERT(retrieved_size == strlen(value), "Retrieved value has correct size");
    TEST_ASSERT(memcmp(retrieved_value, value, retrieved_size) == 0, "Retrieved value matches");
    
    // Commit the transaction
    size_t commit_result = commit_transaction(txn_id);
    TEST_ASSERT(commit_result == 0, "Commit transaction (async)");
    
    // Wait for commit to complete
    // usleep(100000); // 100ms
    drain_signal_fd(1);
    
    commit_result_t results[10];
    int result_count = 10;
    get_commit_results(results, &result_count);
    TEST_ASSERT(result_count >= 1, "Got commit results");
    TEST_ASSERT(results[0].commit_seq > 0, "Transaction committed successfully");
    
    TEST_PASS("Basic transaction operations");
    return 0;
}

// Test 3: Read-only transaction
int test_read_only_transaction() {
    printf("\n" COLOR_YELLOW "Test 3: Read-Only Transaction\n" COLOR_RESET);
    
    // Start a new transaction
    int txn_id = start_transaction();
    TEST_ASSERT(txn_id > 0, "Start transaction");
    
    // Read the value we stored in the previous test
    const char *key = "test_key";
    void *retrieved_value = NULL;
    size_t retrieved_size = 0;
    int rc = get(txn_id, key, strlen(key), &retrieved_value, &retrieved_size);
    TEST_ASSERT(rc == 1, "Get key-value pair");
    TEST_ASSERT(memcmp(retrieved_value, "test_value", strlen("test_value")) == 0, "Value matches");
    
    // Commit read-only transaction (should return immediately with commit_seq)
    size_t commit_result = commit_transaction(txn_id);
    TEST_ASSERT(commit_result > 0, "Read-only transaction committed immediately");
    
    TEST_PASS("Read-only transaction");
    return 0;
}

// Test 4: Delete operation
int test_delete() {
    printf("\n" COLOR_YELLOW "Test 4: Delete Operation\n" COLOR_RESET);
    
    // Start transaction
    int txn_id = start_transaction();
    TEST_ASSERT(txn_id > 0, "Start transaction");
    
    // Delete a key
    const char *key = "test_key";
    int rc = del(txn_id, key, strlen(key));
    TEST_ASSERT(rc == 0, "Delete key");
    
    // Try to get the deleted key (should not be found in transaction)
    void *retrieved_value = NULL;
    size_t retrieved_size = 0;
    rc = get(txn_id, key, strlen(key), &retrieved_value, &retrieved_size);
    TEST_ASSERT(rc == 0, "Deleted key not found");
    
    // Commit
    size_t commit_result = commit_transaction(txn_id);
    TEST_ASSERT(commit_result == 0, "Commit delete transaction");
    
    // Wait for commit
    // usleep(100000);
    drain_signal_fd(1);
    
    commit_result_t results[10];
    int result_count = 10;
    get_commit_results(results, &result_count);
    TEST_ASSERT(result_count >= 1, "Got commit results");
    
    TEST_PASS("Delete operation");
    return 0;
}

// Test 5: Iterator operations
int test_iterator() {
    printf("\n" COLOR_YELLOW "Test 5: Iterator Operations\n" COLOR_RESET);
    
    // First, populate some data
    int txn_id = start_transaction();
    TEST_ASSERT(txn_id > 0, "Start transaction");
    
    for (int i = 0; i < 10; i++) {
        char key[32], value[32];
        snprintf(key, sizeof(key), "key_%03d", i);
        snprintf(value, sizeof(value), "value_%03d", i);
        int rc = put(txn_id, key, strlen(key), value, strlen(value));
        TEST_ASSERT(rc == 0, "Put key-value pair");
    }
    
    // Commit
    commit_transaction(txn_id);
    // usleep(100000);
    drain_signal_fd(1);
    
    commit_result_t results[10];
    int result_count = 10;
    get_commit_results(results, &result_count);
    
    // Now test iterator
    txn_id = start_transaction();
    TEST_ASSERT(txn_id > 0, "Start transaction for iterator");
    
    int iter_id = create_iterator(txn_id, NULL, 0, NULL, 0, 0);
    TEST_ASSERT(iter_id > 0, "Create iterator");
    
    int count = 0;
    while (1) {
        void *key_data, *value_data;
        size_t key_size, value_size;
        int rc = read_iterator(iter_id, &key_data, &key_size, &value_data, &value_size);
        if (rc == 0) break; // No more items
        TEST_ASSERT(rc == 1, "Read iterator");
        count++;
    }
    
    TEST_ASSERT(count == 10, "Iterator read all items");
    
    int rc = close_iterator(iter_id);
    TEST_ASSERT(rc == 0, "Close iterator");
    
    // Read-only commit
    commit_transaction(txn_id);
    
    TEST_PASS("Iterator operations");
    return 0;
}

// Test 6: 100,000 sequential read/write operations
int test_large_sequential_operations() {
    printf("\n" COLOR_YELLOW "Test 6: 100,000 Sequential Read/Write Operations\n" COLOR_RESET);
    
    const int num_operations = 100000;
    double start_time = get_time_ms();
    
    // Phase 1: Write 100,000 key-value pairs
    printf("  Writing %d key-value pairs...\n", num_operations);
    double write_start = get_time_ms();
    
    for (int i = 0; i < num_operations; i++) {
        int txn_id = start_transaction();
        TEST_ASSERT(txn_id > 0, "Start transaction");
        
        char key[32], value[64];
        snprintf(key, sizeof(key), "perf_key_%06d", i);
        snprintf(value, sizeof(value), "perf_value_%06d_data", i);
        
        int rc = put(txn_id, key, strlen(key), value, strlen(value));
        TEST_ASSERT(rc == 0, "Put operation");
        
        // Commit transaction
        commit_transaction(txn_id);
        
        // Periodically wait for commits to complete to avoid queue buildup
        if (i % 1000 == 999) {
            drain_signal_fd(0);
            commit_result_t results[1000];
            int result_count = 1000;
            while (get_commit_results(results, &result_count) > 0) {
                result_count = 1000;
            }
            
            if (i % 10000 == 9999) {
                printf("    Progress: %d/%d (%.1f%%)\n", i + 1, num_operations, 
                       (i + 1) * 100.0 / num_operations);
            }
        }
    }
    
    // Wait for all commits to complete
    printf("  Waiting for commits to complete...\n");
    // usleep(500000); // 500ms
    drain_signal_fd(1);
    commit_result_t results[1000];
    int result_count = 1000;
    while (get_commit_results(results, &result_count) > 0) {
        result_count = 1000;
    }
    
    double write_time = get_time_ms() - write_start;
    printf("  Write phase completed in %.2f seconds (%.0f ops/sec)\n", 
           write_time / 1000.0, num_operations / (write_time / 1000.0));
    
    // Phase 2: Read 100,000 key-value pairs
    printf("  Reading %d key-value pairs...\n", num_operations);
    double read_start = get_time_ms();
    
    for (int i = 0; i < num_operations; i++) {
        int txn_id = start_transaction();
        TEST_ASSERT(txn_id > 0, "Start transaction");
        
        char key[32], expected_value[64];
        snprintf(key, sizeof(key), "perf_key_%06d", i);
        snprintf(expected_value, sizeof(expected_value), "perf_value_%06d_data", i);
        
        void *retrieved_value = NULL;
        size_t retrieved_size = 0;
        int rc = get(txn_id, key, strlen(key), &retrieved_value, &retrieved_size);
        TEST_ASSERT(rc == 1, "Get operation");
        TEST_ASSERT(retrieved_size == strlen(expected_value), "Value size matches");
        TEST_ASSERT(memcmp(retrieved_value, expected_value, retrieved_size) == 0, 
                   "Value content matches");
        
        // Read-only commit
        commit_transaction(txn_id);
        
        if (i % 10000 == 9999) {
            printf("    Progress: %d/%d (%.1f%%)\n", i + 1, num_operations, 
                   (i + 1) * 100.0 / num_operations);
        }
    }
    
    double read_time = get_time_ms() - read_start;
    printf("  Read phase completed in %.2f seconds (%.0f ops/sec)\n", 
           read_time / 1000.0, num_operations / (read_time / 1000.0));
    
    double total_time = get_time_ms() - start_time;
    printf("  Total time: %.2f seconds (%.0f total ops/sec)\n", 
           total_time / 1000.0, (num_operations * 2) / (total_time / 1000.0));
    
    TEST_PASS("100,000 sequential read/write operations");
    return 0;
}

// Test 7: 100,000 reads of the same key in a single transaction
int test_single_transaction_many_reads() {
    printf("\n" COLOR_YELLOW "Test 7: Grow transaction until OOM\n" COLOR_RESET);
    
    const int num_reads = 50000000;
    
    // First, write a key-value pair to read
    printf("  Setting up test data...\n");
    int txn_id = start_transaction();
    TEST_ASSERT(txn_id > 0, "Start setup transaction");
    
    const char *key = "single_txn_test_key_but_make_it_rather_long_so_we_run_out_of_memory_faster";
    const char *value = "single_txn_test_value_with_some_data";
    int rc = put(txn_id, key, strlen(key), value, strlen(value));
    TEST_ASSERT(rc == 0, "Put test key-value pair");
    
    commit_transaction(txn_id);
    drain_signal_fd(1);
    
    commit_result_t results[10];
    int result_count = 10;
    get_commit_results(results, &result_count);
    TEST_ASSERT(result_count >= 1, "Setup transaction committed");
    
    // Now perform 100,000 reads in a single transaction
    printf("  Performing %d reads of the same key in a single transaction...\n", num_reads);
    double start_time = get_time_ms();
    
    txn_id = start_transaction();
    TEST_ASSERT(txn_id > 0, "Start transaction for reads");
    
    for (int i = 0; i < num_reads; i++) {
        void *retrieved_value = NULL;
        size_t retrieved_size = 0;
        rc = get(txn_id, key, strlen(key), &retrieved_value, &retrieved_size);
        if (rc < 0) {
            TEST_ASSERT(!strcmp(error_code, "OOM"), "Out-of-memory error expected");
            // We should still be able to commit the OOM transaction
            size_t commit_result = commit_transaction(txn_id);
            TEST_ASSERT(commit_result > 0, "Read-only transaction committed immediately");
                
            TEST_PASS("Grow transaction until OOM");
            return 0;
        }
        TEST_ASSERT(rc == 1, "Get operation succeeded");
        TEST_ASSERT(retrieved_size == strlen(value), "Value size matches");
        TEST_ASSERT(memcmp(retrieved_value, value, retrieved_size) == 0, "Value content matches");
        
        if (i % 100000 == 99999) {
            printf("    Progress: %d/%d (%.1f%%)\n", i + 1, num_reads, 
                   (i + 1) * 100.0 / num_reads);
        }
    }
    
    TEST_ASSERT(0, "Expected to run out of memory before completing all reads");
    return 0;
}

// Test 8: Abort transaction
int test_abort() {
    printf("\n" COLOR_YELLOW "Test 8: Abort Transaction\n" COLOR_RESET);
    
    int txn_id = start_transaction();
    TEST_ASSERT(txn_id > 0, "Start transaction");
    
    // Put a key-value pair
    const char *key = "abort_test_key";
    const char *value = "abort_test_value";
    int rc = put(txn_id, key, strlen(key), value, strlen(value));
    TEST_ASSERT(rc == 0, "Put key-value pair");
    
    // Abort the transaction
    rc = abort_transaction(txn_id);
    TEST_ASSERT(rc == 0, "Abort transaction");
    
    // Start a new transaction and verify the key doesn't exist
    txn_id = start_transaction();
    TEST_ASSERT(txn_id > 0, "Start new transaction");
    
    void *retrieved_value = NULL;
    size_t retrieved_size = 0;
    rc = get(txn_id, key, strlen(key), &retrieved_value, &retrieved_size);
    TEST_ASSERT(rc == 0, "Aborted key not found");
    
    commit_transaction(txn_id);
    
    TEST_PASS("Abort transaction");
    return 0;
}

// Main test runner
int main() {
    printf("\n");
    printf("===========================================\n");
    printf("  OLMDB Native C Test Suite\n");
    printf("===========================================\n");
    
    // Clean up any previous test database
    system("rm -rf ./test_db");
    
    // Run all tests
    if (test_init() < 0) goto cleanup;
    if (test_basic_transaction() < 0) goto cleanup;
    if (test_read_only_transaction() < 0) goto cleanup;
    if (test_delete() < 0) goto cleanup;
    if (test_iterator() < 0) goto cleanup;
    if (test_large_sequential_operations() < 0) goto cleanup;
    if (test_single_transaction_many_reads() < 0) goto cleanup;
    if (test_abort() < 0) goto cleanup;
    
cleanup:
    // Print summary
    printf("\n");
    printf("===========================================\n");
    printf("  Test Summary\n");
    printf("===========================================\n");
    printf("Tests Passed: " COLOR_GREEN "%d\n" COLOR_RESET, tests_passed);
    printf("Tests Failed: " COLOR_RED "%d\n" COLOR_RESET, tests_failed);
    printf("===========================================\n");
    printf("\n");
    
    if (tests_failed > 0) {
        printf(COLOR_RED "Some tests failed!\n" COLOR_RESET);
        return 1;
    } else {
        printf(COLOR_GREEN "All tests passed!\n" COLOR_RESET);
        return 0;
    }
}
