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
 * - Performance testing with growing transaction until OOM
 * - Multi-instance test verifying that two transaction_client_t instances
 *   created in the same process share the same on-disk database via the
 *   commit worker daemon.
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

// The single transaction client used by most tests.
static transaction_client_t *client = NULL;

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

// Test 1: Basic initialization
int test_init() {
    printf("\n" COLOR_YELLOW "Test 1: Database Initialization\n" COLOR_RESET);
    unlink("./.olmdb_native/data.mdb");
    unlink("./.olmdb_native/lock.mdb");

    client = transaction_client_init("./.olmdb_native", "./build/Release/commit_worker", NULL);
    TEST_ASSERT(client != NULL, "Initialize database");

    // A second instance pointing at the same database should also succeed.
    transaction_client_t *second = transaction_client_init("./.olmdb_native", "./build/Release/commit_worker", NULL);
    TEST_ASSERT(second != NULL, "Create a second client instance");
    transaction_client_destroy(second);

    TEST_PASS("Database initialization");
    return 0;
}

// Test 2: Basic transaction operations
int test_basic_transaction() {
    printf("\n" COLOR_YELLOW "Test 2: Basic Transaction Operations\n" COLOR_RESET);

    int txn_id = start_transaction(client);
    TEST_ASSERT(txn_id > 0, "Start transaction");

    const char *key = "test_key";
    const char *value = "test_value";
    int rc = put(client, txn_id, key, strlen(key), value, strlen(value));
    TEST_ASSERT(rc == 0, "Put key-value pair");

    void *retrieved_value = NULL;
    size_t retrieved_size = 0;
    rc = get(client, txn_id, key, strlen(key), &retrieved_value, &retrieved_size);
    TEST_ASSERT(rc == 1, "Get key-value pair from transaction");
    TEST_ASSERT(retrieved_size == strlen(value), "Retrieved value has correct size");
    TEST_ASSERT(memcmp(retrieved_value, value, retrieved_size) == 0, "Retrieved value matches");

    size_t commit_result = commit_transaction(client, txn_id, 0);
    TEST_ASSERT(commit_result == 0, "Commit transaction (async)");

    drain_signal_fd(client, 1);

    commit_result_t results[10];
    int result_count = 10;
    get_commit_results(client, results, &result_count);
    TEST_ASSERT(result_count >= 1, "Got commit results");
    TEST_ASSERT(results[0].commit_seq > 0, "Transaction committed successfully");

    TEST_PASS("Basic transaction operations");
    return 0;
}

// Test 3: Read-only transaction
int test_read_only_transaction() {
    printf("\n" COLOR_YELLOW "Test 3: Read-Only Transaction\n" COLOR_RESET);

    int txn_id = start_transaction(client);
    TEST_ASSERT(txn_id > 0, "Start transaction");

    const char *key = "test_key";
    void *retrieved_value = NULL;
    size_t retrieved_size = 0;
    int rc = get(client, txn_id, key, strlen(key), &retrieved_value, &retrieved_size);
    TEST_ASSERT(rc == 1, "Get key-value pair");
    TEST_ASSERT(memcmp(retrieved_value, "test_value", strlen("test_value")) == 0, "Value matches");

    size_t commit_result = commit_transaction(client, txn_id, 0);
    TEST_ASSERT(commit_result > 0, "Read-only transaction committed immediately");

    TEST_PASS("Read-only transaction");
    return 0;
}

// Test 4: Delete operation
int test_delete() {
    printf("\n" COLOR_YELLOW "Test 4: Delete Operation\n" COLOR_RESET);

    int txn_id = start_transaction(client);
    TEST_ASSERT(txn_id > 0, "Start transaction");

    const char *key = "test_key";
    int rc = del(client, txn_id, key, strlen(key));
    TEST_ASSERT(rc == 0, "Delete key");

    void *retrieved_value = NULL;
    size_t retrieved_size = 0;
    rc = get(client, txn_id, key, strlen(key), &retrieved_value, &retrieved_size);
    TEST_ASSERT(rc == 0, "Deleted key not found");

    size_t commit_result = commit_transaction(client, txn_id, 0);
    TEST_ASSERT(commit_result == 0, "Commit delete transaction");

    drain_signal_fd(client, 1);

    commit_result_t results[10];
    int result_count = 10;
    get_commit_results(client, results, &result_count);
    TEST_ASSERT(result_count >= 1, "Got commit results");

    TEST_PASS("Delete operation");
    return 0;
}

// Test 5: Iterator operations
int test_iterator() {
    printf("\n" COLOR_YELLOW "Test 5: Iterator Operations\n" COLOR_RESET);

    int txn_id = start_transaction(client);
    TEST_ASSERT(txn_id > 0, "Start transaction");

    for (int i = 0; i < 10; i++) {
        char key[32], value[32];
        snprintf(key, sizeof(key), "key_%03d", i);
        snprintf(value, sizeof(value), "value_%03d", i);
        int rc = put(client, txn_id, key, strlen(key), value, strlen(value));
        TEST_ASSERT(rc == 0, "Put key-value pair");
    }

    commit_transaction(client, txn_id, 0);
    drain_signal_fd(client, 1);

    commit_result_t results[10];
    int result_count = 10;
    get_commit_results(client, results, &result_count);

    txn_id = start_transaction(client);
    TEST_ASSERT(txn_id > 0, "Start transaction for iterator");

    int iter_id = create_iterator(client, txn_id, NULL, 0, NULL, 0, 0);
    TEST_ASSERT(iter_id > 0, "Create iterator");

    int count = 0;
    while (1) {
        void *key_data, *value_data;
        size_t key_size, value_size;
        int rc = read_iterator(client, iter_id, &key_data, &key_size, &value_data, &value_size);
        if (rc == 0) break; // No more items
        TEST_ASSERT(rc == 1, "Read iterator");
        count++;
    }

    TEST_ASSERT(count == 10, "Iterator read all items");

    int rc = close_iterator(client, iter_id);
    TEST_ASSERT(rc == 0, "Close iterator");

    commit_transaction(client, txn_id, 0);

    TEST_PASS("Iterator operations");
    return 0;
}

// Test 6: 100,000 sequential read/write operations
int test_large_sequential_operations() {
    printf("\n" COLOR_YELLOW "Test 6: 100,000 Sequential Read/Write Operations\n" COLOR_RESET);

    const int num_operations = 100000;
    double start_time = get_time_ms();

    printf("  Writing %d key-value pairs...\n", num_operations);
    double write_start = get_time_ms();

    for (int i = 0; i < num_operations; i++) {
        int txn_id = start_transaction(client);
        TEST_ASSERT(txn_id > 0, "Start transaction");

        char key[32], value[64];
        snprintf(key, sizeof(key), "perf_key_%06d", i);
        snprintf(value, sizeof(value), "perf_value_%06d_data", i);

        int rc = put(client, txn_id, key, strlen(key), value, strlen(value));
        TEST_ASSERT(rc == 0, "Put operation");

        commit_transaction(client, txn_id, 0);

        if (i % 1000 == 999) {
            drain_signal_fd(client, 0);
            commit_result_t results[1000];
            int result_count = 1000;
            while (get_commit_results(client, results, &result_count) > 0) {
                result_count = 1000;
            }

            if (i % 10000 == 9999) {
                printf("    Progress: %d/%d (%.1f%%)\n", i + 1, num_operations,
                       (i + 1) * 100.0 / num_operations);
            }
        }
    }

    printf("  Waiting for commits to complete...\n");
    drain_signal_fd(client, 1);
    commit_result_t results[1000];
    int result_count = 1000;
    while (get_commit_results(client, results, &result_count) > 0) {
        result_count = 1000;
    }

    double write_time = get_time_ms() - write_start;
    printf("  Write phase completed in %.2f seconds (%.0f ops/sec)\n",
           write_time / 1000.0, num_operations / (write_time / 1000.0));

    printf("  Reading %d key-value pairs...\n", num_operations);
    double read_start = get_time_ms();

    for (int i = 0; i < num_operations; i++) {
        int txn_id = start_transaction(client);
        TEST_ASSERT(txn_id > 0, "Start transaction");

        char key[32], expected_value[64];
        snprintf(key, sizeof(key), "perf_key_%06d", i);
        snprintf(expected_value, sizeof(expected_value), "perf_value_%06d_data", i);

        void *retrieved_value = NULL;
        size_t retrieved_size = 0;
        int rc = get(client, txn_id, key, strlen(key), &retrieved_value, &retrieved_size);
        TEST_ASSERT(rc == 1, "Get operation");
        TEST_ASSERT(retrieved_size == strlen(expected_value), "Value size matches");
        TEST_ASSERT(memcmp(retrieved_value, expected_value, retrieved_size) == 0,
                   "Value content matches");

        commit_transaction(client, txn_id, 0);

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

// Test 7: Grow transaction until OOM
int test_single_transaction_many_reads() {
    printf("\n" COLOR_YELLOW "Test 7: Grow transaction until OOM\n" COLOR_RESET);

    const int num_reads = 50000000;

    printf("  Setting up test data...\n");
    int txn_id = start_transaction(client);
    TEST_ASSERT(txn_id > 0, "Start setup transaction");

    const char *key = "single_txn_test_key_but_make_it_rather_long_so_we_run_out_of_memory_faster";
    const char *value = "single_txn_test_value_with_some_data";
    int rc = put(client, txn_id, key, strlen(key), value, strlen(value));
    TEST_ASSERT(rc == 0, "Put test key-value pair");

    commit_transaction(client, txn_id, 0);
    drain_signal_fd(client, 1);

    commit_result_t results[10];
    int result_count = 10;
    get_commit_results(client, results, &result_count);
    TEST_ASSERT(result_count >= 1, "Setup transaction committed");

    printf("  Performing %d reads of the same key in a single transaction...\n", num_reads);

    txn_id = start_transaction(client);
    TEST_ASSERT(txn_id > 0, "Start transaction for reads");

    for (int i = 0; i < num_reads; i++) {
        void *retrieved_value = NULL;
        size_t retrieved_size = 0;
        rc = get(client, txn_id, key, strlen(key), &retrieved_value, &retrieved_size);
        if (rc < 0) {
            TEST_ASSERT(!strcmp(error_code, "OOM"), "Out-of-memory error expected");
            size_t commit_result = commit_transaction(client, txn_id, 0);
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

    int txn_id = start_transaction(client);
    TEST_ASSERT(txn_id > 0, "Start transaction");

    const char *key = "abort_test_key";
    const char *value = "abort_test_value";
    int rc = put(client, txn_id, key, strlen(key), value, strlen(value));
    TEST_ASSERT(rc == 0, "Put key-value pair");

    rc = abort_transaction(client, txn_id);
    TEST_ASSERT(rc == 0, "Abort transaction");

    txn_id = start_transaction(client);
    TEST_ASSERT(txn_id > 0, "Start new transaction");

    void *retrieved_value = NULL;
    size_t retrieved_size = 0;
    rc = get(client, txn_id, key, strlen(key), &retrieved_value, &retrieved_size);
    TEST_ASSERT(rc == 0, "Aborted key not found");

    commit_transaction(client, txn_id, 0);

    TEST_PASS("Abort transaction");
    return 0;
}

// Wait until all queued commits for the given client have been processed
// by the commit worker daemon.
static void drain_commits(transaction_client_t *c) {
    drain_signal_fd(c, 1);
    commit_result_t results[256];
    int result_count = 256;
    while (get_commit_results(c, results, &result_count) > 0) {
        result_count = 256;
    }
}

// Test 9: Multiple transaction_client_t instances coexisting in one process
//
// This mirrors the multi-Worker scenario from the JavaScript layer: two
// independent client instances pointing at the same database, used in
// alternation. Writes performed by one instance must become visible to a
// freshly started transaction on the other instance once the commit has
// landed.
int test_multi_client() {
    printf("\n" COLOR_YELLOW "Test 9: Multiple client instances on same database\n" COLOR_RESET);

    transaction_client_t *a = transaction_client_init("./.olmdb_native", "./build/Release/commit_worker", NULL);
    TEST_ASSERT(a != NULL, "Create client A");
    transaction_client_t *b = transaction_client_init("./.olmdb_native", "./build/Release/commit_worker", NULL);
    TEST_ASSERT(b != NULL, "Create client B");

    const char *key = "multi_client_key";
    const char *value = "multi_client_value";

    // Write through client A
    int txn_id_a = start_transaction(a);
    TEST_ASSERT(txn_id_a > 0, "A: start transaction");
    TEST_ASSERT(put(a, txn_id_a, key, strlen(key), value, strlen(value)) == 0, "A: put");
    TEST_ASSERT(commit_transaction(a, txn_id_a, 0) == 0, "A: async commit queued");
    drain_commits(a);

    // Read through client B
    int txn_id_b = start_transaction(b);
    TEST_ASSERT(txn_id_b > 0, "B: start transaction");
    void *got = NULL;
    size_t got_size = 0;
    int rc = get(b, txn_id_b, key, strlen(key), &got, &got_size);
    TEST_ASSERT(rc == 1, "B: get key");
    TEST_ASSERT(got_size == strlen(value) && memcmp(got, value, got_size) == 0, "B: value matches A's write");
    commit_transaction(b, txn_id_b, 0);

    transaction_client_destroy(a);
    transaction_client_destroy(b);

    TEST_PASS("Multiple client instances on same database");
    return 0;
}

// Main test runner
int main() {
    printf("\n");
    printf("===========================================\n");
    printf("  OLMDB Native C Test Suite\n");
    printf("===========================================\n");

    system("rm -rf ./test_db");

    if (test_init() < 0) goto cleanup;
    if (test_basic_transaction() < 0) goto cleanup;
    if (test_read_only_transaction() < 0) goto cleanup;
    if (test_delete() < 0) goto cleanup;
    if (test_iterator() < 0) goto cleanup;
    if (test_large_sequential_operations() < 0) goto cleanup;
    if (test_single_transaction_many_reads() < 0) goto cleanup;
    if (test_abort() < 0) goto cleanup;
    if (test_multi_client() < 0) goto cleanup;

cleanup:
    if (client) transaction_client_destroy(client);

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
