/**
 * OLMDB - Optimistic LMDB
 * Low-level C API
 */
#ifndef OLMDB_LOWLEVEL_H
#define OLMDB_LOWLEVEL_H

#include <stdint.h>
#include <stddef.h>
#include "lowlevel.h"

typedef struct {
    int ltxn_id;
    int success; // 1 for success, 0 for race/failure
} commit_result_t;

// Initialization and cleanup
int init(const char *db_dir, void (*set_signal_fd)(int fd));

// Transaction API
int start_transaction();
int commit_transaction(int ltxn_id);
int abort_transaction(int ltxn_id);

// Data operations
int get(int ltxn_id, const void *key_data, size_t key_size, void **value_data, size_t *value_size);
int put(int ltxn_id, const void *key_data, size_t key_size, const void *value_data, size_t value_size);
int del(int ltxn_id, const void *key_data, size_t key_size);

// Iterator API
int create_iterator(int ltxn_id, 
                         const void *start_key_data, size_t start_key_size, 
                         const void *end_key_data, size_t end_key_size, 
                         int reverse);
int read_iterator(int iterator_id, 
                       void **key_data, size_t *key_size,
                       void **value_data, size_t *value_size);
int close_iterator(int iterator_id);

int get_commit_results(commit_result_t *results, int max_results);

extern char error_message[2048];
extern char error_code[32];

#define SET_ERROR(code_str, msg, ...) \
    do { \
        snprintf(error_message, sizeof(error_message), msg, ##__VA_ARGS__); \
        strncpy(error_code, code_str, sizeof(error_code) - 1); \
        error_code[sizeof(error_code) - 1] = '\0'; \
    } while(0)


#endif // OLMDB_LOWLEVEL_H