#include "transaction_client.h"
#include <node_api.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/socket.h>
#include <linux/limits.h>
#include <assert.h>

#define LOG(fmt, ...) \
    do { \
        fprintf(stderr, "OLMDB: " fmt "\n", ##__VA_ARGS__); \
    } while (0)

// References to JavaScript callbacks and objects
static napi_ref database_error_constructor = NULL;
static napi_env global_env = NULL;

// Storage for per-transaction promise deferreds
static napi_deferred transaction_deferreds[MAX_LTXNS];

static bool signal_job_running = false;
static napi_async_work signal_job_work = NULL;

// Forward declarations
static void start_signal_job();

// Helper to create and throw a DatabaseError
static void throw_database_error(napi_env env) {
    napi_value constructor, error_obj, code_string, message_string;
    
    // Get the DatabaseError constructor
    if (database_error_constructor == NULL || 
        napi_get_reference_value(env, database_error_constructor, &constructor) != napi_ok) {
        napi_throw_error(env, NULL, "DatabaseError constructor not available");
        return;
    }
    
    // Create arguments: message and code
    napi_create_string_utf8(env, error_message, NAPI_AUTO_LENGTH, &message_string);
    napi_create_string_utf8(env, error_code, NAPI_AUTO_LENGTH, &code_string);
    napi_value error_args[] = { message_string, code_string };

    // Create the error object
    napi_new_instance(env, constructor, 2, error_args, &error_obj);
    
    // Throw the error
    napi_throw(env, error_obj);
}

// A synchronous recv() running in an async work job
static void signal_job_exec(napi_env env, void* data) {
    (void)env; // Unused
    (void)data; // Unused
    drain_signal_fd(1); // Blocking read
}

// The async worker job has completed, meaning a new-results signal has been received
static void signal_job_complete(napi_env env, napi_status status, void* data) {
    signal_job_running = false;
    (void)data; // Unused
    
    const int MAX_BATCH_SIZE = 256;
    commit_result_t batch[MAX_BATCH_SIZE];
    
    napi_handle_scope scope;
    napi_open_handle_scope(env, &scope);

    int more_in_queue;
    do {
        int result_count = MAX_BATCH_SIZE;
        more_in_queue = get_commit_results(batch, &result_count);
        
        // Process this batch and resolve the promise for each result
        for (int i = 0; i < result_count; i++) {
            int ltxn_id = batch[i].ltxn_id;
            int callback_slot = ltxn_id_to_slot(ltxn_id);
            
            if (callback_slot >= 0 && callback_slot < MAX_LTXNS && transaction_deferreds[callback_slot]) {
                // Create the commit_seq value
                // We're wrapping at MAX_SAFE_INTEGER -- yes that sucks, as it may cause
                // weirdness every once in a while on large installs. But it beats the
                // alternative of losing precision and being wrong more and more over time.
                napi_value commit_seq_value;
                napi_create_int64(env, batch[i].commit_seq & 0x1fffffffffffff, &commit_seq_value);
                
                // Resolve the promise
                napi_resolve_deferred(env, transaction_deferreds[callback_slot], commit_seq_value);
                
                // Clean up the deferred
                transaction_deferreds[callback_slot] = NULL;
            }
        }
    } while (more_in_queue == 2);
    napi_close_handle_scope(env, scope);

    if (more_in_queue) {
        // Wait for a signal to indicate further results
        start_signal_job();
    }
}

static void start_signal_job() {
    if (signal_job_work) {
        napi_delete_async_work(global_env, signal_job_work);
    }

    napi_value resource_string;
    napi_create_string_utf8(global_env, "olmdb:await_fd", NAPI_AUTO_LENGTH, &resource_string);

    if (napi_create_async_work(global_env, NULL, resource_string, signal_job_exec, signal_job_complete, NULL, &signal_job_work) != napi_ok) {
        LOG("Failed to create async work for signal job");
        return;
    }
    if (napi_queue_async_work(global_env, signal_job_work) != napi_ok) {
        LOG("Failed to queue async work for signal job");
        return;
    }
    signal_job_running = true;
}

static napi_value create_database_error_class(napi_env env) {
    napi_value result;
    
    // Define the complete class in JavaScript
    const char* js_code = 
        "(function() {"
        "  class DatabaseError extends Error {"
        "    constructor(message, code) {"
        "      super(message);"
        "      this.name = 'DatabaseError';"
        "      if (code !== undefined) {"
        "        this.code = code;"
        "      }"
        "    }"
        "  }"
        "  return DatabaseError;"
        "})()";
    
    // Create string from JavaScript code
    napi_value script;
    napi_status status = napi_create_string_utf8(env, js_code, NAPI_AUTO_LENGTH, &script);
    if (status != napi_ok) return NULL;
    
    // Execute the JavaScript code
    status = napi_run_script(env, script, &result);
    if (status != napi_ok) return NULL;
    
    // Store a reference to the constructor for later use
    status = napi_create_reference(env, result, 1, &database_error_constructor);
    if (status != napi_ok) return NULL;
    
    return result;
}

// NAPI wrapper functions
napi_value js_init(napi_env env, napi_callback_info info) {
    size_t argc = 2;
    napi_value argv[2];    
    if (napi_get_cb_info(env, info, &argc, argv, NULL, NULL) != napi_ok || argc != 2) {
        napi_throw_type_error(env, NULL, "Two arguments expected");
        return NULL;
    }

    // Get optional database directory
    napi_valuetype arg_type;
    char db_dir[PATH_MAX];
    db_dir[0] = 0;
    if (napi_get_value_string_utf8(env, argv[0], db_dir, sizeof(db_dir), NULL) != napi_ok) {
        if (napi_typeof(env, argv[0], &arg_type) != napi_ok || (arg_type != napi_undefined && arg_type != napi_null)) {
            napi_throw_type_error(env, NULL, "Database path must be a string or undefined");
            return NULL;
        }
    }

    // Get commit worker binary path
    char commit_worker_bin[PATH_MAX];
    if (napi_get_value_string_utf8(env, argv[1], commit_worker_bin, sizeof(commit_worker_bin), NULL) != napi_ok) {
        napi_throw_type_error(env, NULL, "Commit worker binary path must be a string");
        return NULL;
    }

    global_env = env;

    // Call C API
    int result = init(db_dir[0] ? db_dir : NULL, commit_worker_bin, NULL);

    if (result < 0) {
        throw_database_error(env);
        return NULL;
    }
    
    // Return undefined instead of the result value
    napi_value undefined;
    napi_get_undefined(env, &undefined);
    return undefined;
}

napi_value js_start_transaction(napi_env env, napi_callback_info info) {
    int ltxn_id = start_transaction();
    
    if (ltxn_id < 0) {
        throw_database_error(env);
        return NULL;
    }
    
    napi_value result;
    napi_create_int32(env, ltxn_id, &result);
    return result;
}

napi_value js_commit_transaction(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value argv[1];
    int32_t ltxn_id;

    if (napi_get_cb_info(env, info, &argc, argv, NULL, NULL) != napi_ok || argc < 1 ||
        napi_get_value_int32(env, argv[0], &ltxn_id) != napi_ok) {
        napi_throw_type_error(env, NULL, "Expected transaction ID as integer");
        return NULL;
    }
    
    size_t commit_seq = commit_transaction(ltxn_id);

    // For read-only transactions (commit_seq > 0), return the number directly
    if (commit_seq > 0) {
        napi_value js_result;
        // We're wrapping at MAX_SAFE_INTEGER -- yes that sucks, as it may cause
        // weirdness every once in a while on large installs. But it beats the
        // alternative of losing precision and being wrong more and more over time.
        napi_create_int64(env, commit_seq & 0x1fffffffffffff, &js_result);
        return js_result;
    }
    
    // For async commits (commit_seq == 0), create and return a Promise
    int callback_slot = ltxn_id_to_slot(ltxn_id);
    if (callback_slot < 0 || callback_slot >= MAX_LTXNS) {
        napi_throw_error(env, NULL, "Transaction ID out of range");
        return NULL;
    }
    
    // Clean up any existing deferred in this slot (shouldn't happen)
    if (transaction_deferreds[callback_slot]) {
        transaction_deferreds[callback_slot] = NULL;
    }
    
    // Create a Promise and store the deferred
    napi_value promise;
    napi_deferred deferred;
    if (napi_create_promise(env, &deferred, &promise) != napi_ok) {
        napi_throw_error(env, NULL, "Failed to create promise");
        return NULL;
    }
    
    transaction_deferreds[callback_slot] = deferred;
    
    // Start the signal job if not already running
    if (!signal_job_running) {
        start_signal_job();
    }
    
    return promise;
}

napi_value js_abort_transaction(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value argv[1];
    int32_t ltxn_id;
    
    if (napi_get_cb_info(env, info, &argc, argv, NULL, NULL) != napi_ok || argc < 1 ||
        napi_get_value_int32(env, argv[0], &ltxn_id) != napi_ok) {
        napi_throw_type_error(env, NULL, "Expected transaction ID as integer");
        return NULL;
    }
    
    // Clean up any deferred for this transaction
    int callback_slot = ltxn_id_to_slot(ltxn_id);
    if (callback_slot >= 0 && callback_slot < MAX_LTXNS && transaction_deferreds[callback_slot]) {
        // Reject the promise since we're aborting
        napi_value error_msg;
        napi_create_string_utf8(env, "Transaction aborted", NAPI_AUTO_LENGTH, &error_msg);
        napi_reject_deferred(env, transaction_deferreds[callback_slot], error_msg);
        transaction_deferreds[callback_slot] = NULL;
    }
    
    int result = abort_transaction(ltxn_id);
    
    if (result < 0) {
        throw_database_error(env);
        return NULL;
    }
    
    napi_value undefined;
    napi_get_undefined(env, &undefined);
    return undefined;
}

napi_value js_get(napi_env env, napi_callback_info info) {
    size_t argc = 2;
    napi_value argv[2];
    int32_t ltxn_id;
    
    if (napi_get_cb_info(env, info, &argc, argv, NULL, NULL) != napi_ok || argc < 2 ||
        napi_get_value_int32(env, argv[0], &ltxn_id) != napi_ok) {
        napi_throw_type_error(env, NULL, "Expected transaction ID and key");
        return NULL;
    }
    
    // Get key buffer
    void *key_data;
    size_t key_size;
    if (napi_get_arraybuffer_info(env, argv[1], &key_data, &key_size) != napi_ok) {
        napi_throw_type_error(env, NULL, "Expected key as ArrayBuffer");
        return NULL;
    }
    
    // Call C API
    void* value_data;
    size_t value_size;
    assert(key_size > 0);
    int result = get(ltxn_id, key_data, key_size, &value_data, &value_size);
    
    if (result == -1) {
        throw_database_error(env);
        return NULL;
    }
    
    if (result == 0) { // Not found
        napi_value undefined;
        napi_get_undefined(env, &undefined);
        return undefined;
    }
    
    // Return value as ArrayBuffer
    napi_value result_buffer;
    napi_create_external_arraybuffer(env, value_data, value_size, NULL, NULL, &result_buffer);
    return result_buffer;
}

napi_value js_put(napi_env env, napi_callback_info info) {
    size_t argc = 3;
    napi_value argv[3];
    int32_t ltxn_id;
    
    if (napi_get_cb_info(env, info, &argc, argv, NULL, NULL) != napi_ok || argc < 3 ||
        napi_get_value_int32(env, argv[0], &ltxn_id) != napi_ok) {
        napi_throw_type_error(env, NULL, "Expected transaction ID, key, and value");
        return NULL;
    }
    
    // Get key and value buffers
    void *key_data, *value_data;
    size_t key_size, value_size;
    if (napi_get_arraybuffer_info(env, argv[1], &key_data, &key_size) != napi_ok ||
        napi_get_arraybuffer_info(env, argv[2], &value_data, &value_size) != napi_ok) {
        napi_throw_type_error(env, NULL, "Expected key and value as ArrayBuffers");
        return NULL;
    }
    
    // Call C API
    int result = put(ltxn_id, key_data, key_size, value_data, value_size);
    
    if (result < 0) {
        throw_database_error(env);
        return NULL;
    }
    
    napi_value undefined;
    napi_get_undefined(env, &undefined);
    return undefined;
}

napi_value js_del(napi_env env, napi_callback_info info) {
    size_t argc = 2;
    napi_value argv[2];
    int32_t ltxn_id;
    
    if (napi_get_cb_info(env, info, &argc, argv, NULL, NULL) != napi_ok || argc < 2 ||
        napi_get_value_int32(env, argv[0], &ltxn_id) != napi_ok) {
        napi_throw_type_error(env, NULL, "Expected transaction ID and key");
        return NULL;
    }
    
    // Get key buffer
    void *key_data;
    size_t key_size;
    if (napi_get_arraybuffer_info(env, argv[1], &key_data, &key_size) != napi_ok) {
        napi_throw_type_error(env, NULL, "Expected key as ArrayBuffer");
        return NULL;
    }
    
    // Call C API
    int result = del(ltxn_id, key_data, key_size);
    
    if (result < 0) {
        throw_database_error(env);
        return NULL;
    }
    
    napi_value undefined;
    napi_get_undefined(env, &undefined);
    return undefined;
}

napi_value js_create_iterator(napi_env env, napi_callback_info info) {
    size_t argc = 4;
    napi_value argv[4];
    int32_t ltxn_id;
    
    if (napi_get_cb_info(env, info, &argc, argv, NULL, NULL) != napi_ok || argc < 1 ||
        napi_get_value_int32(env, argv[0], &ltxn_id) != napi_ok) {
        napi_throw_type_error(env, NULL, "Expected transaction ID and optional parameters");
        return NULL;
    }
    
    // Get optional parameters
    void *start_key_data = NULL, *end_key_data = NULL;
    size_t start_key_size = 0, end_key_size = 0;
    bool reverse = false;
    napi_valuetype key_type;
    
    // Start key (optional)
    if (argc >= 2 && napi_typeof(env, argv[1], &key_type) == napi_ok && 
        key_type != napi_null && key_type != napi_undefined) {
        if (napi_get_arraybuffer_info(env, argv[1], &start_key_data, &start_key_size) != napi_ok) {
            napi_throw_type_error(env, NULL, "Start key must be an ArrayBuffer");
            return NULL;
        }
    }
    
    // End key (optional)
    if (argc >= 3 && napi_typeof(env, argv[2], &key_type) == napi_ok && 
        key_type != napi_null && key_type != napi_undefined) {
        if (napi_get_arraybuffer_info(env, argv[2], &end_key_data, &end_key_size) != napi_ok) {
            napi_throw_type_error(env, NULL, "End key must be an ArrayBuffer");
            return NULL;
        }
    }
    
    // Reverse flag (optional)
    if (argc >= 4) {
        if (napi_get_value_bool(env, argv[3], &reverse) != napi_ok) {
            napi_throw_type_error(env, NULL, "Reverse must be a boolean");
            return NULL;
        }
    }
    
    // Call C API
    int iterator_id = create_iterator(ltxn_id, start_key_data, start_key_size, 
                                      end_key_data, end_key_size, reverse ? 1 : 0);
    
    if (iterator_id < 0) {
        throw_database_error(env);
        return NULL;
    }
    
    napi_value result;
    napi_create_int32(env, iterator_id, &result);
    return result;
}

napi_value js_read_iterator(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value argv[1];
    int32_t iterator_id;
    
    if (napi_get_cb_info(env, info, &argc, argv, NULL, NULL) != napi_ok || argc < 1 ||
        napi_get_value_int32(env, argv[0], &iterator_id) != napi_ok) {
        napi_throw_type_error(env, NULL, "Expected iterator ID");
        return NULL;
    }
    
    // Call C API
    void *key_data, *value_data;
    size_t key_size, value_size;
    int result = read_iterator(iterator_id, &key_data, &key_size, &value_data, &value_size);
    
    if (result < 0) {
        throw_database_error(env);
        return NULL;
    }
    
    if (result == 0) { // No more items
        napi_value undefined;
        napi_get_undefined(env, &undefined);
        return undefined;
    }
    
    // Create result object with key and value
    napi_value result_obj, key_buffer, value_buffer;
    napi_create_object(env, &result_obj);
    napi_create_external_arraybuffer(env, key_data, key_size, NULL, NULL, &key_buffer);
    napi_create_external_arraybuffer(env, value_data, value_size, NULL, NULL, &value_buffer);
    napi_set_named_property(env, result_obj, "key", key_buffer);
    napi_set_named_property(env, result_obj, "value", value_buffer);
    
    return result_obj;
}

napi_value js_close_iterator(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value argv[1];
    int32_t iterator_id;
    
    if (napi_get_cb_info(env, info, &argc, argv, NULL, NULL) != napi_ok || argc < 1 ||
        napi_get_value_int32(env, argv[0], &iterator_id) != napi_ok) {
        napi_throw_type_error(env, NULL, "Expected iterator ID");
        return NULL;
    }
    
    // Call C API
    int result = close_iterator(iterator_id);
    
    if (result < 0) {
        throw_database_error(env);
        return NULL;
    }
    
    napi_value undefined;
    napi_get_undefined(env, &undefined);
    return undefined;
}

// Module initialization
napi_value Init(napi_env env, napi_value exports) {
    // Create DatabaseError class
    napi_value db_error_class = create_database_error_class(env);
    napi_set_named_property(env, exports, "DatabaseError", db_error_class);
    
    // Function definition table
    struct { const char* name; napi_callback fn; } functions[] = {
        {"init", js_init},
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
    napi_value fn;
    for (size_t i = 0; i < sizeof(functions) / sizeof(functions[0]); i++) {
        if (napi_create_function(env, NULL, 0, functions[i].fn, NULL, &fn) != napi_ok ||
            napi_set_named_property(env, exports, functions[i].name, fn) != napi_ok) {
            napi_throw_error(env, NULL, "Failed to export function");
            return NULL;
        }
    }
    
    return exports;
}

NAPI_MODULE(NODE_GYP_MODULE_NAME, Init)