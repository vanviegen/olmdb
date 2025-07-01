#include "lowlevel.h"
#include <node_api.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

// Reference to the commit result callback
static napi_ref on_commit_result_cb = NULL;
static napi_env global_env = NULL;

// Helper for handling arraybuffer parameters
static napi_status get_arraybuffer_info(napi_env env, napi_value value, void** data, size_t* size) {
    return napi_get_arraybuffer_info(env, value, data, size);
}

// Callback function for signal fd changes
static void setup_signal_fd(int fd) {
    if (!on_commit_result_cb || !global_env) {
        return;
    }

    napi_value callback, undefined, args[1], result;
    napi_get_reference_value(global_env, on_commit_result_cb, &callback);
    napi_get_undefined(global_env, &undefined);
    
    // Create the fd argument and call the JavaScript callback
    napi_create_int32(global_env, fd, &args[0]);
    napi_call_function(global_env, undefined, callback, 1, args, &result);
}

// NAPI wrapper functions
napi_value js_init(napi_env env, napi_callback_info info) {
    size_t argc = 2;
    napi_value argv[2];
    char *db_dir = NULL;
    char db_dir_buffer[512];
    
    if (napi_get_cb_info(env, info, &argc, argv, NULL, NULL) != napi_ok || argc < 1) {
        napi_throw_type_error(env, NULL, "Expected callback function and optional database path");
        return NULL;
    }
    
    // Check that first argument is a function
    napi_valuetype arg_type;
    if (napi_typeof(env, argv[0], &arg_type) != napi_ok || arg_type != napi_function) {
        napi_throw_type_error(env, NULL, "Expected first argument to be a signal fd callback function");
        return NULL;
    }
    
    // Store callback reference
    if (on_commit_result_cb) napi_delete_reference(env, on_commit_result_cb);
    if (napi_create_reference(env, argv[0], 1, &on_commit_result_cb) != napi_ok) {
        napi_throw_error(env, NULL, "Failed to store callback reference");
        return NULL;
    }
    
    global_env = env;
    
    // Get optional database directory
    if (argc >= 2) {
        if (napi_get_value_string_utf8(env, argv[1], db_dir_buffer, sizeof(db_dir_buffer), NULL) != napi_ok) {
            napi_throw_type_error(env, NULL, "Database path must be a string");
            return NULL;
        }
        db_dir = db_dir_buffer;
    }
    
    // Call C API
    int result = init(db_dir, setup_signal_fd);
    
    napi_value js_result;
    napi_create_int32(env, result, &js_result);
    return js_result;
}

napi_value js_start_transaction(napi_env env, napi_callback_info info) {
    int ltxn_id = start_transaction();
    
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
        
    int result = commit_transaction(ltxn_id);
    
    napi_value js_result;
    napi_create_int32(env, result, &js_result);
    return js_result;
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
    
    int result = abort_transaction(ltxn_id);
    
    napi_value js_result;
    napi_create_int32(env, result, &js_result);
    return js_result;
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
    if (get_arraybuffer_info(env, argv[1], &key_data, &key_size) != napi_ok) {
        napi_throw_type_error(env, NULL, "Expected key as ArrayBuffer");
        return NULL;
    }
    
    // Call C API
    void* value_data;
    size_t value_size;
    int result = get(ltxn_id, key_data, key_size, &value_data, &value_size);
    
    if (result == -1) {
        // Return error code as integer
        napi_value js_result;
        napi_create_int32(env, result, &js_result);
        return js_result;
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
    if (get_arraybuffer_info(env, argv[1], &key_data, &key_size) != napi_ok ||
        get_arraybuffer_info(env, argv[2], &value_data, &value_size) != napi_ok) {
        napi_throw_type_error(env, NULL, "Expected key and value as ArrayBuffers");
        return NULL;
    }
    
    // Call C API
    int result = put(ltxn_id, key_data, key_size, value_data, value_size);
    
    napi_value js_result;
    napi_create_int32(env, result, &js_result);
    return js_result;
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
    if (get_arraybuffer_info(env, argv[1], &key_data, &key_size) != napi_ok) {
        napi_throw_type_error(env, NULL, "Expected key as ArrayBuffer");
        return NULL;
    }
    
    // Call C API
    int result = del(ltxn_id, key_data, key_size);
    
    napi_value js_result;
    napi_create_int32(env, result, &js_result);
    return js_result;
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
        if (get_arraybuffer_info(env, argv[1], &start_key_data, &start_key_size) != napi_ok) {
            napi_throw_type_error(env, NULL, "Start key must be an ArrayBuffer");
            return NULL;
        }
    }
    
    // End key (optional)
    if (argc >= 3 && napi_typeof(env, argv[2], &key_type) == napi_ok && 
        key_type != napi_null && key_type != napi_undefined) {
        if (get_arraybuffer_info(env, argv[2], &end_key_data, &end_key_size) != napi_ok) {
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
        // Return error code as integer
        napi_value js_result;
        napi_create_int32(env, result, &js_result);
        return js_result;
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
    
    napi_value js_result;
    napi_create_int32(env, result, &js_result);
    return js_result;
}

napi_value js_get_commit_results(napi_env env, napi_callback_info info) {
    const int MAX_BATCH_SIZE = 64;
    commit_result_t batch[MAX_BATCH_SIZE];
    int batch_size;
    size_t total_results = 0;
    
    // Create JavaScript array to hold results
    napi_value js_results;
    napi_create_array(env, &js_results);
    
    // Keep querying until we get fewer results than the batch size
    do {
        batch_size = get_commit_results(batch, MAX_BATCH_SIZE);
        
        if (batch_size < 0 && total_results == 0) {
            // Error or no commits in progress, return undefined
            napi_value code;
            napi_get_undefined(env, &code);
            return code;
        }
        
        // Process this batch and add directly to JavaScript array
        for (int i = 0; i < batch_size; i++) {
            napi_value result_obj, txn_id_val, success_val;
            
            napi_create_object(env, &result_obj);
            napi_create_int32(env, batch[i].ltxn_id, &txn_id_val);
            napi_get_boolean(env, batch[i].success, &success_val);
            
            napi_set_named_property(env, result_obj, "ltxn_id", txn_id_val);
            napi_set_named_property(env, result_obj, "success", success_val);
            
            napi_set_element(env, js_results, total_results++, result_obj);
        }        
    } while (batch_size == MAX_BATCH_SIZE);  // If we got a full batch, there might be more
    
    return js_results;
}

napi_value js_error_code(napi_env env, napi_callback_info info) {
    napi_value result;
    if (error_code[0]) {
        napi_create_string_utf8(env, error_code, NAPI_AUTO_LENGTH, &result);
    } else {
        napi_get_undefined(env, &result);
    }
    return result;
}

napi_value js_error_message(napi_env env, napi_callback_info info) {
    napi_value result;
    if (error_message[0]) {
        napi_create_string_utf8(env, error_message, NAPI_AUTO_LENGTH, &result);
    } else {
        napi_get_undefined(env, &result);
    }
    return result;
}

// Module initialization
napi_value Init(napi_env env, napi_value exports) {
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
        {"closeIterator", js_close_iterator},
        {"getCommitResults", js_get_commit_results},
        {"errorCode", js_error_code},
        {"errorMessage", js_error_message}
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