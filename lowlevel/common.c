// This file must be included rather than linked, because it relies on error handling
// and logging macros that differ based on the context.

#include "common.h"

MDB_env *dbenv;
MDB_dbi dbi;


int init_lmdb(const char *db_dir) {
    // Initialize LMDB environment
    int rc = mdb_env_create(&dbenv);
    if (rc != MDB_SUCCESS) {
        SET_LMDB_ERROR("env create", rc);
        return -1;
    }
    
    rc = mdb_env_set_mapsize(dbenv, 16ULL * 1024ULL * 1024ULL * 1024ULL * 1024ULL); // 16TB
    if (rc != MDB_SUCCESS) {
        mdb_env_close(dbenv);
        dbenv = NULL;
        SET_LMDB_ERROR("set map size", rc);
        return -1;
    }
    
    rc = mdb_env_set_maxreaders(dbenv, 196);
    if (rc != MDB_SUCCESS) {
        mdb_env_close(dbenv);
        dbenv = NULL;
        SET_LMDB_ERROR("set max readers", rc);
        return -1;
    }
    
    rc = mdb_env_open(dbenv, db_dir, MDB_NOTLS, 0664);
    if (rc != MDB_SUCCESS) {
        mdb_env_close(dbenv);
        dbenv = NULL;
        SET_LMDB_ERROR("env init", rc);
        return -1;
    }
    
    // Open the database (within a transaction)
    MDB_txn *wtxn;
    rc = mdb_txn_begin(dbenv, NULL, 0, &wtxn);
    if (rc != MDB_SUCCESS) {
        mdb_env_close(dbenv);
        dbenv = NULL;
        SET_LMDB_ERROR("dbi init txn begin", rc);
        return -1;
    }
    
    rc = mdb_dbi_open(wtxn, NULL, 0, &dbi);
    if (rc != MDB_SUCCESS) {
        mdb_txn_abort(wtxn);
        mdb_env_close(dbenv);
        dbenv = NULL;
        SET_LMDB_ERROR("dbi init", rc);
        return -1;
    }
    
    rc = mdb_txn_commit(wtxn);
    if (rc != MDB_SUCCESS) {
        mdb_env_close(dbenv);
        dbenv = NULL;
        SET_LMDB_ERROR("dbi init txn commit", rc);
        return -1;
    }
    
    return 0;
}

int place_cursor(MDB_cursor *cursor, MDB_val *key, MDB_val *value, int reverse) {
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

// A simple and very fast hash function for checksums
uint64_t checksum(const char *data, size_t len, uint64_t val) {
    if (!data) return 0;
    val ^= len;
    val *= CHECKSUM_PRIME;
    for (size_t i = 0; i < len; i++) {
        val ^= (uint8_t)data[i];
        val *= CHECKSUM_PRIME;
    }
    return val;
}

