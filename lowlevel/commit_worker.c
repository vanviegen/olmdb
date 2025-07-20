#include "common.h"
#include <errno.h>
#include <fcntl.h>
#include <linux/limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#define MAX_BATCHED_COMMITS 10240 // Maximum number of ltxn that we try to process in a single wtxn
#define MAX_CLIENTS 256 // Maximum number of processes that can be connected to this worker

#define LOG(fmt, ...) \
    do { \
        if (log_fd >= 0) { \
            dprintf(log_fd, fmt "\n", ##__VA_ARGS__); \
        } \
    } while (0)

#define SET_ERROR(code_str, msg, ...) \
    do { \
        LOG("[%s] " msg, code_str, ##__VA_ARGS__); \
    } while(0)

// Translate Pointer, from JS process address space to our address space
#define TP(ptr) (ptr ? (typeof(ptr))((uintptr_t)(ptr) + shared_memory_displacement): NULL)


typedef struct {
    void *shared_memory;
    uintptr_t shared_memory_displacement; // Where our mapping is located relative to the main thread mapping
    size_t shared_memory_size;
    uint32_t pid;
    uint8_t waiting_for_signal;
} client_t;

typedef struct {
    ltxn_t *ltxn;
    int client_fd;
} queued_commit_t;

client_t clients[MAX_CLIENTS];

static queued_commit_t queued_commits[MAX_BATCHED_COMMITS];
static int queued_commit_count = 0;

static int epoll_fd = -1;
int log_fd = -1; // 

static void perform_queued_commits();
static void handle_client_command(int client_fd);


// We must include this instead of linking it, because of the error handling and logging macros
#include "common.c"


int main(int argc, const char *argv[]) {
    if (argc != 2) {
        fprintf(stderr, "This program should only be invoked by the lowlevel OLMDB module.\n");
        return 1;
    }
    const int socket_fd = 0; // File descriptor for the socket, passed by the main process
    const char *db_dir = argv[1];

    // Daemonize
    setsid(); 
    if (fork() > 0) _exit(0);

    // Start listening (so clients don't get connection refused while we're starting up)
    listen(socket_fd, SOMAXCONN);

    // Set up logging
    char log_path[PATH_MAX];
    char *after = stpncpy(log_path, db_dir, sizeof(log_path) - 1);
    strncat(after, "/commit_worker.log", log_path + sizeof(log_path) - after - 1);
    log_fd = open(log_path, O_WRONLY | O_CREAT | O_APPEND, 0644);

    // Recreate dbenv for our new process
    if (init_lmdb(db_dir) < 0) return 1;

    // Setup epoll
    epoll_fd = epoll_create1(0);
    if (epoll_fd < 0) {
        LOG_INTERNAL_ERROR("Failed to create epoll instance: %s", strerror(errno));
        return -1;
    }

    {
        struct epoll_event event;
        event.events = EPOLLIN;
        event.data.fd = socket_fd;
        if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, socket_fd, &event) < 0) {
            LOG_INTERNAL_ERROR("Failed to add socket to epoll: %s", strerror(errno));
            close(epoll_fd);
            return -1;
        }
    }

    LOG("Commit worker started");

    // Run the server loop
    while (1) {

        int timeout = 10000; // If we have no clients for 10s, we'll want to exit
        if (queued_commit_count) {
            timeout = 0; // Don't block, we have commits to process
        } else {
            for (int i = 0; i < MAX_CLIENTS; i++) {
                if (clients[i].shared_memory) {
                    timeout = -1; // We have clients; wait for commands indefinitely
                    break;
                }
            }
        }

        struct epoll_event events[8];
        int nfds = epoll_wait(epoll_fd, events, 8, timeout);

        if (nfds < 0) {
            if (errno == EINTR) continue; // Interrupted, retry
            LOG_INTERNAL_ERROR("Failed to wait for epoll events: %s", strerror(errno));
            break;
        }

        if (nfds == 0) {
            // Timeout
            if (queued_commit_count) {
                perform_queued_commits();
            } else {
                LOG("No connections for 10 seconds, shutting down commit worker");
                if (dbenv) mdb_env_close(dbenv);
                _exit(0); // Don't call atexit handlers, just exit
            }
            continue;
        }

        for(int i=0; i<nfds; i++) {
            if (events[i].data.fd == socket_fd) {
                // Accept a new client connection
                int client_fd = accept(socket_fd, NULL, NULL);
                if (client_fd < 0) {
                    LOG_INTERNAL_ERROR("Failed to accept client connection: %s", strerror(errno));
                    continue;
                }
                if (client_fd >= MAX_CLIENTS) {
                    LOG_INTERNAL_ERROR("Too many clients, rejecting connection");
                    close(client_fd);
                    continue;
                }

                // Add the new client to the epoll instance
                struct epoll_event client_event;
                client_event.events = EPOLLIN | EPOLLRDHUP; // Read and hangup events
                client_event.data.fd = client_fd;
                if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &client_event) < 0) {
                    LOG_INTERNAL_ERROR("Failed to add client socket to epoll: %s", strerror(errno));
                    close(client_fd);
                    continue;
                }
            } else {
                // Handle data on existing client connection
                handle_client_command(events[i].data.fd);
            }
        }
    }

    close(epoll_fd);
    return 0;
}

static int recv_fd(int socket_fd) {
    struct msghdr msg = {0};
    struct iovec iov[1];
    char data[100];
    
    // Control message buffer
    char control[CMSG_SPACE(sizeof(int))];
    struct cmsghdr *cmsg;
    
    // Set up the message
    iov[0].iov_base = data;
    iov[0].iov_len = sizeof(data);
    
    msg.msg_iov = iov;
    msg.msg_iovlen = 1;
    msg.msg_control = control;
    msg.msg_controllen = sizeof(control);
    
    // Receive the message
    if (recvmsg(socket_fd, &msg, 0) < 0) {
        return -1;
    }
    
    // Extract the file descriptor from control data
    cmsg = CMSG_FIRSTHDR(&msg);
    if (cmsg && cmsg->cmsg_level == SOL_SOCKET && 
        cmsg->cmsg_type == SCM_RIGHTS) {
        return *((int *)CMSG_DATA(cmsg));
    }
    
    return -1;  // No FD received
}

static void handle_client_command(int client_fd) {
    // Do a nonblocking recv
    char buffer[128];
    ssize_t n = recv(client_fd, buffer, sizeof(buffer), MSG_DONTWAIT);

    ASSERT_OR_RETURN(client_fd >= 0 && client_fd < MAX_CLIENTS, );
    client_t *client = &clients[client_fd];

    if (n <= 0) {
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            // Try again later
            return;
        }
        if (n < 0) LOG_INTERNAL_ERROR("Failed to receive data from client fd=%d pid=%d: %s", client_fd, client->pid, strerror(errno));
        else if (client->pid) LOG("Client pid=%d disconnected", client->pid);
        close(client_fd); // This will also deregister from epoll
        if (client->shared_memory) {
            munmap(client->shared_memory, client->shared_memory_size);
            memset(client, 0, sizeof(client_t)); // Reset client state
            for(int i = 0; i < queued_commit_count; i++) {
                if (queued_commits[i].client_fd == client_fd) {
                    queued_commits[i--] = queued_commits[--queued_commit_count]; // Remove from queue
                }
            }
        }
        return;
    }

    if (n==sizeof(init_command_t) && buffer[0]=='i') { // The shared memory file descriptor
        init_command_t *init_cmd = (init_command_t *)buffer;
        int smfd = recv_fd(client_fd);
        if (smfd < 0) {
            LOG_INTERNAL_ERROR("Failed to receive file descriptor from client %d: %s", client_fd, strerror(errno));
            close(client_fd);
            return;
        }
        void *mem = mmap(NULL, init_cmd->mmap_size, PROT_READ | PROT_WRITE, MAP_SHARED, smfd, 0);
        if (mem == MAP_FAILED) {
            LOG_INTERNAL_ERROR("Failed to map shared memory for client %d: %s", client_fd, strerror(errno));
            close(client_fd);
            return;
        }

        client->shared_memory = mem;
        client->shared_memory_size = init_cmd->mmap_size;
        client->shared_memory_displacement = (uintptr_t)mem - (uintptr_t)init_cmd->mmap_ptr;
        client->pid = init_cmd->pid;
        LOG("Client pid=%d initialized", client->pid);
        return;
    }

    if (!client->shared_memory) {
        LOG_INTERNAL_ERROR("Client fd=%d does command=%c len=%zd before init", client_fd, buffer[0], n);
        close(client_fd);
        return;
    }

    uintptr_t shared_memory_displacement = client->shared_memory_displacement;

    if (n==sizeof(commit_command_t) && buffer[0]=='c') { // A logical transaction to be committed
        commit_command_t *commit_command = (commit_command_t *)buffer;
        queued_commits[queued_commit_count].ltxn = TP(commit_command->ltxn);
        queued_commits[queued_commit_count].client_fd = client_fd;
        queued_commit_count++;
        client->waiting_for_signal = 1;

        if (queued_commit_count >= MAX_BATCHED_COMMITS) {
            perform_queued_commits();
        }

        return;
    }

    LOG_INTERNAL_ERROR("Unexpected data from client pid=%d: len=%d type=%c", client->pid, (int)n, buffer[0]);
}

static void error_shutdown() {
    // Let's hope a new worker will be started that is more successful
    LOG("Shutting down commit worker due to unexpected error");
    // Underscore means atexit handlers (from libuv/node) should not be called
    _exit(1);
}

// Validate that all read values haven't changed: returns TRANSACTION_*
static int validate_reads(MDB_txn *wtxn, ltxn_t *ltxn, uintptr_t shared_memory_displacement, MDB_cursor *validation_cursor) {
    read_log_t *current = TP(ltxn->first_read_log);
    MDB_val key, value;
    
    while (current) {
        if (current->row_count != 0) { // Iterator
            int reverse = current->row_count < 0;
            
            key.mv_size = current->key_size;
            key.mv_data = current->key_data;
            int rc = place_cursor(validation_cursor, &key, &value, reverse);
            if (rc != MDB_SUCCESS) {
                LOG_INTERNAL_ERROR("Failed to place cursor for iterator validation: %s", mdb_strerror(rc));
                error_shutdown();
            }
            
            uint64_t cs = CHECKSUM_INITIAL;
            cs = checksum(key.mv_data, key.mv_size, cs);
            cs = checksum(value.mv_data, value.mv_size, cs);
            
            int row_count = abs(current->row_count);
            for(int i = 0; i < row_count; i++) {
                // Read the next item in the LMDB cursor
                int rc = mdb_cursor_get(validation_cursor, &key, &value, reverse ? MDB_PREV : MDB_NEXT);
                if (rc == MDB_NOTFOUND) {
                    key.mv_data = value.mv_data = NULL;
                    key.mv_size = value.mv_size = 0;
                } else if (rc != MDB_SUCCESS) {
                    LOG_INTERNAL_ERROR("Failed to read next item in cursor validation: %s", mdb_strerror(rc));
                    error_shutdown();
                }
                cs = checksum(key.mv_data, key.mv_size, cs);
                cs = checksum(value.mv_data, value.mv_size, cs);
            }
            
            if (cs != current->checksum) {
                return 0; // Validation failed
            }
        } else { // Regular read (row_count == 0)
            key.mv_data = current->key_data;
            key.mv_size = current->key_size;
            int rc = mdb_get(wtxn, dbi, &key, &value);
            uint64_t cs = CHECKSUM_INITIAL;
            if (rc == MDB_NOTFOUND) {
                cs = 0;
            } else if (rc == MDB_SUCCESS) {
                cs = checksum((char *)value.mv_data, value.mv_size, cs);
            } else {
                LOG_INTERNAL_ERROR("Failed to read key '%.*s' during validation: %s", (int)key.mv_size, (char *)key.mv_data, mdb_strerror(rc));
                error_shutdown();
            }
            if (cs != current->checksum) {
                return 0; // Validation failed
            }
        }
        current = TP(current->next_ptr);
    }
    
    return 1; // Validation okay
}

static void perform_updates(MDB_txn *wtxn, ltxn_t *ltxn, uintptr_t shared_memory_displacement) {
    // Process all write entries in skiplist order
    update_log_t *update = TP(ltxn->update_log_skiplist_ptrs[0]);
    
    while (update) {
        MDB_val key;
        key.mv_data = update->data;
        key.mv_size = update->key_size;
        if (update->value_size == 0) {
            // Delete operation
            int rc = mdb_del(wtxn, dbi, &key, NULL);
            if (rc != MDB_SUCCESS && rc != MDB_NOTFOUND) {
                LOG_INTERNAL_ERROR("Failed to delete key %.*s: %s", (int)update->key_size, update->data, mdb_strerror(rc));
                error_shutdown();
            }
        } else {
            // Put operation
            MDB_val value;
            value.mv_data = update->data + update->key_size;
            value.mv_size = update->value_size;
            
            int rc = mdb_put(wtxn, dbi, &key, &value, 0);
            if (rc != MDB_SUCCESS) {
                LOG_INTERNAL_ERROR("Failed to put key %.*s: %s", (int)update->key_size, update->data, mdb_strerror(rc));
                error_shutdown();
            }
        }
        update = TP(update->next_ptrs[0]);
    }
}

static void perform_queued_commits() {
    if (!queued_commit_count) return; // Nothing to process

    int raced = 0, success = 0, notified=0;
    struct timespec start, commit, end;

    clock_gettime(CLOCK_MONOTONIC, &start);

    // Start a wtxn
    MDB_txn *wtxn;
    int rc = mdb_txn_begin(dbenv, NULL, 0, &wtxn);
    if (rc != MDB_SUCCESS) {
        LOG_INTERNAL_ERROR("Failed to begin write transaction: %s", mdb_strerror(rc));
        error_shutdown();
    }

    // Create a cursor, used for iteration validation
    // Cursors within read/write transactions will be closed automatically
    // when the transaction ends
    MDB_cursor *validation_cursor;
    rc = mdb_cursor_open(wtxn, dbi, &validation_cursor);
    if (rc != MDB_SUCCESS) {
        LOG_INTERNAL_ERROR("Failed to open cursor for validation: %s", mdb_strerror(rc));
        error_shutdown();
    }

    // Process all queued ltxns
    for(int i=0; i<queued_commit_count; i++) {
        ltxn_t *ltxn = queued_commits[i].ltxn;
        if (ltxn->state == TRANSACTION_COMMITTING) {
            int client_fd = queued_commits[i].client_fd;
            uintptr_t shared_memory_displacement = clients[client_fd].shared_memory_displacement;
            if (validate_reads(wtxn, ltxn, shared_memory_displacement, validation_cursor)) {
                perform_updates(wtxn, ltxn, shared_memory_displacement);
            } else {
                ltxn->state = TRANSACTION_RACED;
                raced++;
            }
        }
    }

    clock_gettime(CLOCK_MONOTONIC, &commit);

    // Commit the wtxn
    rc = mdb_txn_commit(wtxn);
    if (rc != MDB_SUCCESS) {
        LOG_INTERNAL_ERROR("Failed to commit write transaction: %s", mdb_strerror(rc));
        error_shutdown();
    }

    // Mark all non-raced ltxns as succeeded
    for(int i=0; i<queued_commit_count; i++) {
        ltxn_t *ltxn = queued_commits[i].ltxn;
        // If the transaction wasn't marked as raced, it has succeeded!
        if (ltxn->state == TRANSACTION_COMMITTING) {
            ltxn->state = TRANSACTION_SUCCEEDED; // Mark transaction as succeeded
            success++;
        }
    }

    // Notify all committing clients that their commits have been processed
    for(int i=0; i<queued_commit_count; i++) {
        int client_fd = queued_commits[i].client_fd;

        // int client_fd = queued_commits[i].client_fd;
        if (clients[client_fd].waiting_for_signal) {
            send(client_fd, "s", 1, 0); // Signal that one or multiple commits have been processed
            clients[client_fd].waiting_for_signal = 0;
            notified++;
        }
    }

    clock_gettime(CLOCK_MONOTONIC, &end);
    queued_commit_count = 0;

    LOG("Processed %d succeeded and %d raced commits for %d clients in %lld ms (including %lld ms for commit)", success, raced, notified, 
        (long long)((end.tv_sec - start.tv_sec) * 1000 + (end.tv_nsec - start.tv_nsec) / 1000000),
        (long long)((end.tv_sec - commit.tv_sec) * 1000 + (end.tv_nsec - commit.tv_nsec) / 1000000));
}

