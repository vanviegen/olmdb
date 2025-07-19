#!/usr/bin/env sh
set -e # Exit on error

# Try to find Node.js headers using multiple strategies
NODE_PREFIX=$(node -p "
  const path = require('path');
  const prefix = process.config.variables.node_prefix || 
                 process.env.NODE_PREFIX || 
                 process.env.NPM_CONFIG_PREFIX ||
                 path.dirname(process.execPath);
  // If prefix is root ('/'), try the parent of execPath as base
  prefix === '/' ? path.dirname(path.dirname(process.execPath)) : prefix;
")

# Check for headers in the determined prefix
NODE_INCLUDE="$NODE_PREFIX/include/node"
if [ ! -f "$NODE_INCLUDE/node_api.h" ]; then
    # Try alternative include structure
    NODE_INCLUDE="$NODE_PREFIX/include"
    if [ ! -f "$NODE_INCLUDE/node_api.h" ]; then
        echo "Error: Could not find Node.js headers (node_api.h)" >&2
        echo "Node prefix: $NODE_PREFIX" >&2
        echo "Searched in:" >&2
        echo "  $NODE_PREFIX/include/node" >&2
        echo "  $NODE_PREFIX/include" >&2
        echo "Environment variables:" >&2
        echo "  NODE_PATH: ${NODE_PATH:-not set}" >&2
        echo "  NODE_PREFIX: ${NODE_PREFIX:-not set}" >&2
        echo "  NPM_CONFIG_PREFIX: ${NPM_CONFIG_PREFIX:-not set}" >&2
        exit 1
    fi
fi

build_target() {
    build_name="$1"
    cflags="$2"

    OUTDIR="build/$build_name"
    LMDBDIR="$OUTDIR/lmdb"
    mkdir -p "$LMDBDIR"

    # Compile lmdb object files
    for a in vendor/lmdb/*.c; do
        target="$LMDBDIR/$(basename "$a" .c).o"
        if [ -f "$target" ] && [ "$target" -nt "$a" ]; then
            continue
        fi
        (set -x; gcc $cflags -fPIC -c "$a" -o "$target")
    done

    (
        set -x

        # Build shared object for Node.js
        gcc -fPIC -shared $cflags \
        -I"$NODE_INCLUDE" \
        lowlevel/napi.c \
        lowlevel/transaction_client.c \
        "$LMDBDIR"/*.o \
        -o "$OUTDIR/transaction_client.node"
        
        gcc $cflags \
        lowlevel/commit_worker.c \
        "$LMDBDIR"/*.o \
        -o "$OUTDIR/commit_worker"
    )
}

if [ "$1" != "release" ] ; then build_target debug -g ; fi
if [ "$1" != "debug" ] ; then build_target release -O2 ; fi
