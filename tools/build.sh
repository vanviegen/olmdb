#!/usr/bin/env sh
set -e # Exit on error

# Try to find Node.js headers in common locations
NODE_INCLUDE=$(node -p "process.config.variables.node_prefix + '/include/node'")
if [ ! -f "$NODE_INCLUDE/node_api.h" ]; then
    # Fallback to common paths
    if [ -f "/usr/local/include/node/node_api.h" ]; then
        NODE_INCLUDE="/usr/local/include/node"
    elif [ -f "/usr/include/node/node_api.h" ]; then
        NODE_INCLUDE="/usr/include/node"
    else
        echo "Error: Could not find Node.js headers (node_api.h)" >&2
        echo "Searched in:" >&2
        echo "  $NODE_INCLUDE" >&2
        echo "  /usr/local/include/node" >&2
        echo "  /usr/include/node" >&2
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
