#!/usr/bin/env sh
set -e # Exit on error

NODE_INCLUDE=$(node -p "process.config.variables.node_prefix + '/include/node'")

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
