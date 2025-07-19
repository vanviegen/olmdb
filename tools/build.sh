#!/usr/bin/env sh
set -e # Exit on error

# Try to find Node.js headers by checking a list of potential directories
NODE_INCLUDE=$(node -p "
  const path = require('path');
  
  // Get potential prefixes in order of preference
  const prefixes = [
    process.config.variables.node_prefix,
    process.env.NODE_PREFIX,
    process.env.NPM_CONFIG_PREFIX,
    path.dirname(process.execPath)
  ].filter(Boolean).map(prefix => 
    // If prefix is root ('/'), try the parent of execPath as base
    prefix === '/' ? path.dirname(path.dirname(process.execPath)) : prefix
  );
  
  // Create list of potential include directories
  const potentialDirs = [];
  
  // Add directories based on prefixes
  prefixes.forEach(prefix => {
    potentialDirs.push(path.join(prefix, 'include/node'));
    potentialDirs.push(path.join(prefix, 'include'));
  });
  
  // Add common default paths
  potentialDirs.push('/usr/local/include/node');
  potentialDirs.push('/usr/include/node');
  potentialDirs.push('/usr/local/include');
  potentialDirs.push('/usr/include');
  
  // Remove duplicates while preserving order
  const uniqueDirs = [...new Set(potentialDirs)];
  
  // Find the first directory that contains node_api.h
  const fs = require('fs');
  for (const dir of uniqueDirs) {
    try {
      if (fs.existsSync(path.join(dir, 'node_api.h'))) {
        console.log(dir);
        process.exit(0);
      }
    } catch (e) {
      // Continue to next directory
    }
  }
  
  // If none found, print error and searched paths
  console.error('Error: Could not find Node.js headers (node_api.h)');
  console.error('Searched in:');
  uniqueDirs.forEach(dir => console.error('  ' + dir));
  process.exit(1);
")

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
