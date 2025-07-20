{
  "targets": [
    {
      "target_name": "transaction_client",
      "type": "shared_library",
      "product_name": "transaction_client",
      "product_extension": "node",
      "sources": [
        "lowlevel/napi.c",
        "lowlevel/transaction_client.c",
        "vendor/lmdb/mdb.c",
        "vendor/lmdb/midl.c"
      ],
      "include_dirs": [
        "vendor/lmdb"
      ],
      "cflags": [
        "-fPIC",
        "-Wno-unused-but-set-variable"
      ],
      "configurations": {
        "Debug": {
          "cflags": ["-g", "-O0"],
          "defines": ["DEBUG"]
        },
        "Release": {
          "cflags": ["-O2"],
          "defines": ["NDEBUG"]
        }
      }
    },
    {
      "target_name": "commit_worker",
      "type": "executable",
      "product_name": "commit_worker",
      "sources": [
        "lowlevel/commit_worker.c",
        "vendor/lmdb/mdb.c",
        "vendor/lmdb/midl.c"
      ],
      "include_dirs": [
        "vendor/lmdb"
      ],
      "cflags": [
        "-Wno-unused-but-set-variable"
      ],
      "configurations": {
        "Debug": {
          "cflags": ["-g", "-O0"],
          "defines": ["DEBUG"]
        },
        "Release": {
          "cflags": ["-O2"],
          "defines": ["NDEBUG"]
        }
      }
    }
  ]
}
