{
  "targets": [
    {
      "target_name": "olmdb_lowlevel",
      "sources": [
        "lowlevel/mdb.c",
        "lowlevel/midl.c",
        "lowlevel/lowlevel.c",
        "lowlevel/lowlevel-napi.c"
      ],
      "include_dirs": [
        "lowlevel"
      ],
      "cflags": [
        "-O2"
      ],
    }
  ]
}