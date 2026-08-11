# Runtime File IO

[`direct_native_runtime_io.ty`](direct_native_runtime_io.ty) writes, flushes,
reads, strips, and splits a file at runtime. It uses
`/tmp/tyrion_direct_native_runtime_io.txt`.

```sh
./build/tyrionic --build ./examples/direct_native_runtime_io.ty --out ./build/runtime_io
./build/runtime_io
```

