# Builtin Keyword Arguments

[`direct_native_builtin_kwargs.ty`](direct_native_builtin_kwargs.ty) uses
keyword arguments with `enumerate` and `sorted`, including a lambda sort key.

```sh
./build/tyrionic --build ./examples/direct_native_builtin_kwargs.ty --out ./build/builtin_kwargs
./build/builtin_kwargs
```

