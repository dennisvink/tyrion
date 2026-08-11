# Multi-Argument Calls

[`direct_native_int_call_abi_branch.ty`](direct_native_int_call_abi_branch.ty)
passes eight integer arguments through an ordinary user-defined function,
exercising arguments beyond register capacity.

```sh
./build/tyrionic --build ./examples/direct_native_int_call_abi_branch.ty --out ./build/int_call_abi
./build/int_call_abi
```

