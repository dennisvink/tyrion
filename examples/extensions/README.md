# Host Extensions

Tyrion can compile C extensions directly into a native application. The
compiler reads each extension manifest, compiles its C sources for the host,
and links the resulting objects into the application executable.

These examples require a host C compiler available as `cc`.

## Layout

This directory is both the example source directory and the extension root:

```text
extensions/
  foo.ty
  foobar.ty
  foobar_wrapper.ty
  rand.ty
  rand_wrapper.ty
  foo/
    ext.toml
    src/foo.c
  foobar/
    ext.toml
    src/foobar.c
  rand/
    ext.toml
    src/rand.c
```

The value passed to `--ext-dir` is scanned for extension directories containing
an `ext.toml` manifest.

## Manifest

A minimal extension manifest looks like this:

```toml
name = "foo"
abi = "tyrion-host-ext/v1"
call_symbol = "tyext_foo_call_v1"
sources = ["src/foo.c"]
```

- `name` is passed to `__tyrion_native_host_load`.
- `abi` selects the current static host-extension ABI.
- `call_symbol` names the C dispatch function.
- `sources` lists C source files relative to the manifest directory.

An extension can contain multiple C sources. An application can also reference
multiple extensions; all referenced extension objects are linked into the same
executable.

The ABI identifier and `__tyrion_*` names are stable internal compatibility
identifiers and retain their existing spelling.

## C Dispatch ABI

An extension exports one dispatch function:

```c
const char *tyext_foo_call_v1(const char *operation, const char *payload);
```

The compiler/runtime passes an operation name and one string payload. The
returned C string is copied into an owned Tyrion string. Return `NULL` when
an operation cannot produce a value.

## Calling An Extension

The low-level Tyrion interface is:

```ty
handle = __tyrion_native_host_load("foo")
result = __tyrion_native_host_call(handle, "foo.echo", "hello")
__tyrion_native_host_unload(handle)
```

Applications normally hide these calls behind a Tyrion wrapper module, as
shown by `foobar_wrapper.ty` and `rand_wrapper.ty`.

## Foo

`foo.ty` demonstrates the raw ABI with deterministic `echo` and `ping`
operations:

```sh
mkdir -p build
./build/tyrionic \
  --build ./examples/extensions/foo.ty \
  --out ./build/extension_foo \
  --ext-static=required \
  --ext-dir ./examples/extensions
./build/extension_foo
```

Expected output:

```text
hello-static
pong
```

`static_lifecycle.ty` additionally verifies that aliases share unload state,
independent loads remain independent, unknown operations return `none`, and
repeated unload is idempotent.

## Foobar

`foobar.ty` imports `foobar_wrapper.ty`. Its C extension randomly changes the
case of alphabetic characters at runtime:

```sh
./build/tyrionic \
  --build ./examples/extensions/foobar.ty \
  --out ./build/extension_foobar \
  --ext-static=required \
  --ext-dir ./examples/extensions
./build/extension_foobar
```

The letter case can differ between executions while the underlying text remains
`barfoo` and `somestring`.

## Rand

`rand.ty` imports `rand_wrapper.ty` and exposes:

- `rand()` for a float in `[0, 1)`
- `rand_u64()` for a non-negative integer
- `rand_int(max_exclusive)` for a bounded integer
- `rand_bool()` for a boolean
- `rand_hex(nbytes)` for random bytes encoded as lowercase hexadecimal

```sh
./build/tyrionic \
  --build ./examples/extensions/rand.ty \
  --out ./build/extension_rand \
  --ext-static=required \
  --ext-dir ./examples/extensions
./build/extension_rand
```

The rand extension uses operating-system randomness and produces new values at
runtime.

## Extension Modes

- `--ext-static=required` requires the program to reference at least one valid
  static extension and fails closed otherwise.
- `--ext-static=auto` links referenced extensions when available.
- `--ext-static=off` rejects static extension calls.
- `--ext-dynamic=off` is the default and disables runtime loading.
- `--ext-dynamic=allowed` permits static-first fallback loading beneath
  `--ext-dir`.

Use an explicit `--ext-dir` when the extension root is not the compiler's
default extension directory.

## Dynamic Extensions

A dynamic library exports the same dispatch function plus this manifest:

```c
struct tyrion_host_extension_manifest_v1 {
  uint64_t size;
  uint64_t abi_major;
  const char *name;
  const char *call_symbol;
};

const struct tyrion_host_extension_manifest_v1 *
tyrion_host_extension_manifest_v1(void);
```

The loader requires a structure of at least 32 bytes, ABI major `1`, an exact
name match, and a non-empty call symbol that resolves in the same library.
Dynamic names are identifiers, never paths. Libraries use this constrained
layout, and dynamic mode requires an absolute `--ext-dir`:

```text
<ext-dir>/<name>/libtyrion_ext.dylib   # macOS
<ext-dir>/<name>/libtyrion_ext.so      # Linux
```

For example, on macOS:

```sh
mkdir -p build/dynamic-extensions/foo
cc -dynamiclib -DTYRION_DYNAMIC_EXTENSION \
  examples/extensions/foo/src/foo.c \
  -o build/dynamic-extensions/foo/libtyrion_ext.dylib
./build/tyrionic \
  --build compiler/tests/fixtures/dynamic_extension_lifecycle.ty \
  --out build/dynamic_lifecycle \
  --ext-static=off \
  --ext-dynamic=allowed \
  --ext-dir "$PWD/build/dynamic-extensions"
./build/dynamic_lifecycle
```

## Verify Static Linking

The extension dispatch symbol should be defined in the final executable, not
left as an unresolved dynamic symbol:

```sh
nm -u ./build/extension_foo | grep tyext_foo_call_v1
```

The command should print nothing.
