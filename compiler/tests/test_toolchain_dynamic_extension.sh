#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "$0")" && pwd)
repo_dir=$(cd -- "$script_dir/../.." && pwd)
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/tyrion-toolchain-dynamic.XXXXXX")
trap 'rm -rf "$work_dir"' EXIT

library="$work_dir/libtyrion_ext.so"
link_mode=(-shared -fPIC)
if [[ $(uname -s) == Darwin ]]; then
    library="$work_dir/libtyrion_ext.dylib"
    link_mode=(-dynamiclib)
fi

cc \
    "${link_mode[@]}" \
    -Wall \
    -Wextra \
    -Werror \
    -DTYRION_DYNAMIC_EXTENSION \
    "$repo_dir/extensions/toolchain/src/toolchain.c" \
    "$script_dir/toolchain_dynamic_payload_stub.c" \
    -o "$library"

python3 - "$library" <<'PY'
import ctypes
import sys


class Manifest(ctypes.Structure):
    _fields_ = [
        ("size", ctypes.c_uint64),
        ("abi_major", ctypes.c_uint64),
        ("name", ctypes.c_char_p),
        ("call_symbol", ctypes.c_char_p),
    ]


library = ctypes.CDLL(sys.argv[1])
manifest_function = library.tyrion_host_extension_manifest_v1
manifest_function.restype = ctypes.POINTER(Manifest)
manifest = manifest_function().contents

assert manifest.size >= ctypes.sizeof(Manifest)
assert manifest.abi_major == 1
assert manifest.name == b"toolchain"
assert manifest.call_symbol == b"tyext_toolchain_call_v1"
call = getattr(library, manifest.call_symbol.decode("ascii"))
call.argtypes = [ctypes.c_char_p, ctypes.c_char_p]
call.restype = ctypes.c_char_p
assert call(b"resource.has", b"tls") == b"1"
PY

echo "toolchain dynamic extension: manifest, call symbol, and dispatch pass"
