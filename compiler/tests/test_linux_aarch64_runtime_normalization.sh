#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <bootstrap-compiler>" >&2
  exit 2
fi

bootstrap_compiler=$1
script_dir=$(cd -- "$(dirname -- "$0")" && pwd)
repo_dir=$(cd -- "$script_dir/../.." && pwd)

actual=$(
  "$bootstrap_compiler" \
    --exec-call "$repo_dir/tyrionc.ty" \
    runtime_native_linux_aarch64_line \
    "    bl _strlen"
)

expected="return=    bl strlen"
if [[ "$actual" != "$expected" ]]; then
  echo "unexpected Linux AArch64 strlen normalization: $actual" >&2
  exit 1
fi

echo "Linux AArch64 runtime normalization: ok"
