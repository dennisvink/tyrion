#!/usr/bin/env bash
set -euo pipefail

binary="${1:-build/examples/direct_native_aggregate_builtins}"

if [[ ! -x "$binary" ]]; then
  echo "macOS aggregate diagnostic binary is missing: $binary" >&2
  exit 1
fi

attempt=1
while [[ $attempt -le 3 ]]; do
  echo "LLDB aggregate crash attempt $attempt"
  lldb -b \
    -o run \
    -k 'register read x0 x1 x2 x3 x19 x20 x21 x22 x23 x24 x25 x26 x27 x28 sp fp lr' \
    -k 'bt all' \
    -k 'disassemble --frame' \
    -- "$binary"
  attempt=$((attempt + 1))
done
