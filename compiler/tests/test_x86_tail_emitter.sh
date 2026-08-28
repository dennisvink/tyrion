#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <tyrionc-bootstrap>" >&2
  exit 2
fi

bootstrap=$1
repo_root=$(cd -- "$(dirname -- "$0")/../.." && pwd)
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/tyrion-x86-tail-emitter.XXXXXX")
trap 'rm -rf "$work_dir"' EXIT
assembly="$work_dir/tail-runtime.s"
ln -s "$repo_root/compiler" "$work_dir/compiler"
cp "$repo_root/compiler/tests/x86_tail_emitter_probe.ty" "$work_dir/probe.ty"

size=$("$bootstrap" --exec-call "$work_dir/probe.ty" main "$assembly")
size=${size#return=}
test "$size" -gt 0
if [[ $(uname -s) == "Darwin" || $(uname -m) != "x86_64" ]]; then
  clang --target=x86_64-linux-gnu -c "$assembly" -o "$work_dir/tail-runtime.o"
else
  cc -c "$assembly" -o "$work_dir/tail-runtime.o"
fi
grep -q '^__tyrion_direct_slot_release:$' "$assembly"
grep -q '^__tyrion_native_writer_open:$' "$assembly"
grep -q '^__tyrion_direct_primitive_cache_encode:$' "$assembly"
grep -q '^\.L_tyrion_native_tcp_port_format:$' "$assembly"

echo "x86 tail emitter: capability-selected runtime assembles"
