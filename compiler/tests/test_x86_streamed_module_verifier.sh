#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <compiler>" >&2
  exit 2
fi

compiler=$1
repo_root=$(cd -- "$(dirname -- "$0")/../.." && pwd)
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/tyrion-x86-streamed-module.XXXXXX")
trap 'rm -rf "$work_dir"' EXIT
output="$work_dir/reversed"
cache_dir="$work_dir/cache"
# The C bootstrap needs an adapter to execute the active compiler source;
# generated successors expose that same compiler as their ordinary CLI.  Keep
# the fixture usable at both boundaries, since the streamed-record ownership
# regression only appeared after a source generation.
compiler_help=$("$compiler" --help 2>&1 || true)
compiler_command=("$compiler")
if [[ "$compiler_help" == *"--selfhost-cli"* ]]; then
  compiler_command+=(--selfhost-cli "$repo_root/tyrionc.ty")
fi

"${compiler_command[@]}" \
  --build "$repo_root/compiler/tests/gap_reversed.ty" \
  --out "$output" \
  --target linux-x86_64 \
  --emit=assembly \
  --ext-static=off \
  --cache-dir "$cache_dir" \
  --cache-stats

test -s "$output.s"
grep -F 'main:' "$output.s" >/dev/null
grep -F '__tyrion_direct_slot_release:' "$output.s" >/dev/null

# The sharded executable lane owns `main` in a function object and the direct
# slot/runtime ABI in the separate root object.  Assemble and link it here so
# an empty streamed operation inventory cannot leave a syntactically valid
# assembly-only result with unresolved root-runtime calls.
native_output="$work_dir/reversed-native"
if [[ "$(uname -s)-$(uname -m)" == "Linux-x86_64" ]]; then
  "${compiler_command[@]}" \
    --build "$repo_root/compiler/tests/gap_reversed.ty" \
    --out "$native_output" \
    --target linux-x86_64 \
    --ext-static=off \
    --cache-dir "$work_dir/native-cache" \
    --cache-stats
  test -x "$native_output"
  "$native_output" >/dev/null
fi

# Force the ordinary aggregate assembly record to miss while preserving every
# verified unit and the new root-tail record. This exercises the intended
# staged warm path rather than letting the whole-program diagnostic cache hide
# it.
find "$cache_dir" -maxdepth 1 -name '*.asmcache' -delete
warm_output="$work_dir/reversed-warm"
"${compiler_command[@]}" \
  --build "$repo_root/compiler/tests/gap_reversed.ty" \
  --out "$warm_output" \
  --target linux-x86_64 \
  --emit=assembly \
  --ext-static=off \
  --cache-dir "$cache_dir" \
  --cache-stats

cmp -s "$output.s" "$warm_output.s"
grep -Fx 'tail-miss' "$cache_dir/.generated-asm-cache-events" >/dev/null
grep -Fx 'tail-hit' "$cache_dir/.generated-asm-cache-events" >/dev/null

# The tail also embeds selected extension ABI/configuration. A differently
# configured compiler capability set must receive a separate tail record,
# rather than accidentally reusing the ordinary application runtime text.
extension_output="$work_dir/toolchain-extension"
"${compiler_command[@]}" \
  --build "$repo_root/compiler/tests/probe_toolchain_load.ty" \
  --out "$extension_output" \
  --target linux-x86_64 \
  --emit=assembly \
  --ext-static=auto \
  --ext-dynamic=allowed \
  --ext-dir "$repo_root/extensions" \
  --cache-dir "$cache_dir" \
  --cache-stats

test -s "$extension_output.s"
test "$(find "$cache_dir" -maxdepth 1 -name '*.tailcache' -type f | wc -l | tr -d ' ')" -eq 2
test "$(grep -Fc 'tail-miss' "$cache_dir/.generated-asm-cache-events")" -eq 2

echo "x86 streamed module verifier: source-driven staged tail and runtime cache passed"
