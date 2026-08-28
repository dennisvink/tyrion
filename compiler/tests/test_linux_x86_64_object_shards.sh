#!/usr/bin/env bash
set -euo pipefail

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
bootstrap=${1:-"$repo_root/../tyrionc/build/tyrionc"}
source_file="$repo_root/examples/hello.ty"

if [ "$(uname -s)-$(uname -m)" != "Linux-x86_64" ]; then
    echo 'x86 object shards: skipped (requires native Linux x86-64)'
    exit 0
fi

work_dir=$(mktemp -d "${TMPDIR:-/tmp}/tyrion-x86-object-shards.XXXXXX")
trap 'rm -rf "$work_dir"' EXIT HUP INT TERM

run_cli() {
    "$bootstrap" --selfhost-cli "$repo_root/tyrionc.ty" "$@"
}

test "$("$bootstrap" --exec-call "$repo_root/tyrionc.ty" x86_native_module_shard_partition_count 96)" = 'return=1'
test "$("$bootstrap" --exec-call "$repo_root/tyrionc.ty" x86_native_module_shard_partition_count 97)" = 'return=2'

objects_output="$work_dir/hello-objects"
run_cli --build "$source_file" --out "$objects_output" --target linux-x86_64 \
    --emit=objects --ext-static=off --cache-dir "$work_dir/cache"

test -s "$objects_output.o"
test -s "$objects_output.o.tyobj"
test -s "$objects_output.shards/objects.list"
grep -F 'object-shard-contract=tyrion-object-shard-v2' "$objects_output.tyn" >/dev/null
grep -E '^object-function-input-count=[1-9][0-9]*$' "$objects_output.tyn" >/dev/null
grep -E '^object-module-count=[1-9][0-9]*$' "$objects_output.tyn" >/dev/null
grep -F 'kind=root-runtime' "$objects_output.o.tyobj" >/dev/null
grep -E '^symbol\.[0-9]+=__tyrion_direct_slot_release$' "$objects_output.o.tyobj" >/dev/null
grep -E '^symbol.count=[1-9][0-9]*$' "$objects_output.o.tyobj" >/dev/null

readelf -h "$objects_output.o" | grep -F 'ELF64' >/dev/null
readelf -h "$objects_output.o" | grep -F 'Advanced Micro Devices X86-64' >/dev/null
readelf -S "$objects_output.o" | grep -F '.text' >/dev/null
readelf -S "$objects_output.o" | grep -F '.bss' >/dev/null
nm -g "$objects_output.o" | grep -F '__tyrion_direct_slot_release' >/dev/null

shard_paths=()
main_shard=""
metadata_shard=""
module_shard=""
module_shard_count=0
while IFS= read -r shard; do
    test -s "$shard"
    test -s "$shard.tyobj"
    readelf -h "$shard" | grep -F 'ELF64' >/dev/null
    readelf -h "$shard" | grep -F 'Advanced Micro Devices X86-64' >/dev/null
    readelf -S "$shard" | grep -F '.text' >/dev/null
    if grep -F 'symbol.0=main' "$shard.tyobj" >/dev/null; then
        grep -F 'kind=program-entry' "$shard.tyobj" >/dev/null
        main_shard="$shard"
    elif grep -F 'kind=program-metadata' "$shard.tyobj" >/dev/null; then
        grep -F 'symbol.0=__tyrion_native_process_argc' "$shard.tyobj" >/dev/null
        metadata_shard="$shard"
    elif grep -F 'kind=module' "$shard.tyobj" >/dev/null; then
        grep -E '^symbol.count=[1-9][0-9]*$' "$shard.tyobj" >/dev/null
        module_shard="$shard"
        module_shard_count=$((module_shard_count + 1))
    else
        grep -F 'kind=function' "$shard.tyobj" >/dev/null
    fi
    shard_paths+=("$shard")
done < "$objects_output.shards/objects.list"
test "${#shard_paths[@]}" -gt 0
test -n "$main_shard"
test -n "$metadata_shard"
test "$module_shard_count" -gt 0
nm -g "$main_shard" | grep -E '[[:space:]]main$' >/dev/null
nm -g "$metadata_shard" | grep -E '[[:space:]]__tyrion_native_process_argc$' >/dev/null
nm -g "$module_shard" | grep -E '[[:space:]]__tyrion_unit_' >/dev/null

manual_output="$work_dir/hello-manual-link"
cc "$objects_output.o" "${shard_paths[@]}" -ldl -lm -o "$manual_output"
test "$("$manual_output")" = $'hello world\nmath 5 12 -3'
stack_line=$(readelf -W -l "$manual_output" | grep 'GNU_STACK')
test -n "$stack_line"
if printf '%s\n' "$stack_line" | grep -F 'RWE' >/dev/null; then
    echo 'x86 object shards: final executable unexpectedly requires an executable stack' >&2
    exit 1
fi

native_output="$work_dir/hello-native-link"
run_cli --build "$source_file" --out "$native_output" --target linux-x86_64 \
    --ext-static=auto --cache-dir "$work_dir/native-cache"
test -x "$native_output"
test "$("$native_output")" = $'hello world\nmath 5 12 -3'
grep -F 'native-link-mode=sharded-object-link' "$native_output.tyn" >/dev/null
grep -F 'program-object.count=' "$native_output.link-manifest" >/dev/null
grep -F 'object-bundle-contract=tyrion-x86-object-bundle-v2' "$native_output.tyn" >/dev/null
grep -F 'native-static-extension-count=0' "$native_output.tyn" >/dev/null

# A missing root sidecar makes the bundle questionable. It must take the
# ordinary cached-object path, recreate the sidecar, and never silently accept
# the no-op link. This is deliberately a sidecar failure rather than an ELF
# corruption so the fixture can prove the expected recovery behavior.
rm "$native_output.o.tyobj"
rm -f "$work_dir/native-cache/.generated-asm-cache-events"
run_cli --build "$source_file" --out "$native_output" --target linux-x86_64 \
    --ext-static=auto --cache-dir "$work_dir/native-cache" --cache-stats
test -s "$native_output.o.tyobj"
if grep -Fx 'shard-noop-link' "$work_dir/native-cache/.generated-asm-cache-events" >/dev/null; then
    echo 'x86 object shards: missing sidecar unexpectedly used no-op link' >&2
    exit 1
fi

# A module object is an independently validated final-link input.  Its
# sidecar may be missing or corrupt, but it must be reconstructed from the
# precise per-function object inventory rather than silently accepted by the
# guarded no-op linker path.
module_sidecar=$(find "$native_output.shards" -name 'module-*.o.tyobj' -print -quit)
test -n "$module_sidecar"
rm "$module_sidecar"
rm -f "$work_dir/native-cache/.generated-asm-cache-events"
run_cli --build "$source_file" --out "$native_output" --target linux-x86_64 \
    --ext-static=auto --cache-dir "$work_dir/native-cache" --cache-stats
test -s "$module_sidecar"
if grep -Fx 'shard-noop-link' "$work_dir/native-cache/.generated-asm-cache-events" >/dev/null; then
    echo 'x86 object shards: missing module sidecar unexpectedly used no-op link' >&2
    exit 1
fi

# An unchanged extension-free native build must validate the current objects
# and perform only the final canonical link. This runs before parser/lowering
# entry; the cache-stat event is the focused proof of that fast path.
rm -f "$work_dir/native-cache/.generated-asm-cache-events"
run_cli --build "$source_file" --out "$native_output" --target linux-x86_64 \
    --ext-static=auto --cache-dir "$work_dir/native-cache" --cache-stats
test -x "$native_output"
test "$("$native_output")" = $'hello world\nmath 5 12 -3'
grep -Fx 'shard-noop-link' "$work_dir/native-cache/.generated-asm-cache-events" >/dev/null

# A large source module uses stable callable-identity partitions rather than
# chunks determined by its temporary lowering order. The generated functions
# are intentionally unused: the native planner still emits their explicit
# callable stubs, which gives this fixture a compact, deterministic way to
# exercise the large-module boundary.
partition_source="$work_dir/partitioned-module.ty"
{
    printf 'def selected():\n    return 42\n\n'
    for index in $(seq 1 128); do
        printf 'def partitioned_%s():\n    return %s\n\n' "$index" "$index"
    done
    printf 'print(selected())\n'
} >"$partition_source"
partition_output="$work_dir/partitioned-module"
run_cli --build "$partition_source" --out "$partition_output" --target linux-x86_64 \
    --ext-static=off --cache-dir "$work_dir/partition-cache"
test "$("$partition_output")" = '42'
partition_module_id=$(awk -F '\t' -v path="$partition_source" '$2 == path { print $1; exit }' "$partition_output.shards/function-shards.list")
test -n "$partition_module_id"
partition_module_count=$(find "$partition_output.shards" -name '*.tyobj' -exec grep -l -F "id=module-${partition_module_id}-p" {} + | wc -l | tr -d ' ')
test "$partition_module_count" -gt 1

# Schema v2 adds program metadata, but old root/function sidecars remain
# valid cache inputs. Downgrade this compatible root record in place and prove
# that the guarded no-op lane still accepts it after revalidating ELF symbols.
sed -i '1s/tyrion-object-shard-v2/tyrion-object-shard-v1/' "$native_output.o.tyobj"
rm -f "$work_dir/native-cache/.generated-asm-cache-events"
run_cli --build "$source_file" --out "$native_output" --target linux-x86_64 \
    --ext-static=auto --cache-dir "$work_dir/native-cache" --cache-stats
test -x "$native_output"
test "$("$native_output")" = $'hello world\nmath 5 12 -3'
grep -Fx 'shard-noop-link' "$work_dir/native-cache/.generated-asm-cache-events" >/dev/null

echo 'x86 object shards: ELF validation, manual link, and native manifest link passed'
