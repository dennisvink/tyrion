#!/usr/bin/env bash
set -euo pipefail

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
bootstrap=${1:-"$repo_root/../tyrionc/build/tyrionc"}
probe="$repo_root/_probe_module_interface_cache.ty"
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/tyrion-module-interface.XXXXXX")
cache_dir="$work_dir/cache"
cleanup() {
    if [ "${TYRION_TEST_KEEP_WORK_DIR:-}" = '1' ]; then
        echo "preserved test work directory: $work_dir" >&2
    else
        rm -rf "$work_dir"
    fi
}
trap cleanup EXIT HUP INT TERM

# Interface and implementation identities have opposite requirements:
# implementation changes must be local, while a public contract change must
# propagate to importing modules.
test "$("$bootstrap" --exec-call "$probe" probe_private_implementation_edit)" = 'return=1'
test "$("$bootstrap" --exec-call "$probe" probe_public_interface_edit)" = 'return=1'
test "$("$bootstrap" --exec-call "$probe" probe_class_layout_interface_edit)" = 'return=1'
test "$("$bootstrap" --exec-call "$probe" probe_global_interface_edit)" = 'return=1'
test "$("$bootstrap" --exec-call "$probe" probe_function_shard_empty_dependency_field)" = 'return=1'

# The parsed-AST cache identity is source-local.  The first two-module parse
# creates two records; changing only lib's private body creates one new record
# rather than invalidating main's AST record as the former dependency-inclusive
# fingerprint did.
mkdir "$cache_dir"
test "$("$bootstrap" --exec-call "$probe" probe_private_edit_cache_before "$cache_dir")" = 'return=3'
test "$(find "$cache_dir" -maxdepth 1 -name '*.ast' -type f | wc -l | tr -d ' ')" = '2'
test "$("$bootstrap" --exec-call "$probe" probe_private_edit_cache_after "$cache_dir")" = 'return=3'
test "$(find "$cache_dir" -maxdepth 1 -name '*.ast' -type f | wc -l | tr -d ' ')" = '3'

# Exercise the same property at the native lowering boundary. A private body
# edit creates exactly one new lowering record: lib.private. The unchanged
# public function and the importing/root module retain their records.
project_dir="$work_dir/project"
native_cache_dir="$work_dir/native-cache"
mkdir "$project_dir" "$native_cache_dir"
printf '%s\n' \
    'def add_one(value):' \
    '    return value + 1' > "$project_dir/helper.ty"
printf '%s\n' \
    'import helper' \
    'def public(value):' \
    '    return add_one(value)' \
    '' \
    'def private():' \
    '    return 10' > "$project_dir/lib.ty"
printf '%s\n' \
    'import lib' \
    'result = public(4)' > "$project_dir/main.ty"

run_cli() {
    "$bootstrap" --selfhost-cli "$repo_root/tyrionc.ty" "$@"
}

lower_cache_count() {
    find "$native_cache_dir" -maxdepth 1 -name '*.lowercache' -type f | wc -l | tr -d ' '
}

metadata_object_cache_count() {
    grep -l 'tyrion-x86-program-metadata-v3' "$native_cache_dir"/*.objectcache.meta | wc -l | tr -d ' '
}

root_runtime_object_cache_count() {
    grep -l 'tyrion-x86-root-runtime-v3' "$native_cache_dir"/*.objectcache.meta | wc -l | tr -d ' '
}

run_cli --build "$project_dir/main.ty" --out "$work_dir/native-first" \
    --target linux-x86_64 --emit=objects --ext-static=off \
    --cache-dir "$native_cache_dir" --cache-stats
first_lower_cache_count=$(lower_cache_count)
test "$first_lower_cache_count" -gt 0
test "$(metadata_object_cache_count)" = '1'
test "$(root_runtime_object_cache_count)" = '1'
while IFS= read -r shard; do
    sidecar="$shard.tyobj"
    if grep -Eq '^kind=(function|program-entry)$' "$sidecar"; then
        grep -E '^interface-digest=tyrion-interface-digest-v1-[1-9][0-9]*-[0-9]+-[0-9]+$' "$sidecar" >/dev/null
    fi
    if grep -Fx 'kind=program-entry' "$sidecar" >/dev/null; then
        grep -E '^interface-dependency\.0=tyrion-interface-digest-v1-[1-9][0-9]*-[0-9]+-[0-9]+$' "$sidecar" >/dev/null
        grep -Fx 'interface-dependency.count=1' "$sidecar" >/dev/null
    fi
done < "$work_dir/native-first.shards/objects.list"
interface_dependency_sidecar_count=$(grep -l '^interface-dependency.count=1$' "$work_dir/native-first.shards"/*.tyobj | wc -l | tr -d ' ')
test "$interface_dependency_sidecar_count" -ge 3
rm -f "$native_cache_dir/.generated-asm-cache-events"

printf '%s\n' \
    'import helper' \
    'def public(value):' \
    '    return add_one(value)' \
    '' \
    'def private():' \
    '    return 20' > "$project_dir/lib.ty"
run_cli --build "$project_dir/main.ty" --out "$work_dir/native-second" \
    --target linux-x86_64 --emit=objects --ext-static=off \
    --cache-dir "$native_cache_dir" --cache-stats
second_lower_cache_count=$(lower_cache_count)
test "$second_lower_cache_count" -eq "$((first_lower_cache_count + 1))"
test "$(metadata_object_cache_count)" = '1'
test "$(root_runtime_object_cache_count)" = '1'
grep -Fx 'lower-detail;unit=private;kind=function;result=implementation-changed' "$native_cache_dir/.generated-asm-cache-events" >/dev/null
grep -Fx 'lower-detail;unit=public;kind=function;result=cache-hit' "$native_cache_dir/.generated-asm-cache-events" >/dev/null
grep -Fx 'lower-detail;unit=main;kind=main;result=cache-hit' "$native_cache_dir/.generated-asm-cache-events" >/dev/null

# A capability belongs to the separately cached root runtime object, not to
# every function shard. Adding terminal support to the private function must
# therefore lower only that function and create precisely one additional root
# runtime cache record; program metadata remains reusable.
rm -f "$native_cache_dir/.generated-asm-cache-events"
printf '%s\n' \
    'import helper' \
    'def public(value):' \
    '    return add_one(value)' \
    '' \
    'def private():' \
    '    return ansi_escape()' > "$project_dir/lib.ty"
run_cli --build "$project_dir/main.ty" --out "$work_dir/native-capability" \
    --target linux-x86_64 --emit=objects --ext-static=off \
    --cache-dir "$native_cache_dir" --cache-stats
capability_lower_cache_count=$(lower_cache_count)
test "$capability_lower_cache_count" -eq "$((second_lower_cache_count + 1))"
test "$(metadata_object_cache_count)" = '1'
test "$(root_runtime_object_cache_count)" = '2'
grep -Fx 'lower-detail;unit=private;kind=function;result=implementation-changed' "$native_cache_dir/.generated-asm-cache-events" >/dev/null
grep -Fx 'lower-detail;unit=public;kind=function;result=cache-hit' "$native_cache_dir/.generated-asm-cache-events" >/dev/null
grep -Fx 'lower-detail;unit=main;kind=main;result=cache-hit' "$native_cache_dir/.generated-asm-cache-events" >/dev/null

# A signature/default edit changes lib's interface. It must invalidate its
# sibling function as well as the importing program entry, and the cache
# diagnostics must identify that cause rather than reporting opaque misses.
rm -f "$native_cache_dir/.generated-asm-cache-events"
printf '%s\n' \
    'import helper' \
    'def public(value, suffix=1):' \
    '    return add_one(value) + suffix' \
    '' \
    'def private():' \
    '    return 20' > "$project_dir/lib.ty"
run_cli --build "$project_dir/main.ty" --out "$work_dir/native-third" \
    --target linux-x86_64 --emit=objects --ext-static=off \
    --cache-dir "$native_cache_dir" --cache-stats
third_lower_cache_count=$(lower_cache_count)
test "$third_lower_cache_count" -eq "$((capability_lower_cache_count + 3))"
test "$(metadata_object_cache_count)" = '1'
test "$(root_runtime_object_cache_count)" = '2'
grep -Fx 'lower-detail;unit=public;kind=function;result=consumed-interface-changed' "$native_cache_dir/.generated-asm-cache-events" >/dev/null
grep -Fx 'lower-detail;unit=private;kind=function;result=consumed-interface-changed' "$native_cache_dir/.generated-asm-cache-events" >/dev/null
grep -Fx 'lower-detail;unit=main;kind=main;result=consumed-interface-changed' "$native_cache_dir/.generated-asm-cache-events" >/dev/null

echo 'module interface fingerprints: private edits stay local and public contracts propagate passed'
