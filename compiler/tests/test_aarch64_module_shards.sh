#!/usr/bin/env bash
set -euo pipefail

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
bootstrap=${1:-"$repo_root/../tyrionc/build/tyrionc"}
fixture="$repo_root/compiler/tests/aarch64_module_shards/entry.ty"
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/tyrion-aarch64-module-shards.XXXXXX")
trap 'rm -rf "$work_dir"' EXIT HUP INT TERM

run_cli() {
    "$bootstrap" --selfhost-cli "$repo_root/tyrionc.ty" "$@"
}

# Module compaction is a target-native relocatable-link operation. A non-ARM
# host can still validate individual cross-produced function objects through
# test_emit_objects.sh; this fixture proves the native executable boundary.
case "$(uname -s)-$(uname -m)" in
    Darwin-arm64|Linux-aarch64) ;;
    *) exit 0 ;;
esac

output="$work_dir/module-shards"
cache_dir="$work_dir/cache"
run_cli --build "$fixture" --out "$output" --ext-static=off \
    --cache-dir "$cache_dir" --cache-stats
test -x "$output"
test "$("$output")" = "Hello Sansa!"
test -s "$output.shards/function-objects.list"
test -s "$output.shards/objects.list"

# The core module may be pruned for this small fixture, but helper and entry
# must remain distinct final-link module objects. Their portable sidecars must
# not expose an absolute checkout path.
module_count=$(wc -l < "$output.shards/objects.list" | tr -d ' ')
test "$module_count" -ge 2
while IFS= read -r shard; do
    grep -Fx 'kind=module' "$shard.tyobj" >/dev/null
    ! grep -F "$repo_root" "$shard.tyobj" >/dev/null
done < "$output.shards/objects.list"

hits_before=$(awk '$0 == "hit" { count = count + 1 } END { print count + 0 }' "$cache_dir/.generated-object-cache-events")
run_cli --build "$fixture" --out "$output" --ext-static=off \
    --cache-dir "$cache_dir" --cache-stats
hits_after=$(awk '$0 == "hit" { count = count + 1 } END { print count + 0 }' "$cache_dir/.generated-object-cache-events")
test "$hits_after" -ge "$((hits_before + 3))"
test "$("$output")" = "Hello Sansa!"

# `core` is implicitly available to every graph, but users may also import it
# explicitly. The two occurrences denote the same stable built-in module, so
# its function objects must appear once in the portable inventory and once in
# the target-native compacted module input; passing the same `.o` twice to
# `cc -r` is a duplicate-definition link failure.
core_fixture="$work_dir/explicit-core.ty"
printf '%s\n' \
    'import core' \
    'print(abs(-3))' > "$core_fixture"
core_output="$work_dir/explicit-core"
run_cli --build "$core_fixture" --out "$core_output" --ext-static=off \
    --cache-dir "$cache_dir" --cache-stats
test -x "$core_output"
test "$("$core_output")" = "3"
test -s "$core_output.shards/function-objects.list"
test "$(sort "$core_output.shards/function-objects.list" | uniq | wc -l | tr -d ' ')" = \
    "$(wc -l < "$core_output.shards/function-objects.list" | tr -d ' ')"

# Function shards may require a runtime helper that is not called from the
# root program itself. The root object owns that helper, so it must export the
# direct-runtime ABI rather than leave its definition local. This small
# fixture reaches dict-items from a callable shard while preserving the local
# name through parser/cache-backed compiler data.
ownership_output="$work_dir/native-local-name-ownership"
run_cli --build "$repo_root/compiler/tests/native_local_name_ownership.ty" \
    --out "$ownership_output" --ext-static=off --cache-dir "$cache_dir" \
    --cache-stats
test -x "$ownership_output"
test "$("$ownership_output")" = '0'
nm -g "$ownership_output.o" | grep -E '[[:space:]]__tyrion_app_dict_items$' >/dev/null

# Keep the root-runtime ABI in lock-step with all concrete direct-runtime
# calls the callable emitter can place into a separate object. The one
# `__tyrion_app_builtin_` match is an intentional dynamic prefix used while
# selecting a builtin variant, not an assembler symbol by itself.
root_exports="$work_dir/root-runtime-exports.txt"
nm -g "$ownership_output.o" | awk '$2 != "U" { print $NF }' | sort -u > "$root_exports"
while IFS= read -r runtime_symbol; do
    grep -Fx "$runtime_symbol" "$root_exports" >/dev/null
done < <(
    rg -o 'bl __tyrion_app_[A-Za-z0-9_]+' "$repo_root/compiler/backend/components.ty" \
        | sed 's/.*bl //' | sort -u | grep -Fxv '__tyrion_app_builtin_'
)

echo 'AArch64 module shards: module compaction, explicit core, and root runtime ABI exports passed'
