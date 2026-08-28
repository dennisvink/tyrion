#!/usr/bin/env bash
set -euo pipefail

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
bootstrap=${1:-"$repo_root/../tyrionc/build/tyrionc"}
source_file="$repo_root/examples/hello.ty"
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/tyrion-emit-objects.XXXXXX")
cache_dir="$work_dir/cache"
trap 'rm -rf "$work_dir"' EXIT HUP INT TERM

run_cli() {
    "$bootstrap" --selfhost-cli "$repo_root/tyrionc.ty" "$@"
}

output="$work_dir/host"
run_cli --build "$source_file" --out "$output" --emit=objects --ext-static=off \
    --cache-dir "$cache_dir" --cache-stats
test -s "$output.o"
test ! -e "$output"
test ! -e "$output.link-manifest"
grep -F 'program-object='"$output"'.o' "$output.tyn" >/dev/null
grep -F 'object-toolchain=cc-host' "$output.tyn" >/dev/null
grep -F 'native-link-mode=object-only' "$output.tyn" >/dev/null
run_cli --build "$source_file" --out "$output" --emit=objects --ext-static=off \
    --cache-dir "$cache_dir" --cache-stats
grep -F 'hit' "$cache_dir/.generated-object-cache-events" >/dev/null
cache_object=$(find "$cache_dir" -maxdepth 1 -name '*.objectcache.o' -print -quit)
test -n "$cache_object"
printf 'corrupt object cache fixture\n' > "$cache_object"
run_cli --build "$source_file" --out "$output" --emit=objects --ext-static=off \
    --cache-dir "$cache_dir" --cache-stats
test -s "$output.o"
file "$output.o" | grep -E 'Mach-O 64-bit object|ELF 64-bit' >/dev/null
grep -F 'corrupt' "$cache_dir/.generated-object-cache-events" >/dev/null
rm -f "$cache_object"
run_cli --build "$source_file" --out "$output" --emit=objects --ext-static=off \
    --cache-dir "$cache_dir" --cache-stats
test -s "$output.o"
test -s "$cache_object"
tail -n 1 "$cache_dir/.generated-object-cache-events" | grep -Fx 'miss' >/dev/null

# A final-link failure must retain the canonical manifest and report the
# target linker diagnostic.  Passing a directory as the requested executable
# is a harmless, deterministic way to force that boundary on every host.
link_failure_output="$work_dir/link-output-directory"
link_failure_log="$work_dir/link-failure.log"
mkdir -p "$link_failure_output"
if run_cli --build "$source_file" --out "$link_failure_output" --ext-static=off \
    --cache-dir "$cache_dir" >"$link_failure_log" 2>&1; then
    echo 'expected final-link failure for directory output path' >&2
    exit 1
fi
grep -F 'code=link-failed' "$link_failure_log" >/dev/null
grep -F 'diagnostic=' "$link_failure_log" >/dev/null

# Linux object production uses Clang's target assembler. Linking remains a
# separate target-native operation, so this verifies a real ELF object without
# pretending a macOS host can produce a Linux executable.
if [ "$(uname -s)-$(uname -m)" != "Linux-x86_64" ]; then
    cross_output="$work_dir/linux-x86"
    run_cli --build "$source_file" --out "$cross_output" \
        --target linux-x86_64 --emit=objects --ext-static=off
    test -s "$cross_output.o"
    test -s "$cross_output.o.tyobj"
    test ! -e "$cross_output"
    grep -F 'target=linux-x86_64' "$cross_output.tyn" >/dev/null
    grep -F 'object-toolchain=clang-target-x86_64-unknown-linux-gnu' "$cross_output.tyn" >/dev/null
    grep -F "object-shard-list=$cross_output.shards/objects.list" "$cross_output.tyn" >/dev/null
    grep -E '^object-shard-count=[1-9][0-9]*$' "$cross_output.tyn" >/dev/null
    grep -E '^object-function-input-count=[1-9][0-9]*$' "$cross_output.tyn" >/dev/null
    grep -Fx 'object-module-count=0' "$cross_output.tyn" >/dev/null
    grep -F 'object-shard-contract=tyrion-object-shard-v2' "$cross_output.tyn" >/dev/null
    grep -F 'native-link-mode=object-only' "$cross_output.tyn" >/dev/null
    file "$cross_output.o" | grep -F 'ELF 64-bit' >/dev/null
    file "$cross_output.o" | grep -F 'x86-64' >/dev/null
    test -s "$cross_output.shards/objects.list"
    grep -F 'tyrion-object-shard-v2' "$cross_output.o.tyobj" >/dev/null
    grep -F 'kind=root-runtime' "$cross_output.o.tyobj" >/dev/null
    grep -E '^symbol\.[0-9]+=__tyrion_direct_slot_release$' "$cross_output.o.tyobj" >/dev/null
    grep -E '^symbol.count=[1-9][0-9]*$' "$cross_output.o.tyobj" >/dev/null
    cp "$cross_output.shards/objects.list" "$work_dir/linux-x86-first-objects.list"
    metadata_shard=""
    while IFS= read -r shard; do
        test -s "$shard"
        test -s "$shard.tyobj"
        file "$shard" | grep -F 'ELF 64-bit' >/dev/null
        file "$shard" | grep -F 'x86-64' >/dev/null
        grep -F 'tyrion-object-shard-v2' "$shard.tyobj" >/dev/null
        if grep -F 'symbol.0=main' "$shard.tyobj" >/dev/null; then
            grep -F 'kind=program-entry' "$shard.tyobj" >/dev/null
        elif grep -F 'kind=program-metadata' "$shard.tyobj" >/dev/null; then
            grep -F 'symbol.0=__tyrion_native_process_argc' "$shard.tyobj" >/dev/null
            metadata_shard="$shard"
        else
            grep -F 'kind=function' "$shard.tyobj" >/dev/null
        fi
    done < "$cross_output.shards/objects.list"
    test -n "$metadata_shard"
    # A valid function object cache hit must not require recreating the
    # function's standalone `.s` source.  Leave the root assembly alone: it
    # still owns the transition-era runtime/startup aggregate.
    while IFS= read -r shard; do
        rm -f "${shard%.o}.s"
    done < "$cross_output.shards/objects.list"
    run_cli --build "$source_file" --out "$cross_output" \
        --target linux-x86_64 --emit=objects --ext-static=off
    cmp "$work_dir/linux-x86-first-objects.list" "$cross_output.shards/objects.list"
    while IFS= read -r shard; do
        test ! -e "${shard%.o}.s"
    done < "$cross_output.shards/objects.list"
fi

# The shared AArch64 path emits callable boundaries as independently assembled
# ELF/Mach-O objects. Cross-target production validates the relocatable object
# contract without assuming this host can perform the final target-native link.
aarch64_output="$work_dir/linux-aarch64"
class_source="$repo_root/examples/class.ty"
run_cli --build "$class_source" --out "$aarch64_output" \
    --target linux-aarch64 --emit=objects --ext-static=off \
    --cache-dir "$cache_dir" --cache-stats
test -s "$aarch64_output.o"
# The normal object lane must not fall back to the transition-era monolithic
# assembly cache. Its root and callable objects have independent exact keys.
test -z "$(find "$cache_dir" -maxdepth 1 -name '*.asmcache' -print -quit)"
grep -F 'target=linux-aarch64' "$aarch64_output.tyn" >/dev/null
# macOS cross-assembles Linux ARM64 with Clang's target mode; a native Linux
# ARM64 host correctly uses its own system compiler driver instead.
aarch64_object_toolchain='clang-target-aarch64-unknown-linux-gnu'
if [ "$(uname -s)" = Linux ] && { [ "$(uname -m)" = aarch64 ] || [ "$(uname -m)" = arm64 ]; }; then
    aarch64_object_toolchain='cc-host'
fi
grep -F "object-toolchain=$aarch64_object_toolchain" "$aarch64_output.tyn" >/dev/null
test -s "$aarch64_output.o.tyobj"
grep -F 'tyrion-object-shard-v2' "$aarch64_output.o.tyobj" >/dev/null
grep -F 'kind=root-runtime' "$aarch64_output.o.tyobj" >/dev/null
grep -E '^symbol\.[0-9]+=__tyrion_app_slot_release$' "$aarch64_output.o.tyobj" >/dev/null
grep -F "function-object-shard-list=$aarch64_output.shards/objects.list" "$aarch64_output.tyn" >/dev/null
grep -E '^function-object-shard-count=[1-9][0-9]*$' "$aarch64_output.tyn" >/dev/null
test -s "$aarch64_output.shards/objects.list"
while IFS= read -r shard; do
    test -s "$shard"
    test -s "$shard.tyobj"
    file "$shard" | grep -F 'ELF 64-bit' >/dev/null
    file "$shard" | grep -E 'ARM aarch64|ARM64' >/dev/null
    grep -F 'tyrion-object-shard-v2' "$shard.tyobj" >/dev/null
    grep -F 'kind=function' "$shard.tyobj" >/dev/null
done < "$aarch64_output.shards/objects.list"

# On a native AArch64 host the production executable path must link the
# root-only runtime object with those callable shards and run the result. This
# same assertion is exercised on macOS ARM64 and in the Linux ARM64 container.
host_machine=$(uname -m)
if [ "$host_machine" = "arm64" ] || [ "$host_machine" = "aarch64" ]; then
    aarch64_executable="$work_dir/aarch64-sharded-executable"
    run_cli --build "$class_source" --out "$aarch64_executable" --ext-static=off \
        --cache-dir "$cache_dir" --cache-stats
    test -x "$aarch64_executable"
    test -s "$aarch64_executable.shards/objects.list"
    test -s "$aarch64_executable.link-manifest"
    grep -F "$aarch64_executable.o" "$aarch64_executable.link-manifest" >/dev/null
    while IFS= read -r executable_shard; do
        grep -F "$executable_shard" "$aarch64_executable.link-manifest" >/dev/null
    done < "$aarch64_executable.shards/objects.list"
    test "$("$aarch64_executable")" = "Hello Sansa I'm Arya"
    class_hits_before=$(awk '$0 == "hit" { count = count + 1 } END { print count + 0 }' "$cache_dir/.generated-object-cache-events")
    run_cli --build "$class_source" --out "$aarch64_executable" --ext-static=off \
        --cache-dir "$cache_dir" --cache-stats
    class_hits_after=$(awk '$0 == "hit" { count = count + 1 } END { print count + 0 }' "$cache_dir/.generated-object-cache-events")
    # Root plus the constructor and method should all be exact-object hits;
    # final linking still runs through the canonical manifest.
    test "$class_hits_after" -ge "$((class_hits_before + 3))"
    test "$("$aarch64_executable")" = "Hello Sansa I'm Arya"

    # Cache validation is part of the object boundary: corrupting the cached
    # root runtime must rebuild that one object, retain callable cache hits,
    # and still produce a runnable manifest-linked executable.
    root_checksum=$(shasum "$aarch64_executable.o" | awk '{ print $1 }')
    root_cache=""
    for object_cache in "$cache_dir"/*.objectcache.o; do
        if [ "$(shasum "$object_cache" | awk '{ print $1 }')" = "$root_checksum" ]; then
            root_cache="$object_cache"
            break
        fi
    done
    test -n "$root_cache"
    printf 'corrupt AArch64 root object fixture\n' > "$root_cache"
    run_cli --build "$class_source" --out "$aarch64_executable" --ext-static=off \
        --cache-dir "$cache_dir" --cache-stats
    grep -F 'corrupt' "$cache_dir/.generated-object-cache-events" >/dev/null
    test "$("$aarch64_executable")" = "Hello Sansa I'm Arya"

    # Exercise the root ABI used by real callable bodies rather than only a
    # constructor/method pair: arithmetic, comparisons, indexing, lists,
    # slices, dictionaries, object attributes, and printing all cross from a
    # function shard into the root runtime.
    helper_executable="$work_dir/aarch64-sharded-runtime-helpers"
    run_cli --build "$repo_root/examples/direct_native_class_runtime_helpers.ty" \
        --out "$helper_executable" --ext-static=off --cache-dir "$cache_dir" \
        --cache-stats
    helper_expected=$'class 4 8 8\nlist 2 5\ndict b a 2 1\nstr tyrion-lang.ty True b'
    test "$("$helper_executable")" = "$helper_expected"

    # Floats use root-owned text literals and ADRP/LO12 references from
    # callable objects. Compare the native result to Python so the stable
    # literal-label migration exercises actual conversion and arithmetic, not
    # merely a relocatable link.
    scalar_source="$repo_root/examples/direct_native_scalar_expressions.ty"
    scalar_executable="$work_dir/aarch64-sharded-scalars"
    python3 "$scalar_source" > "$work_dir/scalars.python.out"
    run_cli --build "$scalar_source" --out "$scalar_executable" --ext-static=off \
        --cache-dir "$cache_dir" --cache-stats
    "$scalar_executable" > "$work_dir/scalars.native.out"
    cmp "$work_dir/scalars.python.out" "$work_dir/scalars.native.out"

    # An unchanged callable must keep both its source-stable ABI label and its
    # cached object when an unrelated declaration is inserted before it. The
    # two contents are written to one stable module path: different checkout
    # paths are deliberately different module identities and must not share a
    # callable ABI label.
    stable_base="$work_dir/aarch64-stable-unit-base"
    stable_preceding="$work_dir/aarch64-stable-unit-preceding"
    stable_identity_source="$work_dir/aarch64-stable-identity.ty"
    cp "$repo_root/compiler/tests/aarch64_unit_identity_base.ty" "$stable_identity_source"
    run_cli --build "$stable_identity_source" \
        --out "$stable_base" --ext-static=off --cache-dir "$cache_dir" \
        --cache-stats
    test "$("$stable_base")" = "2"
    stable_base_index=$(sed -n 's/^function\.\([0-9][0-9]*\)\.name=stable$/\1/p' "$stable_base.units")
    stable_base_token=$(sed -n "s/^function\.$stable_base_index\.token=//p" "$stable_base.units")
    test -n "$stable_base_token"
    stable_hits_before=$(awk '$0 == "hit" { count = count + 1 } END { print count + 0 }' "$cache_dir/.generated-object-cache-events")
    cp "$repo_root/compiler/tests/aarch64_unit_identity_with_preceding_function.ty" "$stable_identity_source"
    run_cli --build "$stable_identity_source" \
        --out "$stable_preceding" --ext-static=off --cache-dir "$cache_dir" \
        --cache-stats
    stable_preceding_index=$(sed -n 's/^function\.\([0-9][0-9]*\)\.name=stable$/\1/p' "$stable_preceding.units")
    stable_preceding_token=$(sed -n "s/^function\.$stable_preceding_index\.token=//p" "$stable_preceding.units")
    test "$stable_base_token" = "$stable_preceding_token"
    test "$("$stable_preceding")" = $'1\n2'
    stable_hits_after=$(awk '$0 == "hit" { count = count + 1 } END { print count + 0 }' "$cache_dir/.generated-object-cache-events")
    test "$stable_hits_after" -gt "$stable_hits_before"

    # Anonymous lambdas use the same source-stable identity rule. Their
    # generated display names are ordinal, so assert the manifest token and
    # the two object-cache reuses rather than the temporary display name.
    lambda_base="$work_dir/aarch64-lambda-unit-base"
    lambda_preceding="$work_dir/aarch64-lambda-unit-preceding"
    lambda_identity_source="$work_dir/aarch64-lambda-identity.ty"
    cp "$repo_root/compiler/tests/aarch64_lambda_identity_base.ty" "$lambda_identity_source"
    run_cli --build "$lambda_identity_source" \
        --out "$lambda_base" --ext-static=off --cache-dir "$cache_dir" \
        --cache-stats
    test "$("$lambda_base")" = "[1]"
    lambda_base_index=$(sed -n 's/^function\.\([0-9][0-9]*\)\.name=__tyrion_lambda_[0-9][0-9]*$/\1/p' "$lambda_base.units")
    lambda_base_token=$(sed -n "s/^function\.$lambda_base_index\.token=//p" "$lambda_base.units")
    test -n "$lambda_base_token"
    lambda_hits_before=$(awk '$0 == "hit" { count = count + 1 } END { print count + 0 }' "$cache_dir/.generated-object-cache-events")
    cp "$repo_root/compiler/tests/aarch64_lambda_identity_with_preceding_function.ty" "$lambda_identity_source"
    run_cli --build "$lambda_identity_source" \
        --out "$lambda_preceding" --ext-static=off --cache-dir "$cache_dir" \
        --cache-stats
    lambda_preceding_index=$(sed -n 's/^function\.\([0-9][0-9]*\)\.name=__tyrion_lambda_[0-9][0-9]*$/\1/p' "$lambda_preceding.units")
    lambda_preceding_token=$(sed -n "s/^function\.$lambda_preceding_index\.token=//p" "$lambda_preceding.units")
    test "$lambda_base_token" = "$lambda_preceding_token"
    test "$("$lambda_preceding")" = $'1\n[1]'
    lambda_hits_after=$(awk '$0 == "hit" { count = count + 1 } END { print count + 0 }' "$cache_dir/.generated-object-cache-events")
    test "$lambda_hits_after" -ge "$((lambda_hits_before + 2))"

    # Class methods have stable labels too. An ordinary top-level function
    # inserted before the class must not renumber its literal references or
    # invalidate either method object.
    method_base="$work_dir/aarch64-method-unit-base"
    method_preceding="$work_dir/aarch64-method-unit-preceding"
    method_identity_source="$work_dir/aarch64-method-identity.ty"
    cp "$repo_root/compiler/tests/aarch64_method_identity_base.ty" "$method_identity_source"
    run_cli --build "$method_identity_source" \
        --out "$method_base" --ext-static=off --cache-dir "$cache_dir" \
        --cache-stats
    test "$("$method_base")" = "Hello Sansa I'm Arya"
    method_base_index=$(sed -n 's/^function\.\([0-9][0-9]*\)\.name=Greeter\.greet$/\1/p' "$method_base.units")
    method_base_token=$(sed -n "s/^function\.$method_base_index\.token=//p" "$method_base.units")
    test -n "$method_base_token"
    method_hits_before=$(awk '$0 == "hit" { count = count + 1 } END { print count + 0 }' "$cache_dir/.generated-object-cache-events")
    cp "$repo_root/compiler/tests/aarch64_method_identity_with_preceding_function.ty" "$method_identity_source"
    run_cli --build "$method_identity_source" \
        --out "$method_preceding" --ext-static=off --cache-dir "$cache_dir" \
        --cache-stats
    method_preceding_index=$(sed -n 's/^function\.\([0-9][0-9]*\)\.name=Greeter\.greet$/\1/p' "$method_preceding.units")
    method_preceding_token=$(sed -n "s/^function\.$method_preceding_index\.token=//p" "$method_preceding.units")
    test "$method_base_token" = "$method_preceding_token"
    test "$("$method_preceding")" = $'winter\nHello Sansa I\'m Arya'
    method_hits_after=$(awk '$0 == "hit" { count = count + 1 } END { print count + 0 }' "$cache_dir/.generated-object-cache-events")
    test "$method_hits_after" -ge "$((method_hits_before + 2))"

    # Link every direct-native fixture through the root-plus-callable-object
    # path. They cover the supported lowering surface more broadly than the
    # two executable smoke fixtures without making the test depend on fixture
    # I/O or process side effects.
    linkset_dir="$work_dir/aarch64-direct-native-linkset"
    mkdir -p "$linkset_dir"
    for native_source in "$repo_root"/examples/direct_native_*.ty; do
        native_name=$(basename "$native_source" .ty)
        run_cli --build "$native_source" --out "$linkset_dir/$native_name" \
            --ext-static=off --cache-dir "$cache_dir" --cache-stats
        test -x "$linkset_dir/$native_name"
        # Some direct-runtime fixtures have no user-defined callable shard:
        # their root/runtime object is still a valid object-based executable.
        # The manifest must exist, but can legitimately be empty in that case.
        test -e "$linkset_dir/$native_name.shards/objects.list"
    done

    # Terminal chess reaches a separate group of callable/runtime boundaries
    # (ANSI, containment, conversion, range, and set indexing). It is linked
    # but not run here because it deliberately waits for terminal input.
    terminal_executable="$work_dir/aarch64-terminal-chess"
    run_cli --build "$repo_root/examples/terminal_chess.ty" \
        --out "$terminal_executable" --ext-static=off --cache-dir "$cache_dir" \
        --cache-stats
    test -x "$terminal_executable"
    test -s "$terminal_executable.shards/objects.list"
    grep -F "$terminal_executable.o" "$terminal_executable.link-manifest" >/dev/null

    # Full-feature coverage combines collections, classes, lambdas,
    # comprehensions, exceptions, file I/O, and ordinary runtime helpers in a
    # much larger caller graph. Compile and link it without running because
    # the fixture intentionally writes a fixed `/tmp` demonstration file.
    full_feature_executable="$work_dir/aarch64-full-feature"
    run_cli --build "$repo_root/examples/full_feature_test.ty" \
        --out "$full_feature_executable" --ext-static=off --cache-dir "$cache_dir" \
        --cache-stats
    test -x "$full_feature_executable"
    test -s "$full_feature_executable.shards/objects.list"
    grep -F "$full_feature_executable.o" "$full_feature_executable.link-manifest" >/dev/null
fi

echo 'emit objects: native root plus Linux x86-64 and AArch64 function-shard objects passed'
