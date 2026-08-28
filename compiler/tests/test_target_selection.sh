#!/usr/bin/env bash
set -euo pipefail

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
bootstrap=${1:-"$repo_root/../tyrionc/build/tyrionc"}
source_file="$repo_root/examples/hello.ty"
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/tyrion-target-selection.XXXXXX")
cache_dir="$work_dir/cache"
trap 'rm -rf "$work_dir"' EXIT HUP INT TERM

run_cli() {
    "$bootstrap" --selfhost-cli "$repo_root/tyrionc.ty" "$@"
}

# A named target must emit target assembly without assuming this host can link
# or execute it. Linux x86-64 is intentionally exercised from every host: the
# direct x86 emitter is target-selected rather than host-selected.
run_cli --build "$source_file" --out "$work_dir/linux-x86" \
    --target linux-x86_64 --emit=assembly --ext-static=off \
    --cache-dir "$cache_dir" --cache-stats
test -s "$work_dir/linux-x86.s"
grep -F 'target=linux-x86_64' "$work_dir/linux-x86.tyn" >/dev/null
grep -F 'target-context=tyrion-target-v2:linux-x86_64:toolchain=host-default:sysroot=default' "$work_dir/linux-x86.tyn" >/dev/null
grep -F '# tyrion-native-ir-v2 target=x86_64-linux' "$work_dir/linux-x86.s" >/dev/null
run_cli --build "$source_file" --out "$work_dir/linux-x86-warm" \
    --target linux-x86_64 --emit=assembly --ext-static=off \
    --cache-dir "$cache_dir" --cache-stats
grep -F 'hit' "$cache_dir/.generated-asm-cache-events" >/dev/null

# The host default stays equivalent to an explicit host target for the normal
# assembly-only path. The exact suffix differs between the x86 and AArch64
# builders, so locate the emitted source by contract rather than file suffix.
run_cli --build "$source_file" --out "$work_dir/host-default" \
    --emit=assembly --ext-static=off
run_cli --build "$source_file" --out "$work_dir/host-explicit" \
    --target host --emit=assembly --ext-static=off
default_asm=$(find "$work_dir" -maxdepth 1 -type f \( -name 'host-default.s' -o -name 'host-default.runtime.s' \) -print -quit)
explicit_asm=$(find "$work_dir" -maxdepth 1 -type f \( -name 'host-explicit.s' -o -name 'host-explicit.runtime.s' \) -print -quit)
test -n "$default_asm"
test -n "$explicit_asm"
cmp "$default_asm" "$explicit_asm"
grep -E '^function-unit-identity-count=[0-9]+$' "$work_dir/host-default.tyn" >/dev/null
grep -E '^function-unit-identity-token=tyrion-runtime-native-units-v1-[0-9]+-[0-9]+$' "$work_dir/host-default.tyn" >/dev/null

# Phase 5 starts by assigning the shared AArch64 planner a stable identity
# per lowered callable. Cross-target assembly is sufficient here: no host
# linker or sysroot is needed to prove that a class program has identities and
# a no-change rebuild preserves their aggregate token.
class_source="$repo_root/examples/class.ty"
run_cli --build "$class_source" --out "$work_dir/aarch64-identities-a" \
    --target linux-aarch64 --emit=assembly --ext-static=off
run_cli --build "$class_source" --out "$work_dir/aarch64-identities-b" \
    --target linux-aarch64 --emit=assembly --ext-static=off
grep -E '^function-unit-identity-count=[1-9][0-9]*$' "$work_dir/aarch64-identities-a.tyn" >/dev/null
identity_a=$(sed -n 's/^function-unit-identity-token=//p' "$work_dir/aarch64-identities-a.tyn")
identity_b=$(sed -n 's/^function-unit-identity-token=//p' "$work_dir/aarch64-identities-b.tyn")
test -n "$identity_a"
test "$identity_a" = "$identity_b"

# Per-unit sidecars must preserve an unchanged function's implementation
# identity when an unrelated reachable function is inserted before it in the
# same module. Keep the physical input path stable: distinct source files are
# distinct module identities and are correctly not interchangeable objects.
identity_source="$work_dir/aarch64-stable-identity.ty"
cp "$repo_root/compiler/tests/aarch64_unit_identity_base.ty" "$identity_source"
run_cli --build "$identity_source" \
    --out "$work_dir/aarch64-unit-base" --target linux-aarch64 \
    --emit=assembly --ext-static=off
cp "$repo_root/compiler/tests/aarch64_unit_identity_with_preceding_function.ty" "$identity_source"
run_cli --build "$identity_source" \
    --out "$work_dir/aarch64-unit-preceding" --target linux-aarch64 \
    --emit=assembly --ext-static=off
base_index=$(sed -n 's/^function\.\([0-9][0-9]*\)\.name=stable$/\1/p' "$work_dir/aarch64-unit-base.units")
preceding_index=$(sed -n 's/^function\.\([0-9][0-9]*\)\.name=stable$/\1/p' "$work_dir/aarch64-unit-preceding.units")
test -n "$base_index"
test -n "$preceding_index"
base_token=$(sed -n "s/^function\.$base_index\.token=//p" "$work_dir/aarch64-unit-base.units")
preceding_token=$(sed -n "s/^function\.$preceding_index\.token=//p" "$work_dir/aarch64-unit-preceding.units")
test -n "$base_token"
test "$base_token" = "$preceding_token"

if run_cli --build "$source_file" --out "$work_dir/invalid" \
    --target not-a-target --emit=assembly --ext-static=off >"$work_dir/invalid.out" 2>&1; then
    echo 'invalid target unexpectedly succeeded' >&2
    exit 1
fi
grep -F 'unknown-target-id;target=not-a-target' "$work_dir/invalid.out" >/dev/null

echo 'target selection: explicit target assembly, host default equivalence, and invalid-target rejection passed'
