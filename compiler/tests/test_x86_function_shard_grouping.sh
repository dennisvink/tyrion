#!/usr/bin/env bash
set -euo pipefail

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
bootstrap=${1:-"$repo_root/../tyrionc/build/tyrionc"}
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/tyrion-x86-function-shard-grouping.XXXXXX")
trap 'rm -rf "$work_dir"' EXIT HUP INT TERM
ln -s "$repo_root/compiler" "$work_dir/compiler"
cp "$repo_root/compiler/tests/probe_x86_function_shard_grouping.ty" "$work_dir/probe.ty"
cp "$repo_root/compiler/tests/probe_x86_function_shard_grouping_invalid.ty" "$work_dir/invalid-probe.ty"

# Run the imported compiler helper directly through the C bootstrap. This is
# intentionally a fast, host-independent check: native object/link coverage
# belongs to test_linux_x86_64_object_shards.sh on a Linux x86-64 host.
actual=$("$bootstrap" --run "$work_dir/probe.ty")
expected=$'2\ncompiler/a.ty\ncompiler/b.ty\ncache-a1\ncache-a2\nunit_b1'
test "$actual" = "$expected"

if "$bootstrap" --run "$work_dir/invalid-probe.ty" >"$work_dir/invalid.out" 2>&1; then
    echo 'x86 function-shard grouping: inconsistent interface digest unexpectedly succeeded' >&2
    exit 1
fi
grep -F 'inconsistent-module-interface-digest;module=compiler/a.ty' "$work_dir/invalid.out" >/dev/null

echo 'x86 function-shard grouping: deterministic indexed grouping passed'
