#!/usr/bin/env bash
set -euo pipefail

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
bootstrap=${1:-"$repo_root/../tyrionc/build/tyrionc"}
source_file="$repo_root/compiler/tests/probe_toolchain_load.ty"
predicate_source="$repo_root/compiler/tests/probe_static_manifest_literal.ty"
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/tyrion-toolchain-static-extension.XXXXXX")
trap 'rm -rf "$work_dir"' EXIT HUP INT TERM

# `startswith` and `endswith` select opposite ends of a string in the direct
# x86 runtime.  The static manifest parser depends on both; keep their exact
# truth table beside the end-to-end extension check so a future dispatch-table
# mismatch fails before a multi-generation successor run.
"$bootstrap" --selfhost-cli "$repo_root/tyrionc.ty" \
    --build "$predicate_source" --out "$work_dir/predicate" \
    --ext-static=off --cache-dir "$work_dir/predicate-cache"
predicate_output=$("$work_dir/predicate")
if [[ $predicate_output != $'True\nTrue\nFalse\nFalse' ]]; then
    echo "x86 static-manifest string predicate mismatch" >&2
    exit 1
fi

# This is deliberately a generic manifest extension, rather than the special
# embedded TLS resource. It protects the self-hosted path where manifest
# scalar strings must outlive subsequent scans of the same TOML source.
"$bootstrap" --selfhost-cli "$repo_root/tyrionc.ty" \
    --build "$source_file" --out "$work_dir/probe" \
    --ext-static=auto --ext-dynamic=allowed --ext-dir "$repo_root/extensions" \
    --cache-dir "$work_dir/cache"
test -x "$work_dir/probe"
"$work_dir/probe" >/dev/null
