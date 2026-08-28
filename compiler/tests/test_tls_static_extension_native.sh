#!/usr/bin/env bash
set -euo pipefail

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
bootstrap=${1:-"$repo_root/../tyrionc/build/tyrionc"}
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/tyrion-tls-static-native.XXXXXX")
trap 'rm -rf "$work_dir"' EXIT HUP INT TERM

# This reaches native IR verification after the input program has selected the
# built-in TLS resource.  It protects the ownership boundary between the
# application AST (which staged x86 lowering releases) and static extension
# metadata (which must remain canonical and valid afterwards).
"$bootstrap" --selfhost-cli "$repo_root/tyrionc.ty" \
    --build "$repo_root/compiler/tests/probe_tls_load.ty" \
    --out "$work_dir/tls-static" \
    --emit=assembly \
    --ext-static=auto \
    --ext-dynamic=allowed \
    --ext-dir "$repo_root/extensions"

# Assembly-only output follows the staged artifact boundary: the root runtime
# is emitted separately instead of recreating the retired monolithic `.s`
# file.  TLS calls are root-runtime ABI references, so inspect that artifact.
test -s "$work_dir/tls-static.runtime.s"
grep -F 'tyext_tls_call_v1' "$work_dir/tls-static.runtime.s" >/dev/null

echo 'TLS static extension: native metadata verification and assembly emission passed'
