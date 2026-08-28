#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo "usage: $0 <tyrion-compiler>" >&2
    exit 2
fi

compiler=$1
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/tyrion-native-local-name.XXXXXX")
trap 'rm -rf "$work_dir"' EXIT HUP INT TERM

# Keep the source deliberately close to the compiler's module construction
# path.  The local must remain discoverable after the call has traversed
# parser/cache-backed data; a borrowed AST slice used to make the following
# index target look like an uninitialized local in a native successor.
repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
source_file="$repo_root/compiler/tests/native_local_name_ownership.ty"

if ! "$compiler" --build "$source_file" --out "$work_dir/module-local" >"$work_dir/build.out" 2>&1; then
    cat "$work_dir/build.out" >&2
    exit 1
fi

test "$("$work_dir/module-local")" = '0'
echo 'native local-name ownership: native compiler preserves module'
