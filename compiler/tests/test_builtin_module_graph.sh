#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <tyrion-compiler>" >&2
  exit 2
fi

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
compiler="$1"
work_dir="$(mktemp -d)"
trap 'rm -rf "$work_dir"' EXIT

python3 - <<'PY' > "$work_dir/python.out"
print(abs(-7))
PY

"$compiler" --build "$repo_root/compiler/tests/import_core.ty" --out "$work_dir/import-core"
"$work_dir/import-core" > "$work_dir/native.out"
cmp "$work_dir/python.out" "$work_dir/native.out"

echo "builtin core module graph: Python and native outputs match"
