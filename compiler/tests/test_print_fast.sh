#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "usage: $0 <interpreter> <bootstrap> [native-compiler]" >&2
  exit 2
fi

repo_root=$(cd -- "$(dirname -- "$0")/../.." && pwd)
interpreter=$1
bootstrap=$2
native_compiler=${3:-}
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/tyrion-print-fast.XXXXXX")
trap 'rm -rf "$work_dir"' EXIT

fixture="$work_dir/print.ty"
cat >"$fixture" <<'EOF'
print("alpha", "beta", sep="-", end="!")
EOF

printf '%s' 'alpha-beta!' >"$work_dir/python.out"
"$bootstrap" --run "$fixture" >"$work_dir/bootstrap.out"
cmp "$work_dir/python.out" "$work_dir/bootstrap.out"

"$interpreter" "$fixture" >"$work_dir/interpreter.out"
cmp "$work_dir/python.out" "$work_dir/interpreter.out"

if [[ -n "$native_compiler" ]]; then
  native="$work_dir/native"
  "$native_compiler" --build "$fixture" --out "$native" >"$work_dir/native-build.out" 2>"$work_dir/native-build.err"
  "$native" >"$work_dir/native.out"
  cmp "$work_dir/python.out" "$work_dir/native.out"
fi

printf 'print sep/end fast slice: Python, bootstrap, interpreter, and native outputs match\n'
