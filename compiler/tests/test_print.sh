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
fixture="$repo_root/compiler/tests/print_kwargs.ty"
oracle="$repo_root/compiler/tests/print_kwargs_oracle.py"
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/tyrion-print.XXXXXX")
trap 'rm -rf "$work_dir"' EXIT

python3 "$oracle" >"$work_dir/python.out"
"$bootstrap" --run "$fixture" >"$work_dir/bootstrap.out"
cmp "$work_dir/python.out" "$work_dir/bootstrap.out"

rm -f /tmp/tyrion-print-gap.txt
"$interpreter" "$fixture" >"$work_dir/interpreter.out"
cmp "$work_dir/python.out" "$work_dir/interpreter.out"

if [[ -n "$native_compiler" ]]; then
  native="$work_dir/native"
  if "$native_compiler" --build "$fixture" --out "$native" >"$work_dir/native-build.out" 2>"$work_dir/native-build.err"; then
    "$native" "$fixture" >"$work_dir/native.out"
    cmp "$work_dir/python.out" "$work_dir/native.out"
  else
    cat "$work_dir/native-build.err" >&2
    exit 1
  fi
fi

printf 'print: Python, bootstrap, and interpreter outputs match\n'
