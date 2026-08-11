#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 <tyrionic> <tyrion>" >&2
  exit 2
fi

compiler=$1
interpreter=$2
script_dir=$(cd -- "$(dirname -- "$0")" && pwd)
fixtures="$script_dir/fixtures"
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/tyrion-cli-diagnostics.XXXXXX")
trap 'rm -rf "$work_dir"' EXIT

expect_failure() {
  local expected=$1
  shift
  local output="$work_dir/failure.out"
  if "$@" >"$output" 2>&1; then
    echo "command unexpectedly succeeded: $*" >&2
    exit 1
  fi
  if ! grep -F -- "$expected" "$output" >/dev/null; then
    echo "missing diagnostic '$expected': $*" >&2
    sed -n '1,20p' "$output" >&2
    exit 1
  fi
}

expect_failure \
  "parser rejected code=syntax-error;" \
  "$compiler" --build "$fixtures/diagnostic_invalid_token.ty" \
  --out "$work_dir/invalid"

expect_failure \
  "lexical error: unexpected character ;" \
  "$interpreter" "$fixtures/diagnostic_invalid_token.ty"

expect_failure \
  "diagnostic boom" \
  "$interpreter" "$fixtures/diagnostic_runtime_exception.ty"

expect_failure \
  "ZeroDivisionError: division by zero" \
  "$interpreter" "$fixtures/diagnostic_zero_division.ty"

"$compiler" --build "$fixtures/diagnostic_zero_division.ty" \
  --out "$work_dir/zero-division"
expect_failure "ZeroDivisionError: division by zero" "$work_dir/zero-division"

valid_interpreted=$("$interpreter" "$fixtures/diagnostic_valid.ty")
if [[ "$valid_interpreted" != "diagnostic ok" ]]; then
  echo "unexpected interpreter output: $valid_interpreted" >&2
  exit 1
fi

"$compiler" --build "$fixtures/diagnostic_valid.ty" --out "$work_dir/valid"
valid_compiled=$("$work_dir/valid")
if [[ "$valid_compiled" != "diagnostic ok" ]]; then
  echo "unexpected compiled output: $valid_compiled" >&2
  exit 1
fi

echo "CLI diagnostics: ok"
