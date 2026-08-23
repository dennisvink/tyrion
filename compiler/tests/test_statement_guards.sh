#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 || $# -gt 3 ]]; then
  echo "usage: $0 <tyrion-compiler> <tyrion-interpreter> [bootstrap-compiler]" >&2
  exit 2
fi

compiler="$1"
interpreter="$2"
bootstrap="${3:-}"
repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
work_dir="$(mktemp -d)"
trap 'rm -rf "$work_dir"' EXIT

python3 "$repo_root/compiler/tests/statement_guards_oracle.py" > "$work_dir/python.out"

"$compiler" \
  --build "$repo_root/compiler/tests/statement_guards.ty" \
  --out "$work_dir/statement-guards"
"$work_dir/statement-guards" > "$work_dir/native.out"
"$interpreter" "$repo_root/compiler/tests/statement_guards.ty" > "$work_dir/interpreter.out"

cmp "$work_dir/python.out" "$work_dir/native.out"
cmp "$work_dir/python.out" "$work_dir/interpreter.out"

for source in "$repo_root"/compiler/tests/statement_guard_invalid_*.ty; do
  case "$(basename "$source")" in
    statement_guard_invalid_double.ty)
      expected='parser rejected code=syntax-error;line=1;column=25;detail=KW_UNLESS'
      ;;
    statement_guard_invalid_missing_condition.ty)
      expected='parser rejected code=syntax-error;line=1;column=20;detail=NEWLINE'
      ;;
    statement_guard_invalid_pass.ty)
      expected='parser rejected code=syntax-error;line=1;column=6;detail=KW_IF'
      ;;
    statement_guard_invalid_yield.ty)
      expected='parser rejected code=syntax-error;line=2;column=13;detail=KW_IF'
      ;;
    *)
      echo "unexpected malformed guard fixture: $source" >&2
      exit 1
      ;;
  esac

  if "$compiler" --build "$source" --out "$work_dir/invalid" >"$work_dir/compiler-invalid.out" 2>&1; then
    echo "compiler accepted malformed guard: $source" >&2
    exit 1
  fi
  if ! grep -Fqx "$expected" "$work_dir/compiler-invalid.out"; then
    echo "compiler diagnostic mismatch for $source" >&2
    cat "$work_dir/compiler-invalid.out" >&2
    exit 1
  fi

  if "$interpreter" "$source" >"$work_dir/interpreter-invalid.out" 2>&1; then
    echo "interpreter accepted malformed guard: $source" >&2
    exit 1
  fi
  if ! grep -Fqx "$expected" "$work_dir/interpreter-invalid.out"; then
    echo "interpreter diagnostic mismatch for $source" >&2
    cat "$work_dir/interpreter-invalid.out" >&2
    exit 1
  fi

  if [[ -n "$bootstrap" ]]; then
    if "$bootstrap" --exec-call "$repo_root/tyrionc.ty" build_native "$source" "$work_dir/bootstrap-invalid" >"$work_dir/bootstrap-invalid.out" 2>&1; then
      echo "bootstrap accepted malformed guard: $source" >&2
      exit 1
    fi
    if ! grep -Fq "$expected" "$work_dir/bootstrap-invalid.out"; then
      echo "bootstrap diagnostic mismatch for $source" >&2
      cat "$work_dir/bootstrap-invalid.out" >&2
      exit 1
    fi
  fi
done

echo "statement guards: Python, native compiler, and interpreter outputs match"
echo "statement guards: malformed diagnostics match across available paths"
