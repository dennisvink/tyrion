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

input_bytes=$'Arya\n\nSansa\r\n'
printf '%s' "$input_bytes" | python3 "$repo_root/compiler/tests/input_parity.ty" > "$work_dir/python.out"

"$compiler" --build "$repo_root/compiler/tests/input_parity.ty" --out "$work_dir/input-parity"
printf '%s' "$input_bytes" | "$work_dir/input-parity" > "$work_dir/native.out"
printf '%s' "$input_bytes" | "$interpreter" "$repo_root/compiler/tests/input_parity.ty" > "$work_dir/interpreter.out"

cmp "$work_dir/python.out" "$work_dir/native.out"
cmp "$work_dir/python.out" "$work_dir/interpreter.out"

python3 "$repo_root/compiler/tests/input_pty_driver.py" python3 "$repo_root/compiler/tests/input_parity.ty" > "$work_dir/python-pty.out"
python3 "$repo_root/compiler/tests/input_pty_driver.py" "$work_dir/input-parity" > "$work_dir/native-pty.out"
python3 "$repo_root/compiler/tests/input_pty_driver.py" "$interpreter" "$repo_root/compiler/tests/input_parity.ty" > "$work_dir/interpreter-pty.out"
cmp "$work_dir/python-pty.out" "$work_dir/native-pty.out"
cmp "$work_dir/python-pty.out" "$work_dir/interpreter-pty.out"

if [[ -n "$bootstrap" ]]; then
  printf '%s' "$input_bytes" | "$bootstrap" --run "$repo_root/compiler/tests/input_parity.ty" > "$work_dir/bootstrap.out"
  cmp "$work_dir/python.out" "$work_dir/bootstrap.out"
  python3 "$repo_root/compiler/tests/input_pty_driver.py" "$bootstrap" --run "$repo_root/compiler/tests/input_parity.ty" > "$work_dir/bootstrap-pty.out"
  cmp "$work_dir/python-pty.out" "$work_dir/bootstrap-pty.out"
fi

printf 'north' | python3 "$repo_root/compiler/tests/input_partial.ty" > "$work_dir/python-partial.out"
"$compiler" --build "$repo_root/compiler/tests/input_partial.ty" --out "$work_dir/input-partial"
printf 'north' | "$work_dir/input-partial" > "$work_dir/native-partial.out"
printf 'north' | "$interpreter" "$repo_root/compiler/tests/input_partial.ty" > "$work_dir/interpreter-partial.out"
cmp "$work_dir/python-partial.out" "$work_dir/native-partial.out"
cmp "$work_dir/python-partial.out" "$work_dir/interpreter-partial.out"

if [[ -n "$bootstrap" ]]; then
  printf 'north' | "$bootstrap" --run "$repo_root/compiler/tests/input_partial.ty" > "$work_dir/bootstrap-partial.out"
  cmp "$work_dir/python-partial.out" "$work_dir/bootstrap-partial.out"
fi

"$compiler" --build "$repo_root/compiler/tests/input_eof.ty" --out "$work_dir/input-eof"
if "$work_dir/input-eof" </dev/null >"$work_dir/native-eof.out" 2>"$work_dir/native-eof.err"; then
  echo "native input accepted immediate EOF" >&2
  exit 1
fi
if [[ "$(cat "$work_dir/native-eof.out")" != "EOF: " ]]; then
  echo "native input EOF prompt mismatch" >&2
  exit 1
fi
grep -Fqx 'EOFError: EOF when reading a line' "$work_dir/native-eof.err"

if "$interpreter" "$repo_root/compiler/tests/input_eof.ty" </dev/null >"$work_dir/interpreter-eof.out" 2>"$work_dir/interpreter-eof.err"; then
  echo "interpreter input accepted immediate EOF" >&2
  exit 1
fi
if [[ "$(cat "$work_dir/interpreter-eof.out")" != "EOF: " ]]; then
  echo "interpreter input EOF prompt mismatch" >&2
  exit 1
fi
grep -Fq 'EOFError: EOF when reading a line' "$work_dir/interpreter-eof.err"

if [[ -n "$bootstrap" ]]; then
  if "$bootstrap" --run "$repo_root/compiler/tests/input_eof.ty" </dev/null >"$work_dir/bootstrap-eof.out" 2>"$work_dir/bootstrap-eof.err"; then
    echo "bootstrap input accepted immediate EOF" >&2
    exit 1
  fi
  if [[ "$(cat "$work_dir/bootstrap-eof.out")" != "EOF: " ]]; then
    echo "bootstrap input EOF prompt mismatch" >&2
    exit 1
  fi
  grep -Fq 'EOFError: EOF when reading a line' "$work_dir/bootstrap-eof.err"
fi

for error_case in keyword arity; do
  source="$repo_root/compiler/tests/input_${error_case}_error.ty"
  if [[ "$error_case" == keyword ]]; then
    expected='TypeError: input() takes no keyword arguments'
  else
    expected='TypeError: input expected at most 1 argument, got 2'
  fi

  if python3 "$source" </dev/null >"$work_dir/python-${error_case}.out" 2>"$work_dir/python-${error_case}.err"; then
    echo "Python accepted invalid input ${error_case}" >&2
    exit 1
  fi
  grep -Fqx "$expected" <(tail -n 1 "$work_dir/python-${error_case}.err")

  if "$interpreter" "$source" </dev/null >"$work_dir/interpreter-${error_case}.out" 2>"$work_dir/interpreter-${error_case}.err"; then
    echo "interpreter accepted invalid input ${error_case}" >&2
    exit 1
  fi
  grep -Fq "$expected" "$work_dir/interpreter-${error_case}.err"

  if [[ -n "$bootstrap" ]]; then
    if "$bootstrap" --run "$source" </dev/null >"$work_dir/bootstrap-${error_case}.out" 2>"$work_dir/bootstrap-${error_case}.err"; then
      echo "bootstrap accepted invalid input ${error_case}" >&2
      exit 1
    fi
    grep -Fq "$expected" "$work_dir/bootstrap-${error_case}.err"
  fi

  if "$compiler" --build "$source" --out "$work_dir/input-invalid" >"$work_dir/compiler-${error_case}.out" 2>&1; then
    echo "compiler accepted invalid input ${error_case}" >&2
    exit 1
  fi
  if [[ "$error_case" == keyword ]]; then
    grep -Fq 'code=unsupported-keyword' "$work_dir/compiler-${error_case}.out"
  else
    grep -Fq 'code=primitive-arity' "$work_dir/compiler-${error_case}.out"
  fi
done

echo "input: Python, native compiler, interpreter, and available bootstrap outputs match"
echo "input: pipes, PTYs, CRLF, partial EOF, immediate EOF, keyword, and arity edges pass"
