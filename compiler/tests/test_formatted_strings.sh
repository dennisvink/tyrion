#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 || $# -gt 3 ]]; then
    echo "usage: $0 <tyrion-compiler> <tyrion-interpreter> [bootstrap-compiler]" >&2
    exit 2
fi

compiler=$1
interpreter=$2
bootstrap=${3:-}
script_dir=$(cd -- "$(dirname -- "$0")" && pwd)
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/tyrion-formatted-strings.XXXXXX")
trap 'rm -rf "$work_dir"' EXIT

python3 "$script_dir/gap_formatted_strings_oracle.py" >"$work_dir/python.out"
"$compiler" --build "$script_dir/gap_formatted_strings.ty" --out "$work_dir/formatted-strings"
"$work_dir/formatted-strings" >"$work_dir/native.out"
"$interpreter" "$script_dir/gap_formatted_strings.ty" >"$work_dir/interpreter.out"
cmp "$work_dir/python.out" "$work_dir/native.out"
cmp "$work_dir/python.out" "$work_dir/interpreter.out"

if [[ -n "$bootstrap" ]]; then
    "$bootstrap" --run "$script_dir/gap_formatted_strings.ty" >"$work_dir/bootstrap.out"
    cmp "$work_dir/python.out" "$work_dir/bootstrap.out"
fi

for specification in \
    'unclosed:unclosed-expression' \
    'empty:empty-expression' \
    'conversion:conversion-not-supported' \
    'format_spec:format-spec-not-supported' \
    'closing:single-closing-brace' \
    'unterminated:unterminated-string'; do
    fixture=${specification%%:*}
    detail=${specification#*:}
    source="$script_dir/gap_formatted_string_invalid_${fixture}.ty"
    expected="lexer rejected code=invalid-formatted-string;detail=$detail"

    if "$compiler" --build "$source" --out "$work_dir/invalid" >"$work_dir/compiler-invalid.out" 2>&1; then
        echo "compiler accepted unsupported formatted string: $source" >&2
        exit 1
    fi
    grep -Fq "$expected" "$work_dir/compiler-invalid.out"

    if "$interpreter" "$source" >"$work_dir/interpreter-invalid.out" 2>&1; then
        echo "interpreter accepted unsupported formatted string: $source" >&2
        exit 1
    fi
    grep -Fq "$expected" "$work_dir/interpreter-invalid.out"

    if [[ -n "$bootstrap" ]]; then
        if "$bootstrap" --run "$source" >"$work_dir/bootstrap-invalid.out" 2>&1; then
            echo "bootstrap accepted unsupported formatted string: $source" >&2
            exit 1
        fi
        grep -Fq "$expected" "$work_dir/bootstrap-invalid.out"
    fi
done

echo "formatted strings: Python, native compiler, interpreter, and available bootstrap outputs match"
echo "formatted strings: malformed and deferred forms fail consistently"
