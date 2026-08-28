#!/usr/bin/env bash
# The C bootstrap must enter the current compiler through the same CLI route
# used by a normal Tyrion application.  This intentionally avoids the legacy
# direct `--exec-call ... build_native` convenience route.
set -euo pipefail

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
bootstrap=${1:-"$repo_root/../tyrionc/build/tyrionc"}
source_file="$repo_root/examples/hello.ty"
class_source="$repo_root/examples/class.ty"
lowering_source="$repo_root/compiler/lowering.ty"

if [ ! -x "$bootstrap" ]; then
    echo "ordinary C-bootstrap CLI rejected code=missing-bootstrap;path=$bootstrap" >&2
    exit 2
fi

# `build_native` is the one common native entry point.  The compiler must
# reach the same application lowering orchestration as an ordinary program;
# the legacy compiler-CLI route analyser/emitter may remain temporarily as
# unreachable compatibility code, but must never re-enter this graph.
build_native_calls=$(awk '
    /^def lowered_function_known_direct_calls\(name\):$/ { in_direct_calls = 1 }
    in_direct_calls && /^    if name == "build_native":$/ { in_build_native = 1 }
    in_build_native { print }
    in_build_native && /^    if name == "lex":$/ { exit }
' "$lowering_source")
printf '%s\n' "$build_native_calls" | grep -F \
    'return ["build_runtime_native_program_if_needed"' >/dev/null
if printf '%s\n' "$build_native_calls" | grep -Eq \
    'build_compiler_cli_native|collect_cli_spec|emit_compiler_cli_asm_into|render_compiler_cli_artifact'; then
    echo 'ordinary C-bootstrap CLI rejected code=compiler-special-reachability' >&2
    exit 1
fi

# The table-plan seed is the other historical injection point.  Keeping this
# check separate from the direct-call list makes an accidental explicit append
# fail even if the common build entry remains clean.
ordinary_plan_seed=$(awk '
    /^def collect_lowered_function_table_plan\(program, build_plan\):$/ { in_plan = 1 }
    in_plan { print }
    in_plan && /^def / && $0 !~ /^def collect_lowered_function_table_plan\(program, build_plan\):$/ { exit }
' "$lowering_source")
if printf '%s\n' "$ordinary_plan_seed" | grep -Eq \
    'build_compiler_cli_native|lowered_function_append_target_sources'; then
    echo 'ordinary C-bootstrap CLI rejected code=compiler-special-plan-injection' >&2
    exit 1
fi

work_dir=$(mktemp -d "${TMPDIR:-/tmp}/tyrion-c-bootstrap-cli.XXXXXX")
trap 'rm -rf "$work_dir"' EXIT HUP INT TERM

"$bootstrap" --selfhost-cli "$repo_root/tyrionc.ty" \
    --build "$source_file" --out "$work_dir/hello" --ext-static=off

test -x "$work_dir/hello"
test "$("$work_dir/hello")" = $'hello world\nmath 5 12 -3'

# Class descriptors retain native method entry points.  This exercises the
# root-manifest address-taken path through the ordinary application route,
# rather than merely checking an assembly-only target selection path.
"$bootstrap" --selfhost-cli "$repo_root/tyrionc.ty" \
    --build "$class_source" --out "$work_dir/class" --ext-static=off

test -x "$work_dir/class"
test "$("$work_dir/class")" = "Hello Sansa I'm Arya"

echo 'ordinary C-bootstrap CLI: current compiler source used the ordinary application path and ran hello and class examples'
