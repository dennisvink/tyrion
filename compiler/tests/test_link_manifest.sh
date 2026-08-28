#!/usr/bin/env bash
set -euo pipefail

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
bootstrap=${1:-"$repo_root/../tyrionc/build/tyrionc"}
source_file="$repo_root/examples/hello.ty"
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/tyrion-link-manifest.XXXXXX")
trap 'rm -rf "$work_dir"' EXIT HUP INT TERM

output="$work_dir/hello"
"$bootstrap" --selfhost-cli "$repo_root/tyrionc.ty" \
    --build "$source_file" --out "$output" --ext-static=off

test -x "$output"
test "$("$output")" = $'hello world\nmath 5 12 -3'
manifest="$output.link-manifest"
response="$output.link.rsp"
test -s "$manifest"
test -s "$response"

grep -F 'tyrion-link-manifest-v1' "$manifest" >/dev/null
grep -F 'target-context=tyrion-target-v2:' "$manifest" >/dev/null
grep -F 'entry-symbol=_main' "$manifest" >/dev/null
grep -F 'program-object.count=1' "$manifest" >/dev/null
grep -F 'runtime-input.count=0' "$manifest" >/dev/null
grep -F 'extension-input.count=0' "$manifest" >/dev/null
grep -F 'library.0=-lm' "$manifest" >/dev/null
grep -F 'library.count=1' "$manifest" >/dev/null
grep -F 'linker-flag.count=0' "$manifest" >/dev/null
grep -F "\"$output.o\"" "$response" >/dev/null
grep -Fx -- '-lm' "$response" >/dev/null

echo 'link manifest: canonical whole-program object, response file, and executable passed'
