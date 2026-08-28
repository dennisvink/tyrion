#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo "usage: $0 <native-tyrionc>" >&2
    exit 2
fi

compiler=$1
repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
source_file="$repo_root/compiler/tests/native_definite_assignment_rebind.ty"
direct_probe="$repo_root/compiler/tests/native_definite_assignment_direct_rebind_probe.ty"
streamed_probe="$repo_root/compiler/tests/native_definite_assignment_streamed_module_probe.ty"
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/tyrion-native-definite-assignment.XXXXXX")
trap 'rm -rf -- "$work_dir"' EXIT HUP INT TERM

case "$(uname -s)-$(uname -m)" in
    Darwin-arm64) target=darwin-aarch64 ;;
    Linux-aarch64) target=linux-aarch64 ;;
    Linux-x86_64) target=linux-x86_64 ;;
    *)
        echo "native definite-assignment rejected code=unsupported-host" >&2
        exit 2
        ;;
esac

"$compiler" --build "$source_file" --out "$work_dir/fixture" \
    --target "$target" --ext-static=off --cache-dir "$work_dir/cache"

test "$("$work_dir/fixture")" = $'value\nlinux-x86_64\nsummary:functions=1:blocks=0:constants=0'

"$compiler" --build "$direct_probe" --out "$work_dir/direct-probe" \
    --target "$target" --ext-static=off --cache-dir "$work_dir/direct-cache"

test "$("$work_dir/direct-probe")" = 'ok'

"$compiler" --build "$streamed_probe" --out "$work_dir/streamed-probe" \
    --target "$target" --ext-static=off --cache-dir "$work_dir/streamed-cache"

test "$("$work_dir/streamed-probe")" = '0'
echo 'native definite-assignment: direct call-result locals, aggregate metadata, summary records, and streamed-module loop-safe plan boundaries pass'
