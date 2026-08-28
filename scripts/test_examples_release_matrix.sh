#!/usr/bin/env bash
# Build every supported example with a generated Tyrion compiler, then run the
# existing Tyrion example oracle through the C bootstrap.  The build directory
# is isolated so this is safe for local, container, and remote successor gates.
set -euo pipefail

if [[ $# -lt 2 || $# -gt 3 ]]; then
    echo "usage: $0 <compiler> <c-bootstrap> [darwin|linux]" >&2
    exit 2
fi

compiler=$(CDPATH= cd -- "$(dirname -- "$1")" && pwd)/$(basename -- "$1")
bootstrap=$(CDPATH= cd -- "$(dirname -- "$2")" && pwd)/$(basename -- "$2")
platform=${3:-"$(uname -s | tr '[:upper:]' '[:lower:]')"}
repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/tyrion-example-corpus.XXXXXX")
trap 'rm -rf -- "$work_dir"' EXIT HUP INT TERM
output_dir="$work_dir/build/examples"
mkdir -p "$output_dir"

test -x "$compiler"
test -x "$bootstrap"

# Keep the source-to-oracle inventory check in lock-step with the release
# workflow.  A new example must be deliberately covered before this gate will
# build it.
while IFS= read -r source; do
    if ! grep -Fq "\"$source\"" "$repo_root/compiler/tests/examples.ty"; then
        echo "Example is missing from compiler/tests/examples.ty: $source" >&2
        exit 1
    fi
done < <(cd "$repo_root" && find examples -type f -name '*.ty' | sort)

while IFS= read -r source; do
    if [[ "$source" == "examples/terminal_chess.ty" && "$platform" != "darwin" ]]; then
        echo "Skipping unsupported Linux terminal example: $source"
        continue
    fi

    name=$(basename "$source" .ty)
    output="$output_dir/$name"
    args=(--build "$repo_root/$source" --out "$output")

    if [[ "$source" == examples/extensions/* ]]; then
        args+=(--ext-static=required --ext-dir "$repo_root/examples/extensions")
    fi

    echo "Building $source -> $output"
    "$compiler" "${args[@]}"
    test -x "$output"
done < <(cd "$repo_root" && find examples -type f -name '*.ty' | sort)

# The oracle deliberately runs from the isolated work root because it expects
# `build/examples/*` relative to its process CWD.  Sources remain in the
# repository; generated binaries and transient example data are cleaned up.
(
    cd "$work_dir"
    "$bootstrap" --exec-call "$repo_root/compiler/tests/examples.ty" main "$platform"
)

echo "example release matrix: compiler-built corpus and runtime oracle passed;platform=$platform"
