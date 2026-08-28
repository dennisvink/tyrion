#!/usr/bin/env bash
# Verify that two ordinary object-path compiler successors converge on the
# same observable compiler behavior and normalized object/link artifacts.
#
# Usage:
#   test_successor_convergence.sh <c3-compiler> <c4-compiler> [scan-source]
#
# The caller owns the expensive c1 -> c2 -> c3 -> c4 generation. This fixture
# is intentionally narrow: it verifies the stable successor contract without
# requiring another compiler rebuild. It is expected to run on the target
# native host used for the successors.
set -euo pipefail

if [ "$#" -lt 2 ] || [ "$#" -gt 3 ]; then
    echo "usage: $0 <c3-compiler> <c4-compiler> [scan-source]" >&2
    exit 2
fi

c3=$1
c4=$2
repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
scan_source=${3:-"$repo_root/examples/hello.ty"}

if [ ! -x "$c3" ] || [ ! -x "$c4" ]; then
    echo 'successor convergence rejected code=missing-compiler' >&2
    exit 2
fi
if [ ! -f "$scan_source" ]; then
    echo "successor convergence rejected code=missing-scan-source;path=$scan_source" >&2
    exit 2
fi

c3_dir=$(CDPATH= cd -- "$(dirname -- "$c3")" && pwd)
c4_dir=$(CDPATH= cd -- "$(dirname -- "$c4")" && pwd)
c3_name=$(basename -- "$c3")
c4_name=$(basename -- "$c4")

for stage in "$c3" "$c4"; do
    test -s "$stage.tyn"
    test -s "$stage.link-manifest"
    test -s "$stage.shards/objects.list"
done

work_dir=$(mktemp -d "${TMPDIR:-/tmp}/tyrion-successor-convergence.XXXXXX")
trap 'rm -rf "$work_dir"' EXIT HUP INT TERM

"$c3" --version >"$work_dir/c3.version"
"$c4" --version >"$work_dir/c4.version"
cmp "$work_dir/c3.version" "$work_dir/c4.version"

"$c3" --scan "$scan_source" >"$work_dir/c3.scan"
"$c4" --scan "$scan_source" >"$work_dir/c4.scan"
cmp "$work_dir/c3.scan" "$work_dir/c4.scan"

normalize_successor_path() {
    input=$1
    stage_dir=$2
    output=$3
    sed "s#${stage_dir}#<successor>#g" "$input" >"$output"
}

normalize_successor_path "$c3.tyn" "$c3_dir" "$work_dir/c3.tyn"
normalize_successor_path "$c4.tyn" "$c4_dir" "$work_dir/c4.tyn"
cmp "$work_dir/c3.tyn" "$work_dir/c4.tyn"

normalize_successor_path "$c3.link-manifest" "$c3_dir" "$work_dir/c3.link-manifest"
normalize_successor_path "$c4.link-manifest" "$c4_dir" "$work_dir/c4.link-manifest"
cmp "$work_dir/c3.link-manifest" "$work_dir/c4.link-manifest"

normalize_successor_path "$c3.shards/objects.list" "$c3_dir" "$work_dir/c3.objects"
normalize_successor_path "$c4.shards/objects.list" "$c4_dir" "$work_dir/c4.objects"
cmp "$work_dir/c3.objects" "$work_dir/c4.objects"

# Compile the formerly problematic aggregate-metadata shape with both native
# successors. This keeps the C2 -> C3 definite-assignment boundary covered
# independently of the much larger compiler rebuild that first exposed it.
bash "$repo_root/compiler/tests/test_native_definite_assignment_rebind.sh" "$c3"
bash "$repo_root/compiler/tests/test_native_definite_assignment_rebind.sh" "$c4"

# The x86 streamed verifier deliberately exercises a native executable link,
# not only assembly inspection.  Run it through both successors on its native
# host so a copied nested module record cannot silently discard the root
# runtime-operation inventory between compiler generations.
if [[ "$(uname -s)-$(uname -m)" == "Linux-x86_64" ]]; then
    bash "$repo_root/compiler/tests/test_x86_streamed_module_verifier.sh" "$c3"
    bash "$repo_root/compiler/tests/test_x86_streamed_module_verifier.sh" "$c4"
fi

# The normalized structural records are the deterministic requirement. Record
# raw executable hashes as evidence without assuming every system linker emits
# byte-identical executable headers.
if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$c3" "$c4"
elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$c3" "$c4"
else
    echo 'successor convergence rejected code=missing-sha256-tool' >&2
    exit 2
fi

printf 'successor convergence: behavior and normalized object manifests passed (%s, %s)\n' \
    "$c3_name" "$c4_name"
