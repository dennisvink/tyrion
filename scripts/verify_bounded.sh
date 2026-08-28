#!/usr/bin/env bash
# Run the C bootstrap's current runtime, evaluator, cache, and target gates
# under one explicit 8 GiB process-tree limit.  This is intentionally a
# focused recovery/iteration gate, not the multi-hour successor checkpoint.
set -euo pipefail

repo_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
bootstrap_dir=${TYRIONC_DIR:-"$repo_dir/../tyrionc"}
bootstrap_bin=${TYRION_BOOTSTRAP:-"$bootstrap_dir/build/tyrionc"}
memory_mib=${TYRION_MEMORY_MIB:-8192}

if [[ ! -x "$bootstrap_bin" ]]; then
    echo "bounded verification rejected code=missing-bootstrap;path=$bootstrap_bin" >&2
    exit 2
fi

runner=()
case "$(uname -s)" in
    Darwin)
        if ! command -v taskpolicy >/dev/null 2>&1; then
            echo 'bounded verification rejected code=missing-taskpolicy' >&2
            exit 2
        fi
        runner=(taskpolicy -m "$memory_mib")
        ;;
    Linux)
        # A user systemd scope contains compiler children and linker/tool
        # processes; a shell ulimit would not provide that process-tree bound.
        if ! command -v systemd-run >/dev/null 2>&1 || ! systemctl --user show-environment >/dev/null 2>&1; then
            echo 'bounded verification rejected code=missing-user-systemd' >&2
            exit 2
        fi
        runner=(systemd-run --user --scope --quiet --wait
            -p "MemoryMax=${memory_mib}M"
            -p 'MemorySwapMax=0')
        ;;
    *)
        echo "bounded verification rejected code=unsupported-host;host=$(uname -s)" >&2
        exit 2
        ;;
esac

run_bounded() {
    "${runner[@]}" "$@"
}

run_bounded make -C "$bootstrap_dir" test
run_bounded make -C "$bootstrap_dir" test-cache
run_bounded bash "$repo_dir/compiler/tests/test_c_bootstrap_ordinary_cli.sh" "$bootstrap_bin"
run_bounded bash "$repo_dir/compiler/tests/test_target_selection.sh" "$bootstrap_bin"
run_bounded bash "$repo_dir/compiler/tests/test_toolchain_static_extension.sh" "$bootstrap_bin"

echo "bounded verification: runtime, evaluator, ordinary-cli, cache, target, and static-extension gates passed;limit=${memory_mib}MiB"
