#!/usr/bin/env bash
# Record one native Tier 1 compiler baseline using a small multi-module
# application.  This is deliberately a measurement tool, not a pass/fail
# performance gate: Phase 0 needs comparable cold/warm/no-op/edit numbers
# before later object-cache work can claim an improvement.
#
# Usage:
#   record_tier1_baseline.sh [compiler] [output-file]
#
# Run it on each target-native Tier 1 builder.  It accepts either the C
# bootstrap (which needs `--selfhost-cli <active-source>`) or a generated
# self-hosted compiler (which must receive the ordinary compiler CLI directly).
# In both cases the measured build follows the ordinary application path.
set -euo pipefail

repo_root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
compiler=${1:-"$repo_root/../tyrionc/build/tyrionc"}
report_path=${2:-"$repo_root/baseline-$(uname -s | tr '[:upper:]' '[:lower:]')-$(uname -m).txt"}
memory_mib=${TYRION_MEMORY_MIB:-8192}

if [[ ! -x "$compiler" ]]; then
    echo "baseline rejected code=missing-compiler;path=$compiler" >&2
    exit 2
fi

# `--selfhost-cli` is intentionally a C-bootstrap adapter, not part of the
# generated compiler's public CLI. Detect that adapter from its usage rather
# than making a generated successor pretend to be a bootstrap binary.
compiler_help=$("$compiler" --help 2>&1 || true)
compiler_command=("$compiler")
compiler_mode=ordinary-cli
if [[ "$compiler_help" == *"--selfhost-cli"* ]]; then
    compiler_command+=(--selfhost-cli "$repo_root/tyrionc.ty")
    compiler_mode=c-bootstrap-selfhost-cli
fi

host=$(uname -s)-$(uname -m)
case "$host" in
    Darwin-arm64) target=darwin-aarch64 ;;
    Linux-aarch64) target=linux-aarch64 ;;
    Linux-x86_64) target=linux-x86_64 ;;
    *)
        echo "baseline rejected code=unsupported-tier1-host;host=$host" >&2
        exit 2
        ;;
esac

work_dir=$(mktemp -d "${TMPDIR:-/tmp}/tyrion-tier1-baseline.XXXXXX")
trap 'rm -rf -- "$work_dir"' EXIT HUP INT TERM
project_dir="$work_dir/project"
cache_dir="$work_dir/cache"
mkdir -p "$project_dir" "$cache_dir"

# Each compile/run is bounded independently.  Linux uses a transient user
# service so compiler, assembler, linker, and their descendants share the
# same cgroup.  The fallback remains useful inside a Docker cgroup already
# capped by the caller; it carries the inherited virtual-memory limit too.
runner_kind='ulimit'
if [[ "$host" == Darwin-arm64 ]]; then
    if ! command -v taskpolicy >/dev/null 2>&1; then
        echo 'baseline rejected code=missing-taskpolicy' >&2
        exit 2
    fi
    runner_kind=taskpolicy
elif command -v systemd-run >/dev/null 2>&1 && systemctl --user show-environment >/dev/null 2>&1; then
    runner_kind=systemd
else
    ulimit -v $((memory_mib * 1024))
fi

last_elapsed_seconds=
last_peak_bytes=unavailable

run_capped() {
    local label=$1
    shift
    local time_file="$work_dir/$label.time"
    last_peak_bytes=unavailable

    if [[ "$runner_kind" == systemd ]]; then
        local unit="tyrion-baseline-${label}-$$-${RANDOM}"
        if ! /usr/bin/time -p systemd-run --user --wait --quiet --unit="$unit" \
            -p "MemoryMax=${memory_mib}M" -p 'MemorySwapMax=0' -p TasksMax=512 \
            "$@" 2>"$time_file"; then
            cat "$time_file" >&2
            return 1
        fi
        last_peak_bytes=$(systemctl --user show "$unit" --property=MemoryPeak --value --no-pager 2>/dev/null || true)
        [[ -n "$last_peak_bytes" && "$last_peak_bytes" != '[not set]' ]] || last_peak_bytes=unavailable
        systemctl --user reset-failed "$unit" >/dev/null 2>&1 || true
    elif [[ "$runner_kind" == taskpolicy ]]; then
        if ! /usr/bin/time -l taskpolicy -m "$memory_mib" "$@" 2>"$time_file"; then
            cat "$time_file" >&2
            return 1
        fi
        # BSD time reports the child maximum RSS in bytes.
        last_peak_bytes=$(awk '/maximum resident set size/ { print $1; found = 1 } END { if (!found) print "unavailable" }' "$time_file")
    else
        if ! /usr/bin/time -p "$@" 2>"$time_file"; then
            cat "$time_file" >&2
            return 1
        fi
    fi
    # GNU/POSIX `time -p` emits `real 0.12`; macOS `/usr/bin/time -l`
    # emits `0.12 real` on the same line as user/sys values. Accept both so
    # a Tier-1 Darwin baseline remains a useful performance record.
    last_elapsed_seconds=$(awk '
        $1 == "real" { value = $2 }
        $2 == "real" { value = $1 }
        END { if (value != "") print value }
    ' "$time_file" | tail -n 1)
    [[ -n "$last_elapsed_seconds" ]] || last_elapsed_seconds=unavailable
}

run_compiler() {
    local label=$1
    local output=$2
    run_capped "$label" "${compiler_command[@]}" \
        --build "$project_dir/entry.ty" --out "$output" --target "$target" \
        --ext-static=off --ext-dynamic=allowed --ext-dir "$repo_root/extensions" \
        --cache-dir "$cache_dir" --cache-stats
    printf '%s.compile_seconds=%s\n' "$label" "$last_elapsed_seconds" >>"$report_path"
    printf '%s.compile_peak_bytes=%s\n' "$label" "$last_peak_bytes" >>"$report_path"
}

assert_output() {
    local label=$1
    local binary=$2
    local expected=$3
    local actual="$work_dir/$label.output"
    run_capped "$label-run" "$binary" >"$actual"
    if [[ $(cat "$actual"; printf x) != "${expected}x" ]]; then
        echo "baseline rejected code=output-mismatch;stage=$label" >&2
        cat "$actual" >&2
        exit 1
    fi
}

file_bytes() {
    if [[ -f "$1" ]]; then
        wc -c <"$1" | tr -d '[:space:]'
    else
        printf '0'
    fi
}

object_bytes() {
    local output=$1
    {
        [[ -f "$output.o" ]] && wc -c <"$output.o"
        [[ -d "$output.shards" ]] && find "$output.shards" -type f -name '*.o' -exec wc -c {} +
    } | awk '{ total += $1 } END { print total + 0 }'
}

cache_event_count() {
    if [[ -f "$cache_dir/.generated-asm-cache-events" ]]; then
        wc -l <"$cache_dir/.generated-asm-cache-events" | tr -d '[:space:]'
    else
        printf '0'
    fi
}

measure_relink() {
    local output=$1
    local relinked="$output.relinked"
    if [[ ! -s "$output.link.rsp" ]]; then
        printf 'link_seconds=unavailable\nlink_peak_bytes=unavailable\n' >>"$report_path"
        return
    fi
    run_capped relink cc "@$output.link.rsp" -o "$relinked"
    printf 'link_seconds=%s\nlink_peak_bytes=%s\n' "$last_elapsed_seconds" "$last_peak_bytes" >>"$report_path"
    [[ -x "$relinked" ]]
    assert_output relink "$relinked" $'Hello Sansa\n'
}

cat >"$project_dir/helper.ty" <<'EOF'
def greet(name):
    return "Hello " + name
EOF
cat >"$project_dir/entry.ty" <<'EOF'
import helper

print(greet("Sansa"))
EOF

: >"$report_path"
printf 'contract=tyrion-tier1-baseline-v1\n' >>"$report_path"
printf 'host=%s\ntarget=%s\ncompiler=%s\ncompiler_mode=%s\nmemory_mib=%s\n' \
    "$host" "$target" "$compiler" "$compiler_mode" "$memory_mib" >>"$report_path"

cold="$work_dir/cold"
run_compiler cold "$cold"
assert_output cold "$cold" $'Hello Sansa\n'
printf 'assembly_bytes=%s\nobject_bytes=%s\n' \
    "$(find "$work_dir" -maxdepth 2 -type f -name '*.s' -exec wc -c {} + | awk '{ total += $1 } END { print total + 0 }')" \
    "$(object_bytes "$cold")" >>"$report_path"
measure_relink "$cold"
printf 'cold.cache_events=%s\n' "$(cache_event_count)" >>"$report_path"

warm="$work_dir/warm"
run_compiler warm "$warm"
assert_output warm "$warm" $'Hello Sansa\n'
printf 'warm.cache_events=%s\n' "$(cache_event_count)" >>"$report_path"

noop="$work_dir/noop"
run_compiler noop "$noop"
assert_output noop "$noop" $'Hello Sansa\n'
printf 'noop.cache_events=%s\n' "$(cache_event_count)" >>"$report_path"

cat >"$project_dir/helper.ty" <<'EOF'
def greet(name):
    return "Greetings " + name
EOF
private_edit="$work_dir/private-edit"
run_compiler private_edit "$private_edit"
assert_output private_edit "$private_edit" $'Greetings Sansa\n'
printf 'private_edit.cache_events=%s\n' "$(cache_event_count)" >>"$report_path"

cat >"$project_dir/helper.ty" <<'EOF'
def greet(name, prefix="Greetings"):
    return prefix + " " + name
EOF
interface_edit="$work_dir/interface-edit"
run_compiler interface_edit "$interface_edit"
assert_output interface_edit "$interface_edit" $'Greetings Sansa\n'
printf 'interface_edit.cache_events=%s\n' "$(cache_event_count)" >>"$report_path"

printf 'baseline report: %s\n' "$report_path"
