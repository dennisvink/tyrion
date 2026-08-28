#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <c-bootstrap>" >&2
  exit 2
fi

if [[ "$(uname -s)-$(uname -m)" != "Linux-x86_64" ]]; then
  echo 'x86 lowering-unit cache: skipped (requires native Linux x86-64)'
  exit 0
fi

memory_mib=${TYRION_MEMORY_MIB:-8192}
if [[ ${TYRION_BOUNDED_RUN:-} != 1 ]]; then
  case "$(uname -s)" in
    Linux)
      # A user systemd scope contains the bootstrap, linker, and all native
      # children.  Minimal CI containers may not provide one; those retain
      # the inherited virtual-memory cap below.
      if command -v systemd-run >/dev/null 2>&1 && systemctl --user show-environment >/dev/null 2>&1; then
        TYRION_BOUNDED_RUN=1 exec systemd-run --user --scope --quiet --wait \
          -p "MemoryMax=${memory_mib}M" -p 'MemorySwapMax=0' "$0" "$@"
      fi
      ;;
  esac
fi

repo_root=$(cd -- "$(dirname -- "$0")/../.." && pwd)
bootstrap=$1
work_dir=$(mktemp -d)
trap 'rm -rf -- "$work_dir"' EXIT

# The Linux fallback still bounds the bootstrap and its inherited native
# children when a user systemd cgroup is unavailable.
ulimit -v $((memory_mib * 1024))

cache_dir="$work_dir/cache"
mkdir -p "$cache_dir"
TIMEFORMAT='%R'
options=(
  --cache-dir "$cache_dir"
  --cache-stats
  --ext-static=off
  --ext-dynamic=allowed
  --ext-dir "$repo_root/extensions"
)

{ time "$bootstrap" --selfhost-cli "$repo_root/tyrionc.ty" \
  --build "$repo_root/compiler/tests/_probe_x86_lowering_cache.ty" \
  --out "$work_dir/cold" "${options[@]}"; } 2>"$work_dir/cold.time"
"$work_dir/cold" >"$work_dir/cold.out"
cp "$cache_dir/.generated-asm-cache-events" "$work_dir/cold.events"

# Force whole-program generation while retaining parsed, lowered, and emitted
# units. This is the staged path rather than the outer whole-assembly hit.
find "$cache_dir" -maxdepth 1 -name '*.asmcache' -delete
cold_event_count=$(wc -l <"$work_dir/cold.events")
{ time "$bootstrap" --selfhost-cli "$repo_root/tyrionc.ty" \
  --build "$repo_root/compiler/tests/_probe_x86_lowering_cache.ty" \
  --out "$work_dir/warm" "${options[@]}"; } 2>"$work_dir/warm.time"
"$work_dir/warm" >"$work_dir/warm.out"
tail -n +$((cold_event_count + 1)) \
  "$cache_dir/.generated-asm-cache-events" >"$work_dir/warm.events"

expected=$'hello Arya\nsum 5\n'
if [[ $(cat "$work_dir/cold.out"; printf x) != "${expected}x" ]]; then
  echo "cold executable output mismatch" >&2
  exit 1
fi
cmp -s "$work_dir/cold.out" "$work_dir/warm.out"
# Object mode deliberately no longer emits the legacy aggregate "$out.s".
# Verify the deterministic root runtime assembly and the normalized
# target-object inventories/manifests instead. Object cache hits are allowed
# to restore validated objects without recreating per-function assembly files.
cmp -s "$work_dir/cold.runtime.s" "$work_dir/warm.runtime.s"
normalize_output_paths() {
  local input=$1
  local output=$2
  sed -e "s#${work_dir}/cold#<output>#g" \
      -e "s#${work_dir}/warm#<output>#g" "$input" >"$output"
}
for artifact in tyn link-manifest; do
  normalize_output_paths "$work_dir/cold.$artifact" "$work_dir/cold.$artifact.normalized"
  normalize_output_paths "$work_dir/warm.$artifact" "$work_dir/warm.$artifact.normalized"
  cmp -s "$work_dir/cold.$artifact.normalized" "$work_dir/warm.$artifact.normalized"
done
for inventory in function-objects.list objects.list; do
  normalize_output_paths "$work_dir/cold.shards/$inventory" "$work_dir/cold.$inventory.normalized"
  normalize_output_paths "$work_dir/warm.shards/$inventory" "$work_dir/warm.$inventory.normalized"
  cmp -s "$work_dir/cold.$inventory.normalized" "$work_dir/warm.$inventory.normalized"
done
grep -qx 'lower-miss' "$work_dir/cold.events"
grep -qx 'tail-miss' "$work_dir/cold.events"
grep -qx 'lower-hit' "$work_dir/warm.events"
grep -qx 'unit-hit' "$work_dir/warm.events"
grep -qx 'tail-hit' "$work_dir/warm.events"

cold_real=$(tail -n 1 "$work_dir/cold.time")
warm_real=$(tail -n 1 "$work_dir/warm.time")
test -n "$cold_real"
test -n "$warm_real"
echo "x86 lowering-unit cache: cold miss, warm staged reuse, and deterministic assembly passed (cold=${cold_real}s warm=${warm_real}s)"
