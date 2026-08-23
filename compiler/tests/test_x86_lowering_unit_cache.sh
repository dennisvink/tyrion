#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <c-bootstrap>" >&2
  exit 2
fi

repo_root=$(cd -- "$(dirname -- "$0")/../.." && pwd)
bootstrap=$1
work_dir=$(mktemp -d)
trap 'rm -rf -- "$work_dir"' EXIT

# Bound the bootstrap and every native child it launches to 8 GiB.
ulimit -v 8388608

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
cmp -s "$work_dir/cold.s" "$work_dir/warm.s"
grep -qx 'lower-miss' "$work_dir/cold.events"
grep -qx 'lower-hit' "$work_dir/warm.events"
grep -qx 'unit-hit' "$work_dir/warm.events"

cold_real=$(tail -n 1 "$work_dir/cold.time")
warm_real=$(tail -n 1 "$work_dir/warm.time")
test -n "$cold_real"
test -n "$warm_real"
echo "x86 lowering-unit cache: cold miss, warm staged reuse, and deterministic assembly passed (cold=${cold_real}s warm=${warm_real}s)"
