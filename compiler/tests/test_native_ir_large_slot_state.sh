#!/usr/bin/env bash
set -euo pipefail

compiler="${1:-build/tyrionic}"
source_path="compiler/tests/_probe_slot_state_rows.ty"
output_path="build/native-ir-large-slot-state"

"$compiler" --build "$source_path" --out "$output_path"
actual="$($output_path)"
test "$actual" = "slot-state-rows 1051137"

echo native-ir-large-slot-state=ok
