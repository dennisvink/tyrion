#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 0 ]]; then
  echo "usage: $0" >&2
  exit 2
fi

repo_root=$(cd -- "$(dirname -- "$0")/../.." && pwd)
python3 - "$repo_root/compiler/targets/linux_x86_64_native.ty" <<'PY'
from pathlib import Path
import sys


text = Path(sys.argv[1]).read_text()
function_start = text.index("def x86_native_emit_v2_module_graph_runtime(out):")
function_end = text.index("\ndef ", function_start + 1)
function = text[function_start:function_end]
visit = function.index('out = asm_writer_append(out, ".L_tyrion_direct_mg_visit:\\n')
recursive_call = function.index("\\tcall .L_tyrion_direct_mg_visit\\n", visit)
emit = function.index(".L_tyrion_direct_mg_visit_emit:\\n", recursive_call)
source_boundary = function.index(
    "\\tleaq .L_tyrion_direct_mg_source_prefix(%rip), %rsi\\n", visit
)
if source_boundary < emit:
    raise SystemExit(
        "x86 module graph emits a source boundary before visiting dependencies"
    )
PY

echo "x86 module graph emitter: dependency-first source boundaries are canonical"
