#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "usage: $0 <tyrionic> [tyrion]" >&2
  exit 2
fi

compiler=$1
interpreter=${2:-}
script_dir=$(cd -- "$(dirname -- "$0")" && pwd)
repo_dir=$(cd -- "$script_dir/../.." && pwd)
fixtures="$script_dir/fixtures"
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/tyrion-http-module.XXXXXX")
server_pid=""

cleanup() {
  if [[ -n "$server_pid" ]]; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
  rm -rf "$work_dir"
}
trap cleanup EXIT

"$repo_dir/compiler/update_builtin_modules.sh" --check

"$compiler" --build "$fixtures/http_module_unit.ty" --out "$work_dir/unit"
unit_output=$("$work_dir/unit")
if [[ "$unit_output" != "HTTP module unit: ok" ]]; then
  echo "unexpected HTTP unit output: $unit_output" >&2
  exit 1
fi

"$compiler" --build "$fixtures/http_module_live.ty" --out "$work_dir/live"
request_count=1
if [[ -n "$interpreter" ]]; then
  request_count=2
fi
python3 "$script_dir/http_test_server.py" --port-file "$work_dir/port" --requests "$request_count" &
server_pid=$!

for _ in {1..100}; do
  if [[ -s "$work_dir/port" ]]; then
    break
  fi
  if ! kill -0 "$server_pid" 2>/dev/null; then
    wait "$server_pid"
  fi
  sleep 0.05
done
if [[ ! -s "$work_dir/port" ]]; then
  echo "HTTP test server did not publish its port" >&2
  exit 1
fi

port=$(<"$work_dir/port")
live_output=$("$work_dir/live" "http://127.0.0.1:$port/winter")
if [[ "$live_output" != "200 capable The North remembers" ]]; then
  echo "unexpected HTTP live output: $live_output" >&2
  exit 1
fi

if [[ -n "$interpreter" ]]; then
  interpreter_unit_output=$("$interpreter" "$fixtures/http_module_unit.ty")
  if [[ "$interpreter_unit_output" != "HTTP module unit: ok" ]]; then
    echo "unexpected interpreter HTTP unit output: $interpreter_unit_output" >&2
    exit 1
  fi

  interpreter_live_output=$("$interpreter" "$fixtures/http_module_live.ty" "http://127.0.0.1:$port/winter")
  if [[ "$interpreter_live_output" != "200 capable The North remembers" ]]; then
    echo "unexpected interpreter HTTP live output: $interpreter_live_output" >&2
    exit 1
  fi
fi

wait "$server_pid"
server_pid=""

echo "HTTP module: ok"
