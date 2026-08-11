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
tls_fixtures="$fixtures/tls"
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/tyrion-https-module.XXXXXX")
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

"$compiler" --build "$fixtures/https_module_live.ty" --out "$work_dir/live"
connections=3
if [[ -n "$interpreter" ]]; then
  connections=4
fi
python3 "$script_dir/tls_test_server.py" \
  --cert "$tls_fixtures/server-cert.pem" \
  --key "$tls_fixtures/server-key.pem" \
  --port-file "$work_dir/port" \
  --connections "$connections" &
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
  echo "TLS test server did not publish its port" >&2
  exit 1
fi

port=$(<"$work_dir/port")
trusted_url="https://localhost:$port/winter"
mismatch_url="https://127.0.0.1:$port/winter"
expected="200 encrypted The encrypted North knows"

if ! trusted_output=$(SSL_CERT_FILE="$tls_fixtures/ca-cert.pem" "$work_dir/live" "$trusted_url"); then
  echo "compiled HTTPS request failed" >&2
  exit 1
fi
if [[ "$trusted_output" != "$expected" ]]; then
  echo "unexpected compiled HTTPS output: $trusted_output" >&2
  exit 1
fi

if "$work_dir/live" "$trusted_url" >"$work_dir/untrusted.out" 2>"$work_dir/untrusted.err"; then
  echo "HTTPS unexpectedly accepted an untrusted certificate" >&2
  exit 1
fi
if ! grep -Fq "TLS connect failed" "$work_dir/untrusted.err"; then
  echo "missing untrusted-certificate diagnostic" >&2
  cat "$work_dir/untrusted.err" >&2
  exit 1
fi

if SSL_CERT_FILE="$tls_fixtures/ca-cert.pem" "$work_dir/live" "$mismatch_url" >"$work_dir/mismatch.out" 2>"$work_dir/mismatch.err"; then
  echo "HTTPS unexpectedly accepted a hostname mismatch" >&2
  exit 1
fi
if ! grep -Fq "TLS connect failed" "$work_dir/mismatch.err"; then
  echo "missing hostname-mismatch diagnostic" >&2
  cat "$work_dir/mismatch.err" >&2
  exit 1
fi

if [[ -n "$interpreter" ]]; then
  if ! interpreter_output=$(SSL_CERT_FILE="$tls_fixtures/ca-cert.pem" "$interpreter" "$fixtures/https_module_live.ty" "$trusted_url"); then
    echo "interpreter HTTPS request failed" >&2
    exit 1
  fi
  if [[ "$interpreter_output" != "$expected" ]]; then
    echo "unexpected interpreter HTTPS output: $interpreter_output" >&2
    exit 1
  fi
fi

wait "$server_pid"
server_pid=""

large_connections=1
if [[ -n "$interpreter" ]]; then
  large_connections=2
fi
python3 "$script_dir/tls_test_server.py" \
  --cert "$tls_fixtures/server-cert.pem" \
  --key "$tls_fixtures/server-key.pem" \
  --port-file "$work_dir/large-port" \
  --connections "$large_connections" \
  --body-bytes 65536 &
server_pid=$!

for _ in {1..100}; do
  if [[ -s "$work_dir/large-port" ]]; then
    break
  fi
  if ! kill -0 "$server_pid" 2>/dev/null; then
    wait "$server_pid"
  fi
  sleep 0.05
done
if [[ ! -s "$work_dir/large-port" ]]; then
  echo "large TLS test server did not publish its port" >&2
  exit 1
fi

large_url="https://localhost:$(<"$work_dir/large-port")/large"
large_expected="200 65536 65 65 65536"
"$compiler" --build "$fixtures/https_module_large.ty" --out "$work_dir/large"
if ! large_output=$(SSL_CERT_FILE="$tls_fixtures/ca-cert.pem" "$work_dir/large" "$large_url"); then
  echo "compiled large HTTPS request failed" >&2
  exit 1
fi
if [[ "$large_output" != "$large_expected" ]]; then
  echo "unexpected compiled large HTTPS output: $large_output" >&2
  exit 1
fi
if [[ -n "$interpreter" ]]; then
  if ! large_interpreter_output=$(SSL_CERT_FILE="$tls_fixtures/ca-cert.pem" "$interpreter" "$fixtures/https_module_large.ty" "$large_url"); then
    echo "interpreter large HTTPS request failed" >&2
    exit 1
  fi
  if [[ "$large_interpreter_output" != "$large_expected" ]]; then
    echo "unexpected interpreter large HTTPS output: $large_interpreter_output" >&2
    exit 1
  fi
fi

wait "$server_pid"
server_pid=""

chunked_connections=1
if [[ -n "$interpreter" ]]; then
  chunked_connections=2
fi
python3 "$script_dir/tls_test_server.py" \
  --cert "$tls_fixtures/server-cert.pem" \
  --key "$tls_fixtures/server-key.pem" \
  --port-file "$work_dir/chunked-port" \
  --connections "$chunked_connections" \
  --body-bytes 233490 \
  --chunked &
server_pid=$!

for _ in {1..100}; do
  if [[ -s "$work_dir/chunked-port" ]]; then
    break
  fi
  if ! kill -0 "$server_pid" 2>/dev/null; then
    wait "$server_pid"
  fi
  sleep 0.05
done
if [[ ! -s "$work_dir/chunked-port" ]]; then
  echo "chunked TLS test server did not publish its port" >&2
  exit 1
fi

chunked_url="https://localhost:$(<"$work_dir/chunked-port")/chunked"
chunked_expected="200 233490 65 65 233490"
if ! chunked_output=$(SSL_CERT_FILE="$tls_fixtures/ca-cert.pem" "$work_dir/large" "$chunked_url"); then
  echo "compiled chunked HTTPS request failed" >&2
  exit 1
fi
if [[ "$chunked_output" != "$chunked_expected" ]]; then
  echo "unexpected compiled chunked HTTPS output: $chunked_output" >&2
  exit 1
fi
if [[ -n "$interpreter" ]]; then
  if ! chunked_interpreter_output=$(SSL_CERT_FILE="$tls_fixtures/ca-cert.pem" "$interpreter" "$fixtures/https_module_large.ty" "$chunked_url"); then
    echo "interpreter chunked HTTPS request failed" >&2
    exit 1
  fi
  if [[ "$chunked_interpreter_output" != "$chunked_expected" ]]; then
    echo "unexpected interpreter chunked HTTPS output: $chunked_interpreter_output" >&2
    exit 1
  fi
fi

wait "$server_pid"
server_pid=""
echo "HTTPS module: ok"
