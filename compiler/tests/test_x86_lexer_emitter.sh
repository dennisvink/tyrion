#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <tyrionc-bootstrap>" >&2
  exit 2
fi

bootstrap=$1
repo_root=$(cd -- "$(dirname -- "$0")/../.." && pwd)
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/tyrion-x86-lexer-emitter.XXXXXX")
trap 'rm -rf "$work_dir"' EXIT
assembly="$work_dir/lexer-runtime.s"
ln -s "$repo_root/compiler" "$work_dir/compiler"
cp "$repo_root/compiler/tests/x86_lexer_emitter_probe.ty" "$work_dir/probe.ty"

"$bootstrap" --exec-call \
  "$work_dir/probe.ty" \
  main "$assembly" >/dev/null

cc -c "$assembly" -o "$work_dir/lexer-runtime.o"
test "$(grep -c '^\.L_tyrion_direct_lex_simple_amp:' "$assembly")" = 1
test "$(grep -c '^\.L_tyrion_direct_lex_simple_caret:' "$assembly")" = 1
test "$(grep -c '^\.L_tyrion_direct_lex_simple_pipe:' "$assembly")" = 1
if sed -n '/^\.L_tyrion_direct_lex_newline:/,/^\.L_tyrion_direct_lex_newline_advance:/p' "$assembly" |
  grep -Fx $'\taddq $1, %r8' >/dev/null; then
  echo "x86 lexer emitted the obsolete newline position adjustment" >&2
  exit 1
fi

echo "x86 lexer emitter: canonical streamed runtime assembles without duplicate labels"
