# Incremental compilation

Tyrion compiler caches are optional. Pass `--cache-dir <directory>` to reuse
frontend and generated-code work; omitting the option preserves the uncached
build path.

## Cache boundaries

- The C bootstrap caches the deterministic module graph and records every
  transitive source dependency with its size, modification time, and content
  hash.
- The Tyrion compiler independently caches its pointer-free module graph and
  module-unit inventory. Warm validation rereads every known source (including
  the built-in core module) and requires an exact byte match. The validated
  units are handed directly to the frontend, so a build traverses the module
  graph only once rather than rebuilding it before AST lookup.
- The Tyrion frontend caches each module's pointer-free parsed AST. A module
  fingerprint contains its complete source, path, compilation context, and
  recursive dependency fingerprints.
- Linux x86-64 gives each lowered function its own deterministic constant pool
  and caches the resulting pointer-free Native IR unit. Unit identities bind
  compiler context, target layout, function identity, and recursive source
  module fingerprint. A validated hit therefore skips lowering and per-function
  verification.
- Linux x86-64 separately caches canonical assembly fragments for verified
  lowering units. A warm staged hit skips lowering, verification, and emission;
  the compiler still reconstructs the aggregate runtime inventory and performs
  the final native assembly and link.
- Linux x86-64 runtime templates are canonical emitter source. Neither the C
  bootstrap writer nor a generated compiler performs late string surgery on
  the streamed assembly, so both paths publish the same template bytes.
- AArch64 caches the deterministic whole-program assembly. Both target paths
  still assemble and perform the final native link on every build.

Cache contexts include compiler version, target backend, extension options,
extension root, and runtime ABI version. Cache filenames are only indexes; each
record retains and verifies its complete fingerprint, so a filename collision
cannot publish stale code.

AST and generated-assembly records are published by writing a temporary file
and atomically renaming it. A missing, malformed, truncated, incompatible, or
fingerprint-mismatched record is treated as a miss and regenerated.
Temporary emitter fragments are removed before publication. Cache event
collection is disabled for ordinary builds and enabled explicitly by the test
suite with `--cache-stats`, so diagnostics add no production-path file I/O and
do not participate in cache identity.

## Staged successor flow

The C bootstrap remains the trusted seed, but compiler generation is divided
into explicit reusable stages:

1. load and validate the deterministic module graph;
2. reuse or parse pointer-free module AST records;
3. lower validated module/function units;
4. reuse or emit target assembly records; and
5. always perform the final native assembly and link.

The resulting self-hosted compiler is then the ordinary compiler used to build
the interpreter and application fixtures. A full successor generation is a
checkpoint after focused cache and semantic probes, not the iteration path for
each language capability.

## Verification

Run all compiler and fixture commands with an 8 GiB process-tree memory cap.

```sh
make test
bash tests/test_module_graph_cache.sh
bash tests/test_ast_cache.sh
# From the Tyrion repository on Linux x86-64:
bash compiler/tests/test_x86_lexer_emitter.sh ../tyrionc/build/tyrionc
bash compiler/tests/test_x86_module_graph_emitter.sh
bash compiler/tests/test_x86_lowering_unit_cache.sh ../tyrionc/build/tyrionc
# Portable compiler-owned extension ABI boundary:
bash compiler/tests/test_toolchain_dynamic_extension.sh
```

`test_module_graph_cache.sh` proves cold/warm graph reuse, deterministic output,
target/compiler-version/ABI invalidation, corrupt-entry recovery, and absence
of incomplete publications.

`test_ast_cache.sh` proves:

- cold parsing and generation followed by warm reuse;
- byte-identical cached assembly and executables;
- identical executable output;
- compiler-option, target, compiler-version, and ABI invalidation;
- corrupt AST and whole-assembly recovery;
- recovery after deletion of one x86-64 function unit, with only that unit
  regenerated;
- recovery after deletion or corruption of one verified x86-64 lowering unit,
  with compatible lowering and assembly units retained;
- reverse-dependent invalidation after editing an imported module, while
  unrelated units remain reusable;
- no leftover incomplete records; and
- a measured warm-build improvement.

`test_x86_lowering_unit_cache.sh` is the focused staged-backend probe. It
forces the whole-program record to miss while retaining verified lowering and
assembly-unit records, then checks cold/warm executable output, byte-identical
assembly, lowering/unit hit events, and separate cold/warm timings.

### Evidence - 2026-08-21

| Host | Cold | Warm | Result |
| --- | ---: | ---: | --- |
| macOS ARM64 | 0.833 s | 0.783 s | pass; peak process-group RSS 179,120 KiB |
| Docker Linux ARM64 | 4.953 s | 0.736 s | pass; current source, 8 GiB container cap |
| Docker Linux x86-64 | 42.924 s | 1.184 s | pass; 8 GiB container cap |
| Remote Linux x86-64 | 87.985 s | 2.005 s | pass; 8 GiB address-space cap |
| Remote Linux x86-64 (current-source staged fixture) | 886.576 s | 406.289 s | pass; 8 GiB address-space cap; whole-program record forced cold while lowering/assembly units were reused |

Source-exact successor checkpoint on the remote Linux x86-64 host completed with
exit status 0 after 6:43:21 wall time. Peak RSS was 6,530,860 KiB under the
8 GiB address-space cap; the emitted assembly was 440,310,313 bytes and the
linked compiler was 84 MiB. The successor reported `tyrion compiler 1.0.0`
and passed the capped `--scan` CLI check against a compiler fixture.

The patched current-source path also completed a capped self-host probe on the
same host in 15:41.66, with peak RSS 111,872 KiB and exit status 0. It emitted
6,985,188 bytes of assembly, linked a 1.5 MiB probe, and the probe executable
returned `False` as expected.

The C runtime/evaluator self-tests, ownership test, module-graph cache test, and
GCC ASan/UBSan run with leak detection pass in the capped Linux ARM64 and
x86-64 Docker builders. The canonical portable cache codec and the source
embedded into native compiler builds are byte-identical.

The compiler-owned toolchain extension also exports the constrained dynamic
manifest used by `--ext-static=off --ext-dynamic=allowed` successors. Its
manifest name, ABI version, call symbol resolution, and dispatch pass on
macOS ARM64, Linux ARM64, Linux AMD64, and the remote Intel host; static
extension builds continue to use the same dispatch symbol without the dynamic
manifest surface.

### Evidence - 2026-08-22

The clean-source C-bootstrap gates passed under an 8 GiB Docker memory limit
on both Linux architectures. Runtime self-tests, evaluator ownership tests,
module-graph cache tests, and the full staged AST/lowering-unit cache suite
passed. The measured cache suite timings were 41.186 s cold / 0.968 s warm on
Linux x86-64 and 4.612 s cold / 0.566 s warm on Linux ARM64. The tests verified
deterministic output, target/version/ABI and option invalidation, deletion and
corruption recovery, per-unit reuse, dependency invalidation, and atomic
publication.

The current-source ARM64 successor chain produced `c1` successfully; its
patched interpreter build exited 0 in 49.40 s with peak RSS 101,312 KiB. It
reported `tyrion interpreter 1.0.0`, and bounded probes for ordinary calls,
starred calls, and `examples/hello.ty` all passed.

The current-source successor on the remote Linux x86-64 host passed its
bounded CLI checks (`--version`, `--help`, and `--scan`). The patched successor
interpreter build exited 0 in 25:20.86 with peak RSS 523,368 KiB, emitted
35 MiB of assembly, and linked a 7.9 MiB binary. It reported
`tyrion interpreter 1.0.0`; ordinary, append, zero-argument, variable-starred,
and fixed-starred call probes all passed under the 8 GiB address-space cap.

The patched macOS ARM64 interpreter was rebuilt from the current source with
the Darwin `taskpolicy -m 8192` memory cap. The native C1 successor and
interpreter build both completed successfully; the interpreter reported
`tyrion interpreter 1.0.0`, and ordinary, variable-starred, fixed-starred, and
`examples/hello.ty` probes passed under the same cap.

The portability fix used by both interpreter artifacts is structural: the
parser stores ordinary positional arguments and starred arguments as explicit
records, so VM call scheduling no longer probes an integer node id as a
dictionary under an exception boundary. This is verified on Linux ARM64,
Linux x86-64, and macOS ARM64; no release or deployment was performed.
