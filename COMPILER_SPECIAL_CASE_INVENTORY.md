# Compiler Special-Case Inventory

This is the Phase 3.5 baseline.  It distinguishes a real native boundary from
an historical compiler-only shortcut.  A native boundary may remain only when
it needs a runtime representation, operating-system authority, or a stable
extension ABI.  Everything else should be executable Tyrion source and travel
through the same module, lowering, object, and link pipeline as an application.

## Classification

| Class | Meaning | Phase 3.5 treatment |
| --- | --- | --- |
| Retain | Requires an OS service, opaque runtime representation, or versioned ABI. | Keep it in `compiler/primitives.ty`; document its owner and test its contract. |
| Generalize | Is compiler support today, but is useful as a reusable language/runtime facility. | Keep one implementation behind a normal primitive or shared module. |
| Remove | Exists solely because the compiler was treated as a special executable. | Replace with ordinary application lowering, then delete after succession tests. |

## Compiler-only lowering and reachability routes

### Remove: compiler-CLI native fast path

The following chain is compiler-specific and has no application equivalent:

- `compiler/cli.ty`: `cli_route_info_add_test`, `cli_route_info`,
  `cli_test_value`, `cli_spec_record_route`, and `cli_route_records_to_spec`
  inspect the source of `cli_main` and convert a fixed set of `argv` tests into
  a private route specification.
- `compiler/backend/components.ty`: `compiler_cli_minimal_helpers_available`,
  `materialize_cli_route_records`, `emit_compiler_cli_asm_into`,
  `render_compiler_cli_artifact`, and the `compiler-cli` preamble/entry
  emitters turn that specification into dedicated assembly.
- `compiler/lowering.ty`: `build_compiler_cli_native`, `collect_cli_spec`,
  `emit_compiler_cli_asm_into`, and `render_compiler_cli_artifact` have unique
  plan kinds, fixed arities, reachability entries, slot conventions, and
  handler wrappers.  `build_native` forcibly includes this graph.
- `compiler/backend/exec_functions.ty`: dedicated dispatch bodies call the
  special CLI emitter rather than normal function lowering.

Replacement order:

1. Make `cli_main` an ordinary source entry point receiving the standard argv
   value provided to every CLI application.
2. Remove `collect_cli_spec`/route inspection; ordinary control flow handles
   help, version, build, and diagnostic routes.
3. Route compiler builds through the same application IR, object manifest, and
   final link manifest as an arbitrary `.ty` CLI.
4. Delete the dedicated plan kinds, wrappers, reachability entries, artifact
   labels, and per-target compiler-CLI emitters only after c1/c2/c3/c4 and
   corpus success on all Tier 1 targets.

### Generalize: compiler-source construction helpers

These are currently fixed lowering entries but describe compiler work rather
than a privileged compiler executable:

- Lexer/parser helpers: `lex`, `token`, token accessors, `parser_expect`,
  `parser_skip_newlines`, `parse_expression`, and `parse_program`.
- Compiler graph helpers: `program_has_function`,
  `program_find_function[_or_none]`, inventory-row decoding, and source
  formatting helpers.
- Assembly construction helpers: `asm_line`, `asm_label`, `asm_dir`,
  `asm_raw`, `asm_section`, `asm_cstr`, and address/label helpers.

The first two groups should progressively become ordinary source functions as
the executable evaluator gains the necessary representation operations.  The
assembly formatting helpers can remain shared compiler-library code, but must
not cause a distinct compiler entry pipeline.  They are not user-language
builtins.

### Retain: target and object boundary

`compiler/target_contract.ty`, `compiler/object_model.ty`, and
`compiler/link_manifest.ty` own target selection, object validation, cache
publication, and host-link permission.  These are common compilation services:
the compiler, interpreter, and user programs must invoke the same API.  The
target contract explicitly allows assembly/object production for a configured
target while allowing link/run only on a compatible native host.

## C-bootstrap evaluator branches

`tyrionc/src/eval.c` currently dispatches named calls directly.  The branch
names are not automatically a problem: the bootstrap is allowed to implement
the Tyrion runtime.  The issue is whether a branch duplicates compiler policy.

### Retain: irreducible ABI and OS boundaries

| Boundary | C evaluator names | Owning ABI module | Rationale |
| --- | --- | --- | --- |
| Source/cache persistence | `read_text_file`, `read_module_graph`, `write_text_file`, `__tyrion_native_cache_encode`, `__tyrion_native_cache_decode` | `compiler-file`, `compiler-cache` | Filesystem authority and durable deterministic cache codec. |
| External tools/linking | `__tyrion_run_tool`, `__tyrion_native_link` | `process`, target link contract | Process spawning and platform linker invocation. |
| Streaming assembly sink | `asm_writer_open`, `asm_writer_open_memory`, `asm_writer_append`, `asm_writer_close`, `asm_writer_bytes`, `asm_writer_text`, `asm_writer_chunks`, `asm_writer_set_backend`, `asm_writer_is_x86` | `compiler-writer` | Opaque streaming writer and bounded emission memory. |
| Token/AST storage | `__tyrion_native_ast_builder_*`, `__tyrion_native_token_builder_*` | `compiler-ast` | Bounded opaque compiler representation. |
| Native extensions | `__tyrion_native_host_load`, `__tyrion_native_host_call`, `__tyrion_native_host_unload`, `std_ext_*` | `host-extension` | Dynamic-library and versioned extension boundary. |
| Network/terminal/clock | `__tyrion_native_tcp_*`, `std_io_tcp_*`, `input`, `__tyrion_current_exe`, `__tyrion_native_getenv`, `__tyrion_native_wall_time` | `tcp`, `terminal`, `clock` | OS handles, environment, and time. |

The primitive schema in `compiler/primitives.ty` is the canonical declaration
for these boundaries: capability, ownership, effect, and implementation key
must be added there before a C evaluator branch is added.

### Complete primitive catalogue

This catalogue covers all 57 schema rows. `Generalize` means the operation is
part of Tyrion semantics and must not acquire compiler-specific behavior;
`Retain` means only the ABI/OS implementation remains native.

| IDs | Names | Class | Owning ABI | Rationale |
| --- | --- | --- | --- | --- |
| 1-4 | `len`, `int`, `str`, `range` | Generalize | `core`, `collections` | Shared language semantics. |
| 5 | `open` | Retain | `file` | File-handle representation and filesystem authority. |
| 6-7, 55 | `read_key`, `ansi_escape`, `input` | Retain | `terminal` | Terminal device boundary; public behavior remains ordinary runtime semantics. |
| 8, 37 | `__tyrion_native_wall_time`, `time` | Retain | `clock` | Host clock query. |
| 9-16 | `__tyrion_native_tcp_*`, `std_io_tcp_*` | Retain | `tcp` | Socket handles, binary buffers, and network syscalls. |
| 17-19 | `read_text_file`, `read_module_graph`, `write_text_file` | Retain | `compiler-file` | Source and cache filesystem authority. |
| 20, 49 | `run_tool`, `__tyrion_run_tool` | Retain | `process` | Child-process invocation and captured result ownership. |
| 21-26 | `asm_writer_*` | Retain | `compiler-writer` | Opaque bounded streaming emitter sink. |
| 27-36 | `float`, `enumerate`, `bool`, `zip`, `sum`, `min`, `max`, `any`, `all`, `sorted` | Generalize | `core`, `collections` | Portable language behavior, with native runtime acceleration allowed. |
| 38-40 | `__tyrion_native_host_load`, `_call`, `_unload` | Retain | `host-extension` | Dynamic-library ABI and opaque host handles. |
| 41-48 | `__tyrion_native_ast_builder_*`, `__tyrion_native_token_builder_*` | Retain | `compiler-ast` | Bounded opaque token/AST storage. |
| 50-51 | `lex`, `program_has_function` | Generalize | `compiler-ast` | Compiler-library semantics; representation access remains bounded. |
| 52-53 | `__tyrion_native_tls_write`, `_read` | Retain | `host-extension` | TLS helper ABI and binary transfer boundary. |
| 54 | `__tyrion_native_ascii_text` | Generalize | `core` | Deterministic bytes-to-text conversion policy. |
| 56-57 | `__tyrion_native_cache_encode`, `_decode` | Retain | `compiler-cache` | Canonical durable cache codec. |

The schema aliases (`std_io_tcp_*` to `__tyrion_native_tcp_*`) are ABI
compatibility names, not independent implementations.  A future primitive
must be rejected unless this table, the schema row, and a target capability
test agree on its owner and rationale.

### Generalize: language semantics currently dispatched in C

The evaluator also contains branches for `range`, `len`, `str`, `int`,
`float`, `bool`, `enumerate`, `zip`, `sum`, `min`, `max`, `any`, `all`,
`sorted`, `map`, `filter`, deletion helpers, dictionary construction/union,
and `method_append`.  These are Tyrion language/runtime semantics, not
compiler-specific semantics.  They may use runtime support while bootstrapping,
but their behavior must be defined by the shared language implementation and
covered by interpreter/native parity tests.  New collection and guard work
must not add compiler-only evaluator behavior.

### Remove: compiler-policy duplication

No C evaluator branch should inspect compiler `cli_main`, decide compiler CLI
routes, select a compiler-only emitter, or choose a compiler-only final link
path.  Those decisions belong to the ordinary compiler source and target/link
contracts.  The current C evaluator does not contain the CLI route parser;
the duplication is in the generated compiler lowering described above.

## First removal completed

The normal `build_native` reachability set no longer includes
`build_compiler_cli_native`, and both lowered-function table builders no
longer inject compiler-CLI target emitters or the compatibility builder.  The
ordinary application builder remains the only route from `build_native` to
native compilation.  The dedicated compatibility implementation remains
unreachable pending c1/c2/c3/c4 deletion evidence.

## Completion evidence required for the removal

For each deletion of a compiler-only path:

1. A normal application fixture and the compiler both use the replacement
   module/IR/object/link path.
2. c1, c2, c3, and c4 build the compiler with no legacy route symbols
   reachable; inspect the link manifest and symbol inventory.
3. Build/run the interpreter, HTTP/TLS fixture, terminal fixture, and corpus
   on macOS ARM64, Linux ARM64, and bounded Linux x86-64.
4. Prove an unchanged compiler rebuild reuses object shards and only relinks;
   a private edit recompiles its shard; a public interface edit invalidates
   dependants.
