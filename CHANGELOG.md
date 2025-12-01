# Changelog

All notable changes to this project are documented in this file.

## [0.3.9] - 2025-12-01
- Hotfix: AOT builds now use a bundled crate copy if installed (e.g., Homebrew `share/tyrion_src`), fall back to `CARGO_MANIFEST_DIR`/`TYRION_CRATE_ROOT`, and otherwise pull the published crate by version, so `tyrion --build` works outside the repo (including Homebrew installs).

## [0.3.8] - 2025-11-30
- Hotfix: AOT builds now locate the Tyrion crate root via `CARGO_MANIFEST_DIR` or `TYRION_CRATE_ROOT`, so `tyrion --build` works when invoked outside the repo directory.

## [0.3.7] - 2025-11-30
- Fixed AOT call dispatch to forward keyword arguments to native/bound methods and class initializers, restoring the requests demo and other keyword-using code paths in AOT builds.
- Added `len` as a builtin so list/tuple/dict/set/str lengths work in both interpreted and AOT execution paths.

## [0.3.6] - 2025-11-30
- Fixed session cookie handling by routing requests through a shared cookie jar that persists redirect-set cookies and keeps Session cookies up to date.
- Response cookies now reflect cookies accumulated during redirects

## [0.3.5] - 2025-11-30
- Added a built-in requests module providing a blocking HTTP client with get/post helpers, Sessions with shared defaults, redirect and timeout controls, binary-safe request/response bodies, multipart and form-urlencoded support, improved error messages, and response helpers (json, raise_for_status, iter_content, iter_lines, redirect history).
- Expanded native function support for keyword arguments to align with requests-style APIs.
- Enhanced networking and TLS features: configurable default timeout via TYRION_REQUESTS_TIMEOUT; SSL verification toggle, custom CA bundles, and client certificate support; improved proxy handling including HTTP/HTTPS proxies, environment-driven defaults, and robust NO_PROXY matching (CIDRs, host:port, wildcards).
- Implemented a richer cookie jar with host-only vs domain cookies, secure/httponly flags, expiry tracking, and correct domain/path matching for request filtering.
- Improved multipart handling with content-type hints and safer binary upload/download behavior.
- Cleaned up minor build warnings in the requests runtime (removal of unused helper and unnecessary mutability).

## [0.3.4] - 2025-11-29
- Added generator/yield execution with keyword and default arguments to support streaming and tailing-style scripts.
- Added interrupt signaling plus built-in `sleep` and `time` helpers.
- Expanded file I/O with seek/read operations and new string helpers.

## [0.3.3] - 2025-11-29
- Version bump release; no functional changes.

## [0.3.2] - 2025-11-29
- Version bump release; no functional changes.

## [0.3.1] - 2025-11-29
- Added a `--version` flag to the `tyrion` CLI.

## [0.3.0] - 2025-11-29
- Introduced ahead-of-time code generation (`tyrion --build`) that emits Rust for native binaries.
- Improved AOT output with performance optimizations.

## [0.2.0] - 2025-11-29
- Fixed parser comment skipping.
- Added brace-delimited blocks and Ruby-style guard clauses.
- Continued interpreter work in Rust, including generator infrastructure.

## [0.1.1] - 2025-11-28
- First published release.
- Added class support and initial examples.
- Added project license.
