# Changelog

All notable changes to this project are documented in this file.

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
