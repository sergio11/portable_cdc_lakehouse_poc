# ADR-007 — Rake as the operational layer instead of shell scripts

- **Status:** Accepted
- **Date:** 2026-10-17

## Context

SRS 10.3 lists the operational commands as POSIX shell invocations
(`podman compose up -d`, `sbt test`, `scripts/run-direct.sh`, …). A `.sh` script
layer is not portable: it cannot run natively on Windows without WSL or Git Bash,
and it forces every contributor onto a POSIX environment even though the POC is
otherwise containerised and platform-independent.

## Decision

All operations are expressed as Rake tasks in `Rakefile` + `rakelib/*.rake`.
There are no shell scripts in the repository.

Consequences accepted:

- Rake becomes a real toolchain dependency, so it is version-pinned in
  `Gemfile`/`Gemfile.lock` for reproducibility.
- `rake -T` is the single documented index of every operational command, which
  the README links to instead of maintaining a second command list.
- Task bodies invoke external tools with argv arrays (`system("podman", "run",
  ...)`), never interpolated shell strings, so paths and values cannot be
  reinterpreted by a shell.

This is a deliberate, documented deviation from SRS 10.3: the *capability* is
unchanged (bring the stack up, run tests, launch pipelines, replay), only the
mechanism differs.

## Consequences

- One entrypoint works identically on Windows, Linux and macOS.
- Build scripts stay in Ruby, a language with real error handling, instead of
  shell constructs such as `|| true`, backgrounding or unchecked pipes.
- Readiness is expressed as probes with timeouts and actionable error messages
  rather than `sleep`.