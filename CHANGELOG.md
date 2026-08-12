# Changelog

All notable changes to the formae Databricks plugin are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Install with `sudo formae plugin install databricks` on the host that runs the
formae agent.

## [0.1.1]

### Added

- The `token` config field accepts a resolvable, so the personal access token
  can be sourced from a formae-managed secret. The agent resolves it live before
  every call, so onboarding a workspace or rotating the token needs no agent
  restart, and the token is stored as a reference rather than a literal.
- A token that is declared but resolves to an empty value is now rejected.
  Previously it fell through to the SDK credential chain, which can succeed
  against a local CLI profile or Azure login and authenticate as a different
  identity than the one the forma names. Omitting the token remains the way to
  use that chain deliberately.

### Changed

- `token` is declared mutable, so rotating it updates the target rather than
  replacing it.
- Requires formae 0.89.0 or later, which resolves references in target config,
  and the schema is built against the `formae@0.89.0` package.

## [0.1.0]

### Added

- Initial release of the Databricks plugin as a standalone package built on the
  formae Plugin SDK.
