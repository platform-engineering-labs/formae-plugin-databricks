# Databricks plugin for formae

[![CI](https://github.com/platform-engineering-labs/formae-plugin-databricks/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/platform-engineering-labs/formae-plugin-databricks/actions/workflows/ci.yml)
[![Monthly](https://github.com/platform-engineering-labs/formae-plugin-databricks/actions/workflows/monthly.yml/badge.svg?branch=main)](https://github.com/platform-engineering-labs/formae-plugin-databricks/actions/workflows/monthly.yml)

Manages Databricks clusters, instance pools and jobs as Infrastructure As Code with [formae](https://github.com/platform-engineering-labs/formae).

[formae](https://github.com/platform-engineering-labs/formae) · [Hub](https://hub.platform.engineering/platform.engineering/databricks)

## Install

Requires the formae CLI: see the [quick start](https://docs.formae.ai/documentation/get-started/quickstart).

```bash
formae plugin install databricks
```

To build and install from source instead: `make install`.

Restart the formae agent afterwards so it loads the plugin.

**New project:** with the agent running, `formae project init --include databricks my-project` creates `my-project` with a `PklProject` that declares the formae and databricks schema packages, so `import "@databricks/..."` resolves, and a starter `main.pkl`. Don't run it in an existing project: it overwrites both files.

**Existing project:** add the plugin to `dependencies` in your `PklProject`, with the current version from the [hub page](https://hub.platform.engineering/platform.engineering/databricks), then run `pkl project resolve`:

```pkl
["databricks"] {
  uri = "package://hub.platform.engineering/plugins/databricks/schema/pkl/databricks/databricks@<version>"
}
```

Next: [write your first forma](https://docs.formae.ai/documentation/get-started/write-your-first-forma), then [`formae apply`](https://docs.formae.ai/documentation/reference/cli/apply) (see [apply modes](https://docs.formae.ai/documentation/concepts/apply-modes)).

With an AI coding assistant, use the [formae plugin](https://docs.formae.ai/documentation/guides/ai-coding-assistants) (formerly `formae-mcp`), which can search the hub and fetch plugin examples. The formae documentation is also available as [llms.txt](https://docs.formae.ai/llms.txt).

## Supported Resources

| Resource Type | Description | Async |
|---------------|-------------|-------|
| `DATABRICKS::Compute::InstancePool` | Instance pools | No |
| `DATABRICKS::Compute::Cluster` | All-purpose clusters | Create/Update |
| `DATABRICKS::Jobs::Job` | Workflow jobs | No |

## Configuration

Configure a Databricks target in your Forma file:

```pkl
import "@formae/formae.pkl"
import "@databricks/databricks.pkl"

target: formae.Target = new formae.Target {
  label = "databricks-target"
  config = new databricks.Config {
    host = "https://your-workspace.cloud.databricks.com"
  }
}
```

### Credentials

The plugin uses the Databricks SDK's default credential chain. Configure
credentials using one of:

**Databricks CLI (recommended for local dev):**

```bash
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
databricks auth login --host "$DATABRICKS_HOST"
```

**Environment Variables:**

```bash
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="your-pat-token"
```

**GitHub OIDC (for CI/CD):** The SDK's native `github-oidc` credential strategy
exchanges a GitHub Actions OIDC token directly with Databricks. See
`.github/workflows/ci.yml` for the configuration.

**A formae-managed secret:** the `token` config field accepts a resolvable, so
the personal access token can come from a secret that the agent resolves live
before every call. Onboarding a workspace or rotating the token then needs no
agent restart, and the token is stored as a reference rather than a literal:

```pkl
config = new databricks.Config {
  host = "https://your-workspace.cloud.databricks.com"
  token = databricksToken.res.secretValue
}
```

Omit `token` to use the credential chain above, which stays the default. A
token that is declared but resolves to an empty value is an error rather than a
silent fall back to the chain, which could otherwise authenticate through a
local CLI profile as a different identity than the one the forma names.

## Examples

See the [examples/](examples/) directory for usage patterns:

- `cluster-with-pool/` - Instance pool, cluster, and scheduled job

```bash
# Evaluate an example
formae eval examples/cluster-with-pool/main.pkl

# Apply resources
formae apply --mode reconcile --watch examples/cluster-with-pool/main.pkl
```

## Development

### Prerequisites

- Go 1.25+
- [Pkl CLI](https://pkl-lang.org/main/current/pkl-cli/index.html) 0.30+
- Databricks credentials (for integration/conformance testing)
- AWS credentials (Databricks compute plane runs on AWS)

### Building

```bash
make build      # Build plugin binary
make test-unit  # Run unit tests
make lint       # Run linter
make install    # Build + install locally
```

### Conformance Testing

Run the full CRUD lifecycle + discovery tests:

```bash
make conformance-test                  # Latest formae version
make conformance-test VERSION=0.80.0   # Specific version
```

The `scripts/ci/clean-environment.sh` script cleans up test resources. It runs
before and after conformance tests and is idempotent.

## License

This plugin is licensed under the [Functional Source License, Version 1.1, ALv2
Future License (FSL-1.1-ALv2)](LICENSE).

Copyright 2026 Platform Engineering Labs Inc.
