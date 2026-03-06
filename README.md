# Databricks Plugin for Formae

[![CI](https://github.com/platform-engineering-labs/formae-plugin-databricks/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/platform-engineering-labs/formae-plugin-databricks/actions/workflows/ci.yml)

Formae plugin for managing Databricks workspace resources.

## Supported Resources

| Resource Type | Description | Async |
|---------------|-------------|-------|
| `Databricks::Compute::InstancePool` | Instance pools | No |
| `Databricks::Compute::Cluster` | All-purpose clusters | Create/Update |
| `Databricks::Jobs::Job` | Workflow jobs | No |

## Installation

```bash
make install
```

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
