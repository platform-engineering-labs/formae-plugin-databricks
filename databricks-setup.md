# Databricks Plugin Setup & Status

## Resources

| Resource | Type | Status | Async |
|---|---|---|---|
| Instance Pool | `Databricks::Compute::InstancePool` | Implemented | No |
| Job | `Databricks::Jobs::Job` | Implemented | No |
| Cluster | `Databricks::Compute::Cluster` | Implemented | Create/Update async |

## Authentication

### Local Development

Use the Databricks CLI to authenticate (no PAT token needed):

```bash
export DATABRICKS_HOST="https://dbc-07926c57-40d1.cloud.databricks.com"
databricks auth login --host "$DATABRICKS_HOST"
```

The SDK's `DefaultCredentials` chain picks up the CLI profile automatically.

### CI (GitHub Actions)

CI uses **GitHub OIDC federation** — no static secrets. The Databricks Go SDK's
native `github-oidc` credential strategy (strategy #6) exchanges a GitHub OIDC
token directly with the Databricks OIDC endpoint.

Required GitHub secrets/variables:
- `DATABRICKS_HOST` — workspace URL
- `DATABRICKS_CLIENT_ID` — Databricks service principal client ID

The service principal must have OIDC federation configured to trust
`https://token.actions.githubusercontent.com` with the appropriate subject claims.

## Testing

```bash
# Build and install
make install

# Run conformance tests (Instance Pool)
make conformance-test

# Clean up test resources
make clean-environment
```

## Conformance Tests

Test fixtures are in `testdata/compute/instancepool/`:
- `instance-pool.pkl` — create
- `instance-pool-update.pkl` — in-place update (name, maxCapacity, autoterminationMinutes)
- `instance-pool-replace.pkl` — replacement (nodeTypeId is createOnly)

## Known Issues

- Cluster async status: `TERMINATED` state after auto-termination is treated as success
- Job SDK uses `int64` IDs internally; converted to string NativeIDs
