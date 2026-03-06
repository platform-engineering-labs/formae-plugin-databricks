# Databricks Plugin Setup & Status

## Resources

| Resource | Type | Status | Async |
|---|---|---|---|
| Instance Pool | `Databricks::Compute::InstancePool` | Implemented | No |
| Job | `Databricks::Jobs::Job` | Implemented | No |
| Cluster | `Databricks::Compute::Cluster` | Implemented | Create/Update async |

## Environment

```bash
export DATABRICKS_HOST="https://adb-7474646302211978.18.azuredatabricks.net"
export DATABRICKS_TOKEN="<your-pat-token>"
```

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
