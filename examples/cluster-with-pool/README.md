# Cluster with Pool

Databricks cluster backed by an instance pool, with a scheduled job.

## What You Get

- Instance Pool (i3.xlarge, up to 10 instances)
- Cluster (Spark 15.4 LTS, 2 workers, auto-terminates after 60 min)
- Scheduled Job (daily ETL at 8:00 UTC)

## Prerequisites

1. Databricks CLI authenticated: `databricks auth login`
2. A Databricks workspace with classic compute enabled (HYBRID mode)

## Configuration

Set your workspace URL:

```bash
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
```

## Deploy

```bash
formae apply --mode reconcile examples/cluster-with-pool/main.pkl
formae status command --watch --output-layout detailed
```

## Tear Down

```bash
formae destroy --query 'stack:cluster-with-pool'
```

## Architecture

```
Instance Pool (shared-compute-pool)
└── Cluster (analytics)
    └── Job (daily-etl)
        └── Task: extract (/Repos/etl/extract)
```

## Notes

- Cluster creation takes 5-8 minutes (instance acquisition + Spark bootstrap)
