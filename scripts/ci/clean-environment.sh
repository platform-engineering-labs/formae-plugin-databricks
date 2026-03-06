#!/bin/bash
# © 2025 Platform Engineering Labs Inc.
# SPDX-License-Identifier: FSL-1.1-ALv2
#
# Clean Environment Hook
# ======================
# Called before AND after conformance tests. Must be idempotent.
# Deletes resources in dependency order: jobs → clusters → pools.

set -euo pipefail

TEST_PREFIX="${TEST_PREFIX:-formae-plugin-sdk-test-}"

echo "clean-environment.sh: Cleaning resources with prefix '${TEST_PREFIX}'"

if ! command -v databricks &>/dev/null; then
    echo "  databricks CLI not found, skipping cleanup"
    exit 0
fi

# Delete jobs matching prefix
echo "  Cleaning jobs..."
databricks jobs list --output json 2>/dev/null | \
    jq -r --arg prefix "$TEST_PREFIX" '.jobs[]? | select(.settings.name | startswith($prefix)) | .job_id' 2>/dev/null | \
    while read -r job_id; do
        echo "    Deleting job $job_id"
        databricks jobs delete "$job_id" 2>/dev/null || true
    done

# Delete clusters matching prefix
echo "  Cleaning clusters..."
databricks clusters list --output json 2>/dev/null | \
    jq -r --arg prefix "$TEST_PREFIX" '.clusters[]? | select(.cluster_name | startswith($prefix)) | .cluster_id' 2>/dev/null | \
    while read -r cluster_id; do
        echo "    Deleting cluster $cluster_id"
        databricks clusters permanent-delete "$cluster_id" 2>/dev/null || true
    done

# Delete instance pools matching prefix
echo "  Cleaning instance pools..."
databricks instance-pools list --output json 2>/dev/null | \
    jq -r --arg prefix "$TEST_PREFIX" '.instance_pools[]? | select(.instance_pool_name | startswith($prefix)) | .instance_pool_id' 2>/dev/null | \
    while read -r pool_id; do
        echo "    Deleting instance pool $pool_id"
        databricks instance-pools delete "$pool_id" 2>/dev/null || true
    done

echo "clean-environment.sh: Cleanup complete"
