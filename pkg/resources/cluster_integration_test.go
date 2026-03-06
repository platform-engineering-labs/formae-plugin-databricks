// © 2025 Platform Engineering Labs Inc.
//
// SPDX-License-Identifier: FSL-1.1-ALv2

//go:build integration

package resources

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/platform-engineering-labs/formae/pkg/plugin/resource"
)

func newTestCluster(t *testing.T) *Cluster {
	t.Helper()
	return &Cluster{Client: newTestClient(t)}
}

func testClusterName(suffix string) string {
	return testPoolName("cluster-" + suffix)
}

func deleteCluster(ctx context.Context, cl *Cluster, nativeID string) {
	_, _ = cl.Delete(ctx, &resource.DeleteRequest{NativeID: nativeID})
}

// waitForCluster polls Status until the cluster reaches a terminal state (Success or Failure).
func waitForCluster(t *testing.T, ctx context.Context, cl *Cluster, requestID, nativeID string, timeout time.Duration) *resource.StatusResult {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		result, err := cl.Status(ctx, &resource.StatusRequest{
			RequestID: requestID,
			NativeID:  nativeID,
		})
		require.NoError(t, err)

		switch result.ProgressResult.OperationStatus {
		case resource.OperationStatusSuccess, resource.OperationStatusFailure:
			return result
		case resource.OperationStatusInProgress:
			t.Logf("Cluster %s: %s", nativeID, result.ProgressResult.StatusMessage)
			time.Sleep(15 * time.Second)
		default:
			t.Fatalf("Unexpected status: %s", result.ProgressResult.OperationStatus)
		}
	}
	t.Fatalf("Cluster %s did not reach terminal state within %v", nativeID, timeout)
	return nil
}

// latestSparkVersion returns a recent LTS Spark version.
// Databricks requires a valid spark version string.
func latestSparkVersion() string {
	return "15.4.x-scala2.12"
}

func TestCluster_CreateReadDeleteLifecycle(t *testing.T) {
	ctx := context.Background()
	prov := newTestCluster(t)
	name := testClusterName("lifecycle")

	numWorkers := 1
	autoTermMin := 10
	props, _ := json.Marshal(clusterProps{
		ClusterName:            name,
		SparkVersion:           latestSparkVersion(),
		NodeTypeId:             "i3.xlarge",
		NumWorkers:             &numWorkers,
		AutoterminationMinutes: &autoTermMin,
		DataSecurityMode:       "SINGLE_USER",
	})

	// Create (async)
	createResult, err := prov.Create(ctx, &resource.CreateRequest{
		ResourceType: ResourceTypeCluster,
		Label:        "test-cluster",
		Properties:   props,
	})
	require.NoError(t, err)
	require.NotNil(t, createResult.ProgressResult)
	assert.Equal(t, resource.OperationStatusInProgress, createResult.ProgressResult.OperationStatus)
	assert.NotEmpty(t, createResult.ProgressResult.NativeID)
	assert.NotEmpty(t, createResult.ProgressResult.RequestID)

	nativeID := createResult.ProgressResult.NativeID
	requestID := createResult.ProgressResult.RequestID
	t.Logf("Created cluster: %s (ID: %s)", name, nativeID)
	t.Cleanup(func() { deleteCluster(ctx, prov, nativeID) })

	// Wait for running/terminated
	statusResult := waitForCluster(t, ctx, prov, requestID, nativeID, 10*time.Minute)
	assert.Equal(t, resource.OperationStatusSuccess, statusResult.ProgressResult.OperationStatus)
	t.Logf("Cluster reached terminal state")

	// Read
	readResult, err := prov.Read(ctx, &resource.ReadRequest{
		NativeID:     nativeID,
		ResourceType: ResourceTypeCluster,
	})
	require.NoError(t, err)
	assert.Equal(t, ResourceTypeCluster, readResult.ResourceType)

	var readProps clusterProps
	require.NoError(t, json.Unmarshal([]byte(readResult.Properties), &readProps))
	assert.Equal(t, name, readProps.ClusterName)
	assert.Equal(t, latestSparkVersion(), readProps.SparkVersion)
	assert.Equal(t, nativeID, readProps.ClusterId)

	// Delete (sync — permanent delete)
	deleteResult, err := prov.Delete(ctx, &resource.DeleteRequest{NativeID: nativeID})
	require.NoError(t, err)
	assert.Equal(t, resource.OperationStatusSuccess, deleteResult.ProgressResult.OperationStatus)

	// Verify gone
	_, err = prov.Read(ctx, &resource.ReadRequest{
		NativeID:     nativeID,
		ResourceType: ResourceTypeCluster,
	})
	assert.Error(t, err)
}

func TestCluster_List(t *testing.T) {
	ctx := context.Background()
	prov := newTestCluster(t)

	// Just verify List doesn't error — there may or may not be clusters
	listResult, err := prov.List(ctx, &resource.ListRequest{
		ResourceType: ResourceTypeCluster,
	})
	require.NoError(t, err)
	require.NotNil(t, listResult)
	t.Logf("List returned %d clusters", len(listResult.NativeIDs))
}

func TestCluster_DeleteAlreadyDeleted(t *testing.T) {
	ctx := context.Background()
	prov := newTestCluster(t)
	name := testClusterName("delete-idem")

	numWorkers := 1
	autoTermMin := 10
	props, _ := json.Marshal(clusterProps{
		ClusterName:            name,
		SparkVersion:           latestSparkVersion(),
		NodeTypeId:             "i3.xlarge",
		NumWorkers:             &numWorkers,
		AutoterminationMinutes: &autoTermMin,
		DataSecurityMode:       "SINGLE_USER",
	})

	createResult, err := prov.Create(ctx, &resource.CreateRequest{
		ResourceType: ResourceTypeCluster,
		Label:        "test-cluster",
		Properties:   props,
	})
	require.NoError(t, err)
	nativeID := createResult.ProgressResult.NativeID

	// Delete once
	_, err = prov.Delete(ctx, &resource.DeleteRequest{NativeID: nativeID})
	require.NoError(t, err)

	// Delete again — should succeed (idempotent)
	deleteResult, err := prov.Delete(ctx, &resource.DeleteRequest{NativeID: nativeID})
	require.NoError(t, err)
	assert.Equal(t, resource.OperationStatusSuccess, deleteResult.ProgressResult.OperationStatus)
}
