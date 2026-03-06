// © 2025 Platform Engineering Labs Inc.
//
// SPDX-License-Identifier: FSL-1.1-ALv2

//go:build integration

package resources

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/databricks/databricks-sdk-go/service/compute"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/platform-engineering-labs/formae-plugin-databricks/pkg/client"
	"github.com/platform-engineering-labs/formae-plugin-databricks/pkg/config"
	"github.com/platform-engineering-labs/formae/pkg/plugin/resource"
)

func newTestClient(t *testing.T) *client.Client {
	t.Helper()
	cfg := &config.Config{}
	c, err := client.NewClient(cfg)
	require.NoError(t, err, "Failed to create Databricks client — check CLI auth")
	return c
}

func newTestInstancePool(t *testing.T) *InstancePool {
	t.Helper()
	return &InstancePool{Client: newTestClient(t)}
}

func testPoolName(suffix string) string {
	return fmt.Sprintf("formae-integration-test-%s-%d", suffix, time.Now().Unix())
}

// deletePool is a cleanup helper that ignores errors (pool may already be gone).
func deletePool(ctx context.Context, c *client.Client, poolID string) {
	_ = c.Workspace.InstancePools.Delete(ctx, compute.DeleteInstancePool{
		InstancePoolId: poolID,
	})
}

func TestInstancePool_CreateReadDeleteLifecycle(t *testing.T) {
	ctx := context.Background()
	prov := newTestInstancePool(t)
	name := testPoolName("lifecycle")

	// Create
	props, _ := json.Marshal(instancePoolProps{
		InstancePoolName: name,
		NodeTypeId:       "i3.xlarge",
		MaxCapacity:      intPtr(2),
	})

	createResult, err := prov.Create(ctx, &resource.CreateRequest{
		ResourceType: ResourceTypeInstancePool,
		Label:        "test-pool",
		Properties:   props,
	})
	require.NoError(t, err)
	require.NotNil(t, createResult.ProgressResult)
	assert.Equal(t, resource.OperationStatusSuccess, createResult.ProgressResult.OperationStatus)
	assert.NotEmpty(t, createResult.ProgressResult.NativeID)

	nativeID := createResult.ProgressResult.NativeID
	t.Logf("Created pool: %s (ID: %s)", name, nativeID)

	t.Cleanup(func() { deletePool(ctx, prov.Client, nativeID) })

	// Read
	readResult, err := prov.Read(ctx, &resource.ReadRequest{
		NativeID:     nativeID,
		ResourceType: ResourceTypeInstancePool,
	})
	require.NoError(t, err)
	assert.Equal(t, ResourceTypeInstancePool, readResult.ResourceType)

	var readProps instancePoolProps
	require.NoError(t, json.Unmarshal([]byte(readResult.Properties), &readProps))
	assert.Equal(t, name, readProps.InstancePoolName)
	assert.Equal(t, "i3.xlarge", readProps.NodeTypeId)
	assert.Equal(t, nativeID, readProps.InstancePoolId)

	// Delete
	deleteResult, err := prov.Delete(ctx, &resource.DeleteRequest{
		NativeID: nativeID,
	})
	require.NoError(t, err)
	assert.Equal(t, resource.OperationStatusSuccess, deleteResult.ProgressResult.OperationStatus)

	// Verify gone
	_, err = prov.Read(ctx, &resource.ReadRequest{
		NativeID:     nativeID,
		ResourceType: ResourceTypeInstancePool,
	})
	assert.Error(t, err)
}

func TestInstancePool_Update(t *testing.T) {
	ctx := context.Background()
	prov := newTestInstancePool(t)
	name := testPoolName("update")

	// Create
	props, _ := json.Marshal(instancePoolProps{
		InstancePoolName: name,
		NodeTypeId:       "i3.xlarge",
		MaxCapacity:      intPtr(2),
	})

	createResult, err := prov.Create(ctx, &resource.CreateRequest{
		ResourceType: ResourceTypeInstancePool,
		Label:        "test-pool",
		Properties:   props,
	})
	require.NoError(t, err)
	nativeID := createResult.ProgressResult.NativeID
	t.Cleanup(func() { deletePool(ctx, prov.Client, nativeID) })

	// Update
	updatedName := name + "-updated"
	desiredProps, _ := json.Marshal(instancePoolProps{
		InstancePoolName: updatedName,
		NodeTypeId:       "i3.xlarge",
		MaxCapacity:      intPtr(4),
	})

	updateResult, err := prov.Update(ctx, &resource.UpdateRequest{
		NativeID:          nativeID,
		ResourceType:      ResourceTypeInstancePool,
		DesiredProperties: desiredProps,
	})
	require.NoError(t, err)
	assert.Equal(t, resource.OperationStatusSuccess, updateResult.ProgressResult.OperationStatus)

	// Verify update
	readResult, err := prov.Read(ctx, &resource.ReadRequest{
		NativeID:     nativeID,
		ResourceType: ResourceTypeInstancePool,
	})
	require.NoError(t, err)

	var readProps instancePoolProps
	require.NoError(t, json.Unmarshal([]byte(readResult.Properties), &readProps))
	assert.Equal(t, updatedName, readProps.InstancePoolName)
	assert.Equal(t, 4, *readProps.MaxCapacity)
}

func TestInstancePool_List(t *testing.T) {
	ctx := context.Background()
	prov := newTestInstancePool(t)
	name := testPoolName("list")

	// Create a pool so we have at least one
	props, _ := json.Marshal(instancePoolProps{
		InstancePoolName: name,
		NodeTypeId:       "i3.xlarge",
		MaxCapacity:      intPtr(1),
	})

	createResult, err := prov.Create(ctx, &resource.CreateRequest{
		ResourceType: ResourceTypeInstancePool,
		Label:        "test-pool",
		Properties:   props,
	})
	require.NoError(t, err)
	nativeID := createResult.ProgressResult.NativeID
	t.Cleanup(func() { deletePool(ctx, prov.Client, nativeID) })

	// List
	listResult, err := prov.List(ctx, &resource.ListRequest{
		ResourceType: ResourceTypeInstancePool,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, listResult.NativeIDs)

	// Verify our pool is in the list
	found := false
	for _, id := range listResult.NativeIDs {
		if id == nativeID {
			found = true
			break
		}
	}
	assert.True(t, found, "Created pool %s should appear in List results", nativeID)
	t.Logf("List returned %d pools", len(listResult.NativeIDs))
}

func TestInstancePool_DeleteAlreadyDeleted(t *testing.T) {
	ctx := context.Background()
	prov := newTestInstancePool(t)
	name := testPoolName("delete-idem")

	// Create and then delete a pool
	props, _ := json.Marshal(instancePoolProps{
		InstancePoolName: name,
		NodeTypeId:       "i3.xlarge",
		MaxCapacity:      intPtr(1),
	})

	createResult, err := prov.Create(ctx, &resource.CreateRequest{
		ResourceType: ResourceTypeInstancePool,
		Label:        "test-pool",
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

func intPtr(v int) *int { return &v }
