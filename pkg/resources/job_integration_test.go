// © 2025 Platform Engineering Labs Inc.
//
// SPDX-License-Identifier: FSL-1.1-ALv2

//go:build integration

package resources

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/platform-engineering-labs/formae/pkg/plugin/resource"
)

func newTestJob(t *testing.T) *Job {
	t.Helper()
	return &Job{Client: newTestClient(t)}
}

func testJobName(suffix string) string {
	return testPoolName("job-" + suffix) // reuse timestamp helper
}

func deleteJob(ctx context.Context, j *Job, nativeID string) {
	_, _ = j.Delete(ctx, &resource.DeleteRequest{NativeID: nativeID})
}

func TestJob_CreateReadDeleteLifecycle(t *testing.T) {
	ctx := context.Background()
	prov := newTestJob(t)
	name := testJobName("lifecycle")

	props, _ := json.Marshal(jobProps{
		Name:              name,
		MaxConcurrentRuns: intPtr(1),
		Tags:              map[string]string{"env": "test"},
	})

	// Create
	createResult, err := prov.Create(ctx, &resource.CreateRequest{
		ResourceType: ResourceTypeJob,
		Label:        "test-job",
		Properties:   props,
	})
	require.NoError(t, err)
	require.NotNil(t, createResult.ProgressResult)
	assert.Equal(t, resource.OperationStatusSuccess, createResult.ProgressResult.OperationStatus)
	assert.NotEmpty(t, createResult.ProgressResult.NativeID)

	nativeID := createResult.ProgressResult.NativeID
	t.Logf("Created job: %s (ID: %s)", name, nativeID)
	t.Cleanup(func() { deleteJob(ctx, prov, nativeID) })

	// Read
	readResult, err := prov.Read(ctx, &resource.ReadRequest{
		NativeID:     nativeID,
		ResourceType: ResourceTypeJob,
	})
	require.NoError(t, err)
	assert.Equal(t, ResourceTypeJob, readResult.ResourceType)

	var readProps jobProps
	require.NoError(t, json.Unmarshal([]byte(readResult.Properties), &readProps))
	assert.Equal(t, name, readProps.Name)
	assert.Equal(t, 1, *readProps.MaxConcurrentRuns)

	// Delete
	deleteResult, err := prov.Delete(ctx, &resource.DeleteRequest{NativeID: nativeID})
	require.NoError(t, err)
	assert.Equal(t, resource.OperationStatusSuccess, deleteResult.ProgressResult.OperationStatus)

	// Verify gone
	_, err = prov.Read(ctx, &resource.ReadRequest{
		NativeID:     nativeID,
		ResourceType: ResourceTypeJob,
	})
	assert.Error(t, err)
}

func TestJob_Update(t *testing.T) {
	ctx := context.Background()
	prov := newTestJob(t)
	name := testJobName("update")

	props, _ := json.Marshal(jobProps{
		Name:              name,
		MaxConcurrentRuns: intPtr(1),
	})

	createResult, err := prov.Create(ctx, &resource.CreateRequest{
		ResourceType: ResourceTypeJob,
		Label:        "test-job",
		Properties:   props,
	})
	require.NoError(t, err)
	nativeID := createResult.ProgressResult.NativeID
	t.Cleanup(func() { deleteJob(ctx, prov, nativeID) })

	// Update
	updatedName := name + "-updated"
	desiredProps, _ := json.Marshal(jobProps{
		Name:              updatedName,
		MaxConcurrentRuns: intPtr(3),
	})

	updateResult, err := prov.Update(ctx, &resource.UpdateRequest{
		NativeID:          nativeID,
		ResourceType:      ResourceTypeJob,
		DesiredProperties: desiredProps,
	})
	require.NoError(t, err)
	assert.Equal(t, resource.OperationStatusSuccess, updateResult.ProgressResult.OperationStatus)

	// Verify update
	readResult, err := prov.Read(ctx, &resource.ReadRequest{
		NativeID:     nativeID,
		ResourceType: ResourceTypeJob,
	})
	require.NoError(t, err)

	var readProps jobProps
	require.NoError(t, json.Unmarshal([]byte(readResult.Properties), &readProps))
	assert.Equal(t, updatedName, readProps.Name)
	assert.Equal(t, 3, *readProps.MaxConcurrentRuns)
}

func TestJob_WithSchedule(t *testing.T) {
	ctx := context.Background()
	prov := newTestJob(t)
	name := testJobName("schedule")

	props, _ := json.Marshal(jobProps{
		Name:              name,
		MaxConcurrentRuns: intPtr(1),
		Schedule: &jobSchedule{
			QuartzCronExpression: "0 0 8 * * ?",
			TimezoneId:           "UTC",
			PauseStatus:          "PAUSED",
		},
	})

	createResult, err := prov.Create(ctx, &resource.CreateRequest{
		ResourceType: ResourceTypeJob,
		Label:        "test-job",
		Properties:   props,
	})
	require.NoError(t, err)
	nativeID := createResult.ProgressResult.NativeID
	t.Cleanup(func() { deleteJob(ctx, prov, nativeID) })

	// Verify schedule was set
	readResult, err := prov.Read(ctx, &resource.ReadRequest{
		NativeID:     nativeID,
		ResourceType: ResourceTypeJob,
	})
	require.NoError(t, err)

	var readProps jobProps
	require.NoError(t, json.Unmarshal([]byte(readResult.Properties), &readProps))
	require.NotNil(t, readProps.Schedule)
	assert.Equal(t, "0 0 8 * * ?", readProps.Schedule.QuartzCronExpression)
	assert.Equal(t, "UTC", readProps.Schedule.TimezoneId)
	assert.Equal(t, "PAUSED", readProps.Schedule.PauseStatus)
}

func TestJob_List(t *testing.T) {
	ctx := context.Background()
	prov := newTestJob(t)
	name := testJobName("list")

	props, _ := json.Marshal(jobProps{
		Name:              name,
		MaxConcurrentRuns: intPtr(1),
	})

	createResult, err := prov.Create(ctx, &resource.CreateRequest{
		ResourceType: ResourceTypeJob,
		Label:        "test-job",
		Properties:   props,
	})
	require.NoError(t, err)
	nativeID := createResult.ProgressResult.NativeID
	t.Cleanup(func() { deleteJob(ctx, prov, nativeID) })

	listResult, err := prov.List(ctx, &resource.ListRequest{
		ResourceType: ResourceTypeJob,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, listResult.NativeIDs)

	found := false
	for _, id := range listResult.NativeIDs {
		if id == nativeID {
			found = true
			break
		}
	}
	assert.True(t, found, "Created job %s should appear in List results", nativeID)
	t.Logf("List returned %d jobs", len(listResult.NativeIDs))
}

func TestJob_DeleteAlreadyDeleted(t *testing.T) {
	ctx := context.Background()
	prov := newTestJob(t)
	name := testJobName("delete-idem")

	props, _ := json.Marshal(jobProps{
		Name:              name,
		MaxConcurrentRuns: intPtr(1),
	})

	createResult, err := prov.Create(ctx, &resource.CreateRequest{
		ResourceType: ResourceTypeJob,
		Label:        "test-job",
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
