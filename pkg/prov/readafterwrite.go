// © 2025 Platform Engineering Labs Inc.
//
// SPDX-License-Identifier: FSL-1.1-ALv2

package prov

import (
	"context"
	"encoding/json"

	"github.com/platform-engineering-labs/formae/pkg/plugin/resource"
)

// readAfterWrite is a decorator that wraps a Provisioner and automatically
// calls Read() after a successful synchronous Create or Update to fill
// ResourceProperties with the complete state from the API. This prevents
// formae's validateRequiredFields from dropping resources due to missing
// schema-required fields the SDK didn't return on the write call.
//
// For async operations (OperationStatusInProgress), the decorator is a no-op —
// properties come from Status() polling instead.
type readAfterWrite struct {
	inner Provisioner
}

// WithReadAfterWrite wraps the given Provisioner with read-after-write behavior.
// Used by the registry so every resource type gets the behavior uniformly.
func WithReadAfterWrite(p Provisioner) Provisioner {
	return &readAfterWrite{inner: p}
}

func (w *readAfterWrite) Create(ctx context.Context, request *resource.CreateRequest) (*resource.CreateResult, error) {
	result, err := w.inner.Create(ctx, request)
	if err != nil || result == nil {
		return result, err
	}
	w.fillPropertiesAfterWrite(ctx, request.ResourceType, request.TargetConfig, result.ProgressResult)
	return result, nil
}

func (w *readAfterWrite) Update(ctx context.Context, request *resource.UpdateRequest) (*resource.UpdateResult, error) {
	result, err := w.inner.Update(ctx, request)
	if err != nil || result == nil {
		return result, err
	}
	w.fillPropertiesAfterWrite(ctx, request.ResourceType, request.TargetConfig, result.ProgressResult)
	return result, nil
}

func (w *readAfterWrite) Delete(ctx context.Context, request *resource.DeleteRequest) (*resource.DeleteResult, error) {
	return w.inner.Delete(ctx, request)
}

func (w *readAfterWrite) Read(ctx context.Context, request *resource.ReadRequest) (*resource.ReadResult, error) {
	return w.inner.Read(ctx, request)
}

func (w *readAfterWrite) Status(ctx context.Context, request *resource.StatusRequest) (*resource.StatusResult, error) {
	return w.inner.Status(ctx, request)
}

func (w *readAfterWrite) List(ctx context.Context, request *resource.ListRequest) (*resource.ListResult, error) {
	return w.inner.List(ctx, request)
}

// fillPropertiesAfterWrite reads the resource by NativeID after a successful
// synchronous write and stores the full property set on the ProgressResult.
// Skipped when the write is async (InProgress), failed, or already populated.
func (w *readAfterWrite) fillPropertiesAfterWrite(ctx context.Context, resourceType string, targetConfig json.RawMessage, pr *resource.ProgressResult) {
	if pr == nil {
		return
	}
	if pr.OperationStatus != resource.OperationStatusSuccess {
		return
	}
	if pr.NativeID == "" {
		return
	}
	if len(pr.ResourceProperties) > 0 {
		return
	}
	readResp, err := w.inner.Read(ctx, &resource.ReadRequest{
		NativeID:     pr.NativeID,
		ResourceType: resourceType,
		TargetConfig: targetConfig,
	})
	if err != nil || readResp == nil || readResp.ErrorCode != "" {
		return
	}
	pr.ResourceProperties = json.RawMessage(readResp.Properties)
}
