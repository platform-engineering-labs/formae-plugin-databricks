// © 2025 Platform Engineering Labs Inc.
//
// SPDX-License-Identifier: FSL-1.1-ALv2

package resources

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/databricks/databricks-sdk-go/service/compute"

	"github.com/platform-engineering-labs/formae-plugin-databricks/pkg/client"
	"github.com/platform-engineering-labs/formae-plugin-databricks/pkg/config"
	"github.com/platform-engineering-labs/formae-plugin-databricks/pkg/prov"
	"github.com/platform-engineering-labs/formae-plugin-databricks/pkg/registry"
	"github.com/platform-engineering-labs/formae/pkg/plugin/resource"
)

const ResourceTypeInstancePool = "Databricks::Compute::InstancePool"

func init() {
	registry.Register(ResourceTypeInstancePool, func(c *client.Client, cfg *config.Config) prov.Provisioner {
		return &InstancePool{Client: c}
	})
}

type InstancePool struct {
	Client *client.Client
}

type instancePoolProps struct {
	InstancePoolName                   string   `json:"instancePoolName"`
	NodeTypeId                         string   `json:"nodeTypeId"`
	MinIdleInstances                   *int     `json:"minIdleInstances,omitempty"`
	MaxCapacity                        *int     `json:"maxCapacity,omitempty"`
	IdleInstanceAutoterminationMinutes *int     `json:"idleInstanceAutoterminationMinutes,omitempty"`
	PreloadedSparkVersions             []string `json:"preloadedSparkVersions,omitempty"`
	InstancePoolId                     string   `json:"instancePoolId,omitempty"`
}

func (ip *InstancePool) Create(ctx context.Context, request *resource.CreateRequest) (*resource.CreateResult, error) {
	var props instancePoolProps
	if err := json.Unmarshal(request.Properties, &props); err != nil {
		return nil, fmt.Errorf("failed to parse properties: %w", err)
	}

	req := compute.CreateInstancePool{
		InstancePoolName: props.InstancePoolName,
		NodeTypeId:       props.NodeTypeId,
	}
	if props.MinIdleInstances != nil {
		req.MinIdleInstances = *props.MinIdleInstances
	}
	if props.MaxCapacity != nil {
		req.MaxCapacity = *props.MaxCapacity
	}
	if props.IdleInstanceAutoterminationMinutes != nil {
		req.IdleInstanceAutoterminationMinutes = *props.IdleInstanceAutoterminationMinutes
	}
	if props.PreloadedSparkVersions != nil {
		req.PreloadedSparkVersions = props.PreloadedSparkVersions
	}

	resp, err := ip.Client.Workspace.InstancePools.Create(ctx, req)
	if err != nil {
		return &resource.CreateResult{
			ProgressResult: &resource.ProgressResult{
				Operation:       resource.OperationCreate,
				OperationStatus: resource.OperationStatusFailure,
				ErrorCode:       mapDatabricksError(err),
			},
		}, fmt.Errorf("failed to create instance pool: %w", err)
	}

	readResult, err := ip.readByID(ctx, resp.InstancePoolId)
	if err != nil {
		return nil, fmt.Errorf("failed to read after create: %w", err)
	}

	return &resource.CreateResult{
		ProgressResult: &resource.ProgressResult{
			Operation:          resource.OperationCreate,
			OperationStatus:    resource.OperationStatusSuccess,
			NativeID:           resp.InstancePoolId,
			ResourceProperties: readResult,
		},
	}, nil
}

func (ip *InstancePool) Read(ctx context.Context, request *resource.ReadRequest) (*resource.ReadResult, error) {
	propsJSON, err := ip.readByID(ctx, request.NativeID)
	if err != nil {
		return &resource.ReadResult{
			ErrorCode: mapDatabricksError(err),
		}, fmt.Errorf("failed to read instance pool: %w", err)
	}

	return &resource.ReadResult{
		ResourceType: ResourceTypeInstancePool,
		Properties:   string(propsJSON),
	}, nil
}

func (ip *InstancePool) Update(ctx context.Context, request *resource.UpdateRequest) (*resource.UpdateResult, error) {
	var props instancePoolProps
	if err := json.Unmarshal(request.DesiredProperties, &props); err != nil {
		return nil, fmt.Errorf("failed to parse desired properties: %w", err)
	}

	editReq := compute.EditInstancePool{
		InstancePoolId:   request.NativeID,
		InstancePoolName: props.InstancePoolName,
		NodeTypeId:       props.NodeTypeId,
	}
	if props.MinIdleInstances != nil {
		editReq.MinIdleInstances = *props.MinIdleInstances
	}
	if props.MaxCapacity != nil {
		editReq.MaxCapacity = *props.MaxCapacity
	}
	if props.IdleInstanceAutoterminationMinutes != nil {
		editReq.IdleInstanceAutoterminationMinutes = *props.IdleInstanceAutoterminationMinutes
	}

	if err := ip.Client.Workspace.InstancePools.Edit(ctx, editReq); err != nil {
		return &resource.UpdateResult{
			ProgressResult: &resource.ProgressResult{
				Operation:       resource.OperationUpdate,
				OperationStatus: resource.OperationStatusFailure,
				ErrorCode:       mapDatabricksError(err),
				NativeID:        request.NativeID,
			},
		}, fmt.Errorf("failed to update instance pool: %w", err)
	}

	propsJSON, err := ip.readByID(ctx, request.NativeID)
	if err != nil {
		return nil, fmt.Errorf("failed to read after update: %w", err)
	}

	return &resource.UpdateResult{
		ProgressResult: &resource.ProgressResult{
			Operation:          resource.OperationUpdate,
			OperationStatus:    resource.OperationStatusSuccess,
			NativeID:           request.NativeID,
			ResourceProperties: propsJSON,
		},
	}, nil
}

func (ip *InstancePool) Delete(ctx context.Context, request *resource.DeleteRequest) (*resource.DeleteResult, error) {
	err := ip.Client.Workspace.InstancePools.Delete(ctx, compute.DeleteInstancePool{
		InstancePoolId: request.NativeID,
	})
	if err != nil && !isDeleteSuccessError(err) {
		return &resource.DeleteResult{
			ProgressResult: &resource.ProgressResult{
				Operation:       resource.OperationDelete,
				OperationStatus: resource.OperationStatusFailure,
				ErrorCode:       mapDatabricksError(err),
				NativeID:        request.NativeID,
			},
		}, fmt.Errorf("failed to delete instance pool: %w", err)
	}

	return &resource.DeleteResult{
		ProgressResult: &resource.ProgressResult{
			Operation:       resource.OperationDelete,
			OperationStatus: resource.OperationStatusSuccess,
			NativeID:        request.NativeID,
		},
	}, nil
}

func (ip *InstancePool) Status(_ context.Context, request *resource.StatusRequest) (*resource.StatusResult, error) {
	return &resource.StatusResult{
		ProgressResult: &resource.ProgressResult{
			Operation:       resource.OperationCheckStatus,
			OperationStatus: resource.OperationStatusSuccess,
			NativeID:        request.NativeID,
		},
	}, nil
}

func (ip *InstancePool) List(ctx context.Context, _ *resource.ListRequest) (*resource.ListResult, error) {
	pools, err := ip.Client.Workspace.InstancePools.ListAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list instance pools: %w", err)
	}

	nativeIDs := make([]string, 0, len(pools))
	for _, pool := range pools {
		nativeIDs = append(nativeIDs, pool.InstancePoolId)
	}

	return &resource.ListResult{
		NativeIDs: nativeIDs,
	}, nil
}

func (ip *InstancePool) readByID(ctx context.Context, poolID string) (json.RawMessage, error) {
	pool, err := ip.Client.Workspace.InstancePools.GetByInstancePoolId(ctx, poolID)
	if err != nil {
		return nil, err
	}

	props := instancePoolProps{
		InstancePoolId:   pool.InstancePoolId,
		InstancePoolName: pool.InstancePoolName,
		NodeTypeId:       pool.NodeTypeId,
	}
	if pool.MinIdleInstances > 0 {
		v := int(pool.MinIdleInstances)
		props.MinIdleInstances = &v
	}
	if pool.MaxCapacity > 0 {
		v := int(pool.MaxCapacity)
		props.MaxCapacity = &v
	}
	if pool.IdleInstanceAutoterminationMinutes > 0 {
		v := int(pool.IdleInstanceAutoterminationMinutes)
		props.IdleInstanceAutoterminationMinutes = &v
	}
	if len(pool.PreloadedSparkVersions) > 0 {
		props.PreloadedSparkVersions = pool.PreloadedSparkVersions
	}

	return json.Marshal(props)
}
