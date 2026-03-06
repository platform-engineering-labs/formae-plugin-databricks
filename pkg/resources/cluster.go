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

const ResourceTypeCluster = "Databricks::Compute::Cluster"

func init() {
	registry.Register(ResourceTypeCluster, func(c *client.Client, cfg *config.Config) prov.Provisioner {
		return &Cluster{Client: c}
	})
}

type Cluster struct {
	Client *client.Client
}

type clusterProps struct {
	ClusterId              string            `json:"clusterId,omitempty"`
	ClusterName            string            `json:"clusterName"`
	SparkVersion           string            `json:"sparkVersion"`
	NodeTypeId             string            `json:"nodeTypeId,omitempty"`
	InstancePoolId         string            `json:"instancePoolId,omitempty"`
	NumWorkers             *int              `json:"numWorkers,omitempty"`
	Autoscale              *autoscaleProps    `json:"autoscale,omitempty"`
	AutoterminationMinutes *int              `json:"autoterminationMinutes,omitempty"`
	DataSecurityMode       string            `json:"dataSecurityMode,omitempty"`
	SparkConf              map[string]string `json:"sparkConf,omitempty"`
	CustomTags             map[string]string `json:"customTags,omitempty"`
}

type autoscaleProps struct {
	MinWorkers int `json:"minWorkers"`
	MaxWorkers int `json:"maxWorkers"`
}

// asyncRequestID is serialized as the RequestID for async status polling.
type asyncRequestID struct {
	OperationType string `json:"operationType"`
	ClusterId     string `json:"clusterId"`
}

func (cl *Cluster) Create(ctx context.Context, request *resource.CreateRequest) (*resource.CreateResult, error) {
	var props clusterProps
	if err := json.Unmarshal(request.Properties, &props); err != nil {
		return nil, fmt.Errorf("failed to parse properties: %w", err)
	}

	req := compute.CreateCluster{
		ClusterName:      props.ClusterName,
		SparkVersion:     props.SparkVersion,
		NodeTypeId:       props.NodeTypeId,
		InstancePoolId:   props.InstancePoolId,
		DataSecurityMode: compute.DataSecurityMode(props.DataSecurityMode),
		SparkConf:        props.SparkConf,
		CustomTags:       props.CustomTags,
	}
	if props.NumWorkers != nil {
		req.NumWorkers = *props.NumWorkers
	}
	if props.Autoscale != nil {
		req.Autoscale = &compute.AutoScale{
			MinWorkers: props.Autoscale.MinWorkers,
			MaxWorkers: props.Autoscale.MaxWorkers,
		}
	}
	if props.AutoterminationMinutes != nil {
		req.AutoterminationMinutes = *props.AutoterminationMinutes
	}

	waiter, err := cl.Client.Workspace.Clusters.Create(ctx, req)
	if err != nil {
		return &resource.CreateResult{
			ProgressResult: &resource.ProgressResult{
				Operation:       resource.OperationCreate,
				OperationStatus: resource.OperationStatusFailure,
				ErrorCode:       mapDatabricksError(err),
			},
		}, fmt.Errorf("failed to create cluster: %w", err)
	}

	reqID, _ := json.Marshal(asyncRequestID{
		OperationType: "create",
		ClusterId:     waiter.ClusterId,
	})

	return &resource.CreateResult{
		ProgressResult: &resource.ProgressResult{
			Operation:       resource.OperationCreate,
			OperationStatus: resource.OperationStatusInProgress,
			RequestID:       string(reqID),
			NativeID:        waiter.ClusterId,
		},
	}, nil
}

func (cl *Cluster) Read(ctx context.Context, request *resource.ReadRequest) (*resource.ReadResult, error) {
	propsJSON, err := cl.readByID(ctx, request.NativeID)
	if err != nil {
		return &resource.ReadResult{
			ErrorCode: mapDatabricksError(err),
		}, fmt.Errorf("failed to read cluster: %w", err)
	}

	return &resource.ReadResult{
		ResourceType: ResourceTypeCluster,
		Properties:   string(propsJSON),
	}, nil
}

func (cl *Cluster) Update(ctx context.Context, request *resource.UpdateRequest) (*resource.UpdateResult, error) {
	var props clusterProps
	if err := json.Unmarshal(request.DesiredProperties, &props); err != nil {
		return nil, fmt.Errorf("failed to parse desired properties: %w", err)
	}

	editReq := compute.EditCluster{
		ClusterId:        request.NativeID,
		ClusterName:      props.ClusterName,
		SparkVersion:     props.SparkVersion,
		NodeTypeId:       props.NodeTypeId,
		InstancePoolId:   props.InstancePoolId,
		DataSecurityMode: compute.DataSecurityMode(props.DataSecurityMode),
		SparkConf:        props.SparkConf,
		CustomTags:       props.CustomTags,
	}
	if props.NumWorkers != nil {
		editReq.NumWorkers = *props.NumWorkers
	}
	if props.Autoscale != nil {
		editReq.Autoscale = &compute.AutoScale{
			MinWorkers: props.Autoscale.MinWorkers,
			MaxWorkers: props.Autoscale.MaxWorkers,
		}
	}
	if props.AutoterminationMinutes != nil {
		editReq.AutoterminationMinutes = *props.AutoterminationMinutes
	}

	_, err := cl.Client.Workspace.Clusters.Edit(ctx, editReq)
	if err != nil {
		return &resource.UpdateResult{
			ProgressResult: &resource.ProgressResult{
				Operation:       resource.OperationUpdate,
				OperationStatus: resource.OperationStatusFailure,
				ErrorCode:       mapDatabricksError(err),
				NativeID:        request.NativeID,
			},
		}, fmt.Errorf("failed to update cluster: %w", err)
	}

	reqID, _ := json.Marshal(asyncRequestID{
		OperationType: "update",
		ClusterId:     request.NativeID,
	})

	return &resource.UpdateResult{
		ProgressResult: &resource.ProgressResult{
			Operation:       resource.OperationUpdate,
			OperationStatus: resource.OperationStatusInProgress,
			RequestID:       string(reqID),
			NativeID:        request.NativeID,
		},
	}, nil
}

func (cl *Cluster) Delete(ctx context.Context, request *resource.DeleteRequest) (*resource.DeleteResult, error) {
	err := cl.Client.Workspace.Clusters.PermanentDeleteByClusterId(ctx, request.NativeID)
	if err != nil && !isDeleteSuccessError(err) {
		return &resource.DeleteResult{
			ProgressResult: &resource.ProgressResult{
				Operation:       resource.OperationDelete,
				OperationStatus: resource.OperationStatusFailure,
				ErrorCode:       mapDatabricksError(err),
				NativeID:        request.NativeID,
			},
		}, fmt.Errorf("failed to delete cluster: %w", err)
	}

	return &resource.DeleteResult{
		ProgressResult: &resource.ProgressResult{
			Operation:       resource.OperationDelete,
			OperationStatus: resource.OperationStatusSuccess,
			NativeID:        request.NativeID,
		},
	}, nil
}

func (cl *Cluster) Status(ctx context.Context, request *resource.StatusRequest) (*resource.StatusResult, error) {
	var reqID asyncRequestID
	if err := json.Unmarshal([]byte(request.RequestID), &reqID); err != nil {
		return &resource.StatusResult{
			ProgressResult: &resource.ProgressResult{
				OperationStatus: resource.OperationStatusFailure,
				ErrorCode:       resource.OperationErrorCodeGeneralServiceException,
			},
		}, fmt.Errorf("failed to parse request ID: %w", err)
	}

	details, err := cl.Client.Workspace.Clusters.GetByClusterId(ctx, reqID.ClusterId)
	if err != nil {
		return &resource.StatusResult{
			ProgressResult: &resource.ProgressResult{
				OperationStatus: resource.OperationStatusFailure,
				ErrorCode:       mapDatabricksError(err),
				NativeID:        reqID.ClusterId,
			},
		}, fmt.Errorf("failed to get cluster status: %w", err)
	}

	operation := resource.OperationCreate
	if reqID.OperationType == "update" {
		operation = resource.OperationUpdate
	}

	switch details.State {
	case compute.StateRunning:
		propsJSON, err := cl.serializeDetails(details)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize cluster: %w", err)
		}
		return &resource.StatusResult{
			ProgressResult: &resource.ProgressResult{
				Operation:          operation,
				OperationStatus:    resource.OperationStatusSuccess,
				NativeID:           reqID.ClusterId,
				ResourceProperties: propsJSON,
			},
		}, nil

	case compute.StatePending, compute.StateResizing, compute.StateRestarting:
		return &resource.StatusResult{
			ProgressResult: &resource.ProgressResult{
				Operation:       operation,
				OperationStatus: resource.OperationStatusInProgress,
				RequestID:       request.RequestID,
				NativeID:        reqID.ClusterId,
				StatusMessage:   fmt.Sprintf("Cluster state: %s", details.State),
			},
		}, nil

	case compute.StateTerminated:
		// For create/update, terminated after auto-termination is still success
		propsJSON, err := cl.serializeDetails(details)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize cluster: %w", err)
		}
		return &resource.StatusResult{
			ProgressResult: &resource.ProgressResult{
				Operation:          operation,
				OperationStatus:    resource.OperationStatusSuccess,
				NativeID:           reqID.ClusterId,
				ResourceProperties: propsJSON,
			},
		}, nil

	default: // ERROR or unknown
		return &resource.StatusResult{
			ProgressResult: &resource.ProgressResult{
				Operation:       operation,
				OperationStatus: resource.OperationStatusFailure,
				ErrorCode:       resource.OperationErrorCodeGeneralServiceException,
				NativeID:        reqID.ClusterId,
				StatusMessage:   fmt.Sprintf("Cluster state: %s", details.State),
			},
		}, nil
	}
}

func (cl *Cluster) List(ctx context.Context, _ *resource.ListRequest) (*resource.ListResult, error) {
	clusters, err := cl.Client.Workspace.Clusters.ListAll(ctx, compute.ListClustersRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to list clusters: %w", err)
	}

	nativeIDs := make([]string, 0, len(clusters))
	for _, c := range clusters {
		nativeIDs = append(nativeIDs, c.ClusterId)
	}

	return &resource.ListResult{
		NativeIDs: nativeIDs,
	}, nil
}

func (cl *Cluster) readByID(ctx context.Context, clusterID string) (json.RawMessage, error) {
	details, err := cl.Client.Workspace.Clusters.GetByClusterId(ctx, clusterID)
	if err != nil {
		return nil, err
	}
	return cl.serializeDetails(details)
}

func (cl *Cluster) serializeDetails(details *compute.ClusterDetails) (json.RawMessage, error) {
	props := clusterProps{
		ClusterId:        details.ClusterId,
		ClusterName:      details.ClusterName,
		SparkVersion:     details.SparkVersion,
		NodeTypeId:       details.NodeTypeId,
		InstancePoolId:   details.InstancePoolId,
		DataSecurityMode: string(details.DataSecurityMode),
		SparkConf:        details.SparkConf,
		CustomTags:       details.CustomTags,
	}
	if details.NumWorkers > 0 {
		v := details.NumWorkers
		props.NumWorkers = &v
	}
	if details.Autoscale != nil {
		props.Autoscale = &autoscaleProps{
			MinWorkers: details.Autoscale.MinWorkers,
			MaxWorkers: details.Autoscale.MaxWorkers,
		}
	}
	if details.AutoterminationMinutes > 0 {
		v := details.AutoterminationMinutes
		props.AutoterminationMinutes = &v
	}
	return json.Marshal(props)
}
