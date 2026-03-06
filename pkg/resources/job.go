// © 2025 Platform Engineering Labs Inc.
//
// SPDX-License-Identifier: FSL-1.1-ALv2

package resources

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/databricks/databricks-sdk-go/service/jobs"

	"github.com/platform-engineering-labs/formae-plugin-databricks/pkg/client"
	"github.com/platform-engineering-labs/formae-plugin-databricks/pkg/config"
	"github.com/platform-engineering-labs/formae-plugin-databricks/pkg/prov"
	"github.com/platform-engineering-labs/formae-plugin-databricks/pkg/registry"
	"github.com/platform-engineering-labs/formae/pkg/plugin/resource"
)

const ResourceTypeJob = "Databricks::Jobs::Job"

func init() {
	registry.Register(ResourceTypeJob, func(c *client.Client, cfg *config.Config) prov.Provisioner {
		return &Job{Client: c}
	})
}

type Job struct {
	Client *client.Client
}

type jobProps struct {
	JobId             *int64              `json:"jobId,omitempty"`
	Name              string              `json:"name"`
	Tasks             []jobTask           `json:"tasks,omitempty"`
	Schedule          *jobSchedule        `json:"schedule,omitempty"`
	MaxConcurrentRuns *int                `json:"maxConcurrentRuns,omitempty"`
	Tags              map[string]string   `json:"tags,omitempty"`
}

type jobTask struct {
	TaskKey           string       `json:"taskKey"`
	Description       string       `json:"description,omitempty"`
	ExistingClusterId string       `json:"existingClusterId,omitempty"`
	NotebookTask      *notebookTask `json:"notebookTask,omitempty"`
}

type notebookTask struct {
	NotebookPath string            `json:"notebookPath"`
	BaseParameters map[string]string `json:"baseParameters,omitempty"`
}

type jobSchedule struct {
	QuartzCronExpression string `json:"quartzCronExpression"`
	TimezoneId           string `json:"timezoneId"`
	PauseStatus          string `json:"pauseStatus,omitempty"`
}

func (j *Job) Create(ctx context.Context, request *resource.CreateRequest) (*resource.CreateResult, error) {
	var props jobProps
	if err := json.Unmarshal(request.Properties, &props); err != nil {
		return nil, fmt.Errorf("failed to parse properties: %w", err)
	}

	createReq := jobs.CreateJob{
		Name: props.Name,
		Tags: props.Tags,
	}
	if props.MaxConcurrentRuns != nil {
		createReq.MaxConcurrentRuns = *props.MaxConcurrentRuns
	}
	if props.Schedule != nil {
		createReq.Schedule = &jobs.CronSchedule{
			QuartzCronExpression: props.Schedule.QuartzCronExpression,
			TimezoneId:           props.Schedule.TimezoneId,
			PauseStatus:          jobs.PauseStatus(props.Schedule.PauseStatus),
		}
	}
	createReq.Tasks = toSDKTasks(props.Tasks)

	resp, err := j.Client.Workspace.Jobs.Create(ctx, createReq)
	if err != nil {
		return &resource.CreateResult{
			ProgressResult: &resource.ProgressResult{
				Operation:       resource.OperationCreate,
				OperationStatus: resource.OperationStatusFailure,
				ErrorCode:       mapDatabricksError(err),
			},
		}, fmt.Errorf("failed to create job: %w", err)
	}

	nativeID := int64ToNativeID(resp.JobId)
	propsJSON, err := j.readByID(ctx, resp.JobId)
	if err != nil {
		return nil, fmt.Errorf("failed to read after create: %w", err)
	}

	return &resource.CreateResult{
		ProgressResult: &resource.ProgressResult{
			Operation:          resource.OperationCreate,
			OperationStatus:    resource.OperationStatusSuccess,
			NativeID:           nativeID,
			ResourceProperties: propsJSON,
		},
	}, nil
}

func (j *Job) Read(ctx context.Context, request *resource.ReadRequest) (*resource.ReadResult, error) {
	jobID, err := nativeIDToInt64(request.NativeID)
	if err != nil {
		return nil, fmt.Errorf("invalid native ID %q: %w", request.NativeID, err)
	}

	propsJSON, err := j.readByID(ctx, jobID)
	if err != nil {
		return &resource.ReadResult{
			ErrorCode: mapDatabricksError(err),
		}, fmt.Errorf("failed to read job: %w", err)
	}

	return &resource.ReadResult{
		ResourceType: ResourceTypeJob,
		Properties:   string(propsJSON),
	}, nil
}

func (j *Job) Update(ctx context.Context, request *resource.UpdateRequest) (*resource.UpdateResult, error) {
	jobID, err := nativeIDToInt64(request.NativeID)
	if err != nil {
		return nil, fmt.Errorf("invalid native ID %q: %w", request.NativeID, err)
	}

	var props jobProps
	if err := json.Unmarshal(request.DesiredProperties, &props); err != nil {
		return nil, fmt.Errorf("failed to parse desired properties: %w", err)
	}

	settings := &jobs.JobSettings{
		Name: props.Name,
		Tags: props.Tags,
	}
	if props.MaxConcurrentRuns != nil {
		settings.MaxConcurrentRuns = *props.MaxConcurrentRuns
	}
	if props.Schedule != nil {
		settings.Schedule = &jobs.CronSchedule{
			QuartzCronExpression: props.Schedule.QuartzCronExpression,
			TimezoneId:           props.Schedule.TimezoneId,
			PauseStatus:          jobs.PauseStatus(props.Schedule.PauseStatus),
		}
	}
	settings.Tasks = toSDKTasks(props.Tasks)

	if err := j.Client.Workspace.Jobs.Update(ctx, jobs.UpdateJob{
		JobId:       jobID,
		NewSettings: settings,
	}); err != nil {
		return &resource.UpdateResult{
			ProgressResult: &resource.ProgressResult{
				Operation:       resource.OperationUpdate,
				OperationStatus: resource.OperationStatusFailure,
				ErrorCode:       mapDatabricksError(err),
				NativeID:        request.NativeID,
			},
		}, fmt.Errorf("failed to update job: %w", err)
	}

	propsJSON, err := j.readByID(ctx, jobID)
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

func (j *Job) Delete(ctx context.Context, request *resource.DeleteRequest) (*resource.DeleteResult, error) {
	jobID, err := nativeIDToInt64(request.NativeID)
	if err != nil {
		return nil, fmt.Errorf("invalid native ID %q: %w", request.NativeID, err)
	}

	err = j.Client.Workspace.Jobs.DeleteByJobId(ctx, jobID)
	if err != nil && !isDeleteSuccessError(err) {
		return &resource.DeleteResult{
			ProgressResult: &resource.ProgressResult{
				Operation:       resource.OperationDelete,
				OperationStatus: resource.OperationStatusFailure,
				ErrorCode:       mapDatabricksError(err),
				NativeID:        request.NativeID,
			},
		}, fmt.Errorf("failed to delete job: %w", err)
	}

	return &resource.DeleteResult{
		ProgressResult: &resource.ProgressResult{
			Operation:       resource.OperationDelete,
			OperationStatus: resource.OperationStatusSuccess,
			NativeID:        request.NativeID,
		},
	}, nil
}

func (j *Job) Status(_ context.Context, request *resource.StatusRequest) (*resource.StatusResult, error) {
	return &resource.StatusResult{
		ProgressResult: &resource.ProgressResult{
			Operation:       resource.OperationCheckStatus,
			OperationStatus: resource.OperationStatusSuccess,
			NativeID:        request.NativeID,
		},
	}, nil
}

func (j *Job) List(ctx context.Context, _ *resource.ListRequest) (*resource.ListResult, error) {
	iter := j.Client.Workspace.Jobs.List(ctx, jobs.ListJobsRequest{})

	var nativeIDs []string
	for iter.HasNext(ctx) {
		job, err := iter.Next(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list jobs: %w", err)
		}
		nativeIDs = append(nativeIDs, int64ToNativeID(job.JobId))
	}

	return &resource.ListResult{
		NativeIDs: nativeIDs,
	}, nil
}

func (j *Job) readByID(ctx context.Context, jobID int64) (json.RawMessage, error) {
	job, err := j.Client.Workspace.Jobs.GetByJobId(ctx, jobID)
	if err != nil {
		return nil, err
	}

	props := jobProps{
		JobId: &job.JobId,
		Name:  job.Settings.Name,
		Tags:  job.Settings.Tags,
	}
	if job.Settings.MaxConcurrentRuns > 0 {
		v := job.Settings.MaxConcurrentRuns
		props.MaxConcurrentRuns = &v
	}
	if job.Settings.Schedule != nil {
		props.Schedule = &jobSchedule{
			QuartzCronExpression: job.Settings.Schedule.QuartzCronExpression,
			TimezoneId:           job.Settings.Schedule.TimezoneId,
			PauseStatus:          string(job.Settings.Schedule.PauseStatus),
		}
	}
	props.Tasks = fromSDKTasks(job.Settings.Tasks)

	return json.Marshal(props)
}

func toSDKTasks(tasks []jobTask) []jobs.Task {
	if len(tasks) == 0 {
		return nil
	}
	sdkTasks := make([]jobs.Task, len(tasks))
	for i, t := range tasks {
		sdkTasks[i] = jobs.Task{
			TaskKey:           t.TaskKey,
			Description:       t.Description,
			ExistingClusterId: t.ExistingClusterId,
		}
		if t.NotebookTask != nil {
			sdkTasks[i].NotebookTask = &jobs.NotebookTask{
				NotebookPath:   t.NotebookTask.NotebookPath,
				BaseParameters: t.NotebookTask.BaseParameters,
			}
		}
	}
	return sdkTasks
}

func fromSDKTasks(tasks []jobs.Task) []jobTask {
	if len(tasks) == 0 {
		return nil
	}
	result := make([]jobTask, len(tasks))
	for i, t := range tasks {
		result[i] = jobTask{
			TaskKey:           t.TaskKey,
			Description:       t.Description,
			ExistingClusterId: t.ExistingClusterId,
		}
		if t.NotebookTask != nil {
			result[i].NotebookTask = &notebookTask{
				NotebookPath:   t.NotebookTask.NotebookPath,
				BaseParameters: t.NotebookTask.BaseParameters,
			}
		}
	}
	return result
}
