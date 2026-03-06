// © 2025 Platform Engineering Labs Inc.
//
// SPDX-License-Identifier: FSL-1.1-ALv2

package resources

import (
	"errors"
	"strconv"
	"strings"

	"github.com/databricks/databricks-sdk-go/apierr"
	"github.com/platform-engineering-labs/formae/pkg/plugin/resource"
)

// mapDatabricksError maps Databricks API errors to formae OperationErrorCode.
func mapDatabricksError(err error) resource.OperationErrorCode {
	if err == nil {
		return ""
	}

	switch {
	case errors.Is(err, apierr.ErrNotFound):
		return resource.OperationErrorCodeNotFound
	case errors.Is(err, apierr.ErrPermissionDenied):
		return resource.OperationErrorCodeAccessDenied
	case errors.Is(err, apierr.ErrUnauthenticated):
		return resource.OperationErrorCodeInvalidCredentials
	case errors.Is(err, apierr.ErrResourceConflict):
		return resource.OperationErrorCodeResourceConflict
	case errors.Is(err, apierr.ErrTooManyRequests):
		return resource.OperationErrorCodeThrottling
	case errors.Is(err, apierr.ErrBadRequest):
		return resource.OperationErrorCodeInvalidRequest
	case errors.Is(err, apierr.ErrInternalError):
		return resource.OperationErrorCodeServiceInternalError
	default:
		return resource.OperationErrorCodeGeneralServiceException
	}
}

// isDeleteSuccessError returns true if the error indicates the resource is already deleted.
// Databricks returns RESOURCE_DOES_NOT_EXIST (404) for some resources and
// INVALID_PARAMETER_VALUE (400) with "does not exist" / "Can't find" for others.
func isDeleteSuccessError(err error) bool {
	if apierr.IsMissing(err) {
		return true
	}
	if errors.Is(err, apierr.ErrInvalidParameterValue) {
		msg := err.Error()
		return strings.Contains(msg, "does not exist") || strings.Contains(msg, "Can't find")
	}
	return false
}

// int64ToNativeID converts an int64 ID to a string NativeID.
func int64ToNativeID(id int64) string {
	return strconv.FormatInt(id, 10)
}

// nativeIDToInt64 converts a string NativeID to an int64 ID.
func nativeIDToInt64(nativeID string) (int64, error) {
	return strconv.ParseInt(nativeID, 10, 64)
}
