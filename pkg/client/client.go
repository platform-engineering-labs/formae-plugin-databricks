// © 2025 Platform Engineering Labs Inc.
//
// SPDX-License-Identifier: FSL-1.1-ALv2

package client

import (
	"github.com/databricks/databricks-sdk-go"

	dbconfig "github.com/platform-engineering-labs/formae-plugin-databricks/pkg/config"
)

// Client wraps the Databricks WorkspaceClient.
type Client struct {
	Workspace *databricks.WorkspaceClient
}

// NewClient creates a new Databricks client from plugin config.
// Uses the SDK's default credential chain (PAT, CLI profile, Azure CLI,
// OAuth, etc.) — same behavior as the Databricks CLI itself.
func NewClient(cfg *dbconfig.Config) (*Client, error) {
	w, err := databricks.NewWorkspaceClient(&databricks.Config{
		Host:  cfg.Host,
		Token: cfg.Token,
	})
	if err != nil {
		return nil, err
	}

	return &Client{Workspace: w}, nil
}
