// © 2025 Platform Engineering Labs Inc.
//
// SPDX-License-Identifier: FSL-1.1-ALv2

package client

import (
	"fmt"

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
//
// A token declared in the target config but resolving to nothing is rejected
// rather than left to that chain. The chain can succeed against a local CLI
// profile or Azure login, which would silently authenticate as a different
// identity than the one the forma names. Omitting the token entirely remains
// the way to defer to the chain deliberately.
func NewClient(cfg *dbconfig.Config) (*Client, error) {
	if cfg.TokenDeclared && cfg.Token == "" {
		return nil, fmt.Errorf("databricks token in target config is empty; omit the token to use the SDK credential chain deliberately")
	}

	w, err := databricks.NewWorkspaceClient(&databricks.Config{
		Host:  cfg.Host,
		Token: cfg.Token,
	})
	if err != nil {
		return nil, err
	}

	return &Client{Workspace: w}, nil
}
