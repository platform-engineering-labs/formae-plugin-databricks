// © 2025 Platform Engineering Labs Inc.
//
// SPDX-License-Identifier: FSL-1.1-ALv2

package client

import (
	"strings"
	"testing"

	dbconfig "github.com/platform-engineering-labs/formae-plugin-databricks/pkg/config"
)

// A token declared in the target config but resolving to nothing must not fall
// through to the SDK credential chain: that chain can succeed against a local
// CLI profile or Azure login, silently authenticating as a different identity
// than the one the forma names.
func TestNewClientRejectsADeclaredButEmptyToken(t *testing.T) {
	_, err := NewClient(&dbconfig.Config{
		Host:          "https://example.cloud.databricks.com",
		Token:         "",
		TokenDeclared: true,
	})
	if err == nil {
		t.Fatal("NewClient accepted a declared but empty token")
	}
	if !strings.Contains(err.Error(), "token") {
		t.Errorf("error %q should name the token", err)
	}
}
