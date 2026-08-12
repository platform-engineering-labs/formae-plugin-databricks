// © 2025 Platform Engineering Labs Inc.
//
// SPDX-License-Identifier: FSL-1.1-ALv2

package config

import (
	"testing"
)

func TestTokenIsTakenFromTheTargetConfig(t *testing.T) {
	cfg := FromTargetConfig([]byte(`{"Host":"https://example.cloud.databricks.com","Token":"dapi-token"}`))

	if cfg.Token != "dapi-token" {
		t.Errorf("Token = %q, want the target config value", cfg.Token)
	}
	if !cfg.TokenDeclared {
		t.Error("TokenDeclared = false, want true for a token present in the config")
	}
}

// An absent token is the documented way to defer to the Databricks SDK
// credential chain, so it must stay distinguishable from an empty one.
func TestAbsentTokenIsNotReportedAsDeclared(t *testing.T) {
	cfg := FromTargetConfig([]byte(`{"Host":"https://example.cloud.databricks.com"}`))

	if cfg.Token != "" {
		t.Errorf("Token = %q, want empty", cfg.Token)
	}
	if cfg.TokenDeclared {
		t.Error("TokenDeclared = true, want false for a token absent from the config")
	}
}

func TestDeclaredButEmptyTokenIsReportedAsDeclared(t *testing.T) {
	cfg := FromTargetConfig([]byte(`{"Host":"https://example.cloud.databricks.com","Token":""}`))

	if !cfg.TokenDeclared {
		t.Error("TokenDeclared = false, want true so an empty declared token can be rejected")
	}
}

func TestHostIsTakenFromTheTargetConfig(t *testing.T) {
	cfg := FromTargetConfig([]byte(`{"Host":"https://example.cloud.databricks.com"}`))

	if cfg.Host != "https://example.cloud.databricks.com" {
		t.Errorf("Host = %q, want the target config value", cfg.Host)
	}
}
