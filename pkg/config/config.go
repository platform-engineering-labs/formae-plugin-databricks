// © 2025 Platform Engineering Labs Inc.
//
// SPDX-License-Identifier: FSL-1.1-ALv2

package config

import "encoding/json"

// Config holds Databricks-specific configuration extracted from a Target.
type Config struct {
	Host  string
	Token string

	// TokenDeclared records whether the target config carried a Token at all,
	// which an empty Token alone cannot express. An absent token defers to the
	// SDK credential chain; a declared but empty one is a misconfiguration.
	TokenDeclared bool
}

// FromTargetConfig extracts Databricks configuration from target config JSON.
// Host is expected from the target config (required in Pkl schema).
//
// Token is optional: when absent, the SDK's default credential chain handles
// auth. When declared it may originate from a formae-managed secret, which the
// agent resolves live before every call, so rotating it needs no agent restart.
func FromTargetConfig(targetConfig json.RawMessage) *Config {
	cfg := &Config{}

	if targetConfig != nil {
		var raw map[string]interface{}
		if err := json.Unmarshal(targetConfig, &raw); err == nil {
			cfg.Host, _ = raw["Host"].(string)
			if token, ok := raw["Token"]; ok {
				cfg.Token, _ = token.(string)
				cfg.TokenDeclared = true
			}
		}
	}

	return cfg
}
