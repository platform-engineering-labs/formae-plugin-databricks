// © 2025 Platform Engineering Labs Inc.
//
// SPDX-License-Identifier: FSL-1.1-ALv2

package config

import "encoding/json"

// Config holds Databricks-specific configuration extracted from a Target.
type Config struct {
	Host  string
	Token string
}

// FromTargetConfig extracts Databricks configuration from target config JSON.
// Host is expected from the target config (required in Pkl schema).
// Token is optional — the SDK's default credential chain handles auth when absent.
func FromTargetConfig(targetConfig json.RawMessage) *Config {
	cfg := &Config{}

	if targetConfig != nil {
		var raw map[string]interface{}
		if err := json.Unmarshal(targetConfig, &raw); err == nil {
			cfg.Host, _ = raw["Host"].(string)
			cfg.Token, _ = raw["Token"].(string)
		}
	}

	return cfg
}
