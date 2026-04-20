// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package app

import (
	"testing"

	"xmidt-org/splitter/internal/publisher"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/xmidt-org/touchstone"
)

func TestBuildPrometheusConfig(t *testing.T) {
	tests := []struct {
		name                    string
		yamlCfg                 *publisher.PrometheusConfig
		expectedBatchMetrics    bool
		expectedCompressedBytes bool
		expectedGoCollectors    bool
		expectedClientLabel     bool
	}{
		{
			name:    "NilYAMLConfig_DefaultsToDisabled",
			yamlCfg: nil,

			expectedBatchMetrics:    false,
			expectedCompressedBytes: false,
			expectedGoCollectors:    false,
			expectedClientLabel:     false,
		},
		{
			name: "AllOptionalMetricsEnabled",
			yamlCfg: &publisher.PrometheusConfig{
				EnableBatchMetrics:    true,
				EnableCompressedBytes: true,
				EnableGoCollectors:    true,
				WithClientLabel:       true,
			},
			expectedBatchMetrics:    true,
			expectedCompressedBytes: true,
			expectedGoCollectors:    true,
			expectedClientLabel:     true,
		},
		{
			name: "SomeOptionalMetricsEnabled",
			yamlCfg: &publisher.PrometheusConfig{
				EnableCompressedBytes: true,
				// BatchMetrics, GoCollectors, ClientLabel remain false
			},
			expectedBatchMetrics:    false,
			expectedCompressedBytes: true,
			expectedGoCollectors:    false,
			expectedClientLabel:     false,
		},
		{
			name: "AllOptionalMetricsDisabled",
			yamlCfg: &publisher.PrometheusConfig{
				EnableBatchMetrics:    false,
				EnableCompressedBytes: false,
				EnableGoCollectors:    false,
				WithClientLabel:       false,
			},
			expectedBatchMetrics:    false,
			expectedCompressedBytes: false,
			expectedGoCollectors:    false,
			expectedClientLabel:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup test touchstone config and registerer
			touchstoneCfg := touchstone.Config{
				DefaultNamespace: "test_namespace",
				DefaultSubsystem: "test_subsystem",
			}
			registerer := prometheus.NewRegistry()

			// Execute
			result := buildPrometheusConfig(tt.yamlCfg, touchstoneCfg, registerer)

			// Verify: namespace/subsystem/registerer always come from touchstone
			assert.Equal(t, "test_namespace", result.Namespace, "namespace should come from touchstone")
			assert.Equal(t, "test_subsystem_publisher", result.Subsystem, "subsystem should come from touchstone with _publisher suffix")
			assert.Equal(t, registerer, result.Registerer, "registerer should come from touchstone")

			// Verify: optional metrics match expected values
			assert.Equal(t, tt.expectedBatchMetrics, result.EnableBatchMetrics, "EnableBatchMetrics mismatch")
			assert.Equal(t, tt.expectedCompressedBytes, result.EnableCompressedBytes, "EnableCompressedBytes mismatch")
			assert.Equal(t, tt.expectedGoCollectors, result.EnableGoCollectors, "EnableGoCollectors mismatch")
			assert.Equal(t, tt.expectedClientLabel, result.WithClientLabel, "WithClientLabel mismatch")
		})
	}
}

func TestBuildPrometheusConfig_YAMLNamespaceIgnored(t *testing.T) {
	// Test that even if YAML config has namespace/subsystem, they are ignored
	// in favor of touchstone values
	yamlCfg := &publisher.PrometheusConfig{
		Namespace:             "yaml_namespace", // Should be ignored
		Subsystem:             "yaml_subsystem", // Should be ignored
		EnableCompressedBytes: true,
	}

	touchstoneCfg := touchstone.Config{
		DefaultNamespace: "touchstone_namespace",
		DefaultSubsystem: "touchstone_subsystem",
	}
	registerer := prometheus.NewRegistry()

	result := buildPrometheusConfig(yamlCfg, touchstoneCfg, registerer)

	// Verify touchstone values take precedence
	assert.Equal(t, "touchstone_namespace", result.Namespace, "namespace must come from touchstone, not YAML")
	assert.Equal(t, "touchstone_subsystem_publisher", result.Subsystem, "subsystem must come from touchstone, not YAML")

	// Verify optional metrics still copied
	assert.True(t, result.EnableCompressedBytes, "optional metrics should still be copied from YAML")
}
