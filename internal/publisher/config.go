// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package publisher

import (
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/xmidt-org/wrpkafka"
)

// Config represents the YAML configuration for the WRP Kafka publisher.
// It can be unmarshaled via goschtalt and converted to functional options.
type Config struct {
	// Required fields
	Brokers Brokers

	// Topic routes for WRP message routing
	TopicRoutes []TopicRoute

	// Connection and retry settings
	RequestTimeout         time.Duration
	CleanupTimeout         time.Duration
	RequestRetries         int
	MaxBufferedRecords     int
	MaxBufferedBytes       int
	AllowAutoTopicCreation bool

	// SASL authentication
	SASL *SASLConfig

	// TLS configuration
	TLS *TLSConfig

	// Prometheus metrics configuration
	Prometheus *PrometheusConfig
}

type Brokers struct {
	RestartOnConfigChange bool
	TargetRegion          string
	Regions               map[string][]string
}

// TopicRoute represents a WRP message routing configuration
type TopicRoute struct {
	Topic   string
	Pattern string
	HashKey string
}

// ToWRPKafkaRoute converts a TopicRoute to a wrpkafka.TopicRoute
func (tr TopicRoute) ToWRPKafkaRoute() (wrpkafka.TopicRoute, error) {
	hashKey, err := wrpkafka.ParseHashKey(tr.HashKey)
	if err != nil {
		return wrpkafka.TopicRoute{}, fmt.Errorf("failed to parse hash key %q: %w", tr.HashKey, err)
	}

	route := wrpkafka.TopicRoute{
		Topic:   tr.Topic,
		Pattern: wrpkafka.Pattern(tr.Pattern),
		HashKey: hashKey,
	}
	return route, nil
}

// ToWRPKafkaRoutes converts all TopicRoutes to wrpkafka.TopicRoute slice
func (c Config) ToWRPKafkaRoutes() ([]wrpkafka.TopicRoute, error) {
	routes := make([]wrpkafka.TopicRoute, len(c.TopicRoutes))
	for i, route := range c.TopicRoutes {
		wrpRoute, err := route.ToWRPKafkaRoute()
		if err != nil {
			return nil, fmt.Errorf("failed to convert route %d: %w", i, err)
		}
		routes[i] = wrpRoute
	}
	return routes, nil
}

// SASLConfig represents SASL authentication configuration
type SASLConfig struct {
	Mechanism string `yaml:"mechanism"`
	Username  string `yaml:"username"`
	// #nosec G117
	Password string `yaml:"password"`
}

// TLSConfig represents TLS configuration
type TLSConfig struct {
	Enabled            bool   `yaml:"enabled"`
	InsecureSkipVerify bool   `yaml:"insecure_skip_verify,omitempty"`
	CAFile             string `yaml:"ca_file,omitempty"`
	CertFile           string `yaml:"cert_file,omitempty"`
	KeyFile            string `yaml:"key_file,omitempty"`
}

// PrometheusConfig represents Prometheus metrics configuration for the publisher.
// This includes both franz-go metrics and application-level metrics.
type PrometheusConfig struct {
	// Namespace is the prometheus namespace for metrics (e.g., "xmidt")
	Namespace string `yaml:"namespace,omitempty"`

	// Subsystem is the prometheus subsystem name (e.g., "splitter")
	Subsystem string `yaml:"subsystem,omitempty"`

	// Registerer is the prometheus registerer to use for metrics.
	// If nil, metrics will be registered with the default prometheus registry.
	// This field is typically set programmatically, not via YAML.
	Registerer prometheus.Registerer `yaml:"-"`

	// Optional franz-go prometheus metrics options (disabled by default)
	// Application-level metrics (buffer utilization, publish counter, publish latency) are always enabled

	EnableRecordMetrics   bool `yaml:"enable_record_metrics,omitempty"`
	EnableBatchMetrics    bool `yaml:"enable_batch_metrics,omitempty"`
	EnableCompressedBytes bool `yaml:"enable_compressed_bytes,omitempty"`
	EnableGoCollectors    bool `yaml:"enable_go_collectors,omitempty"`
	WithClientLabel       bool `yaml:"with_client_label,omitempty"`
}
