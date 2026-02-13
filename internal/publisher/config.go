// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package publisher

import (
	"time"

	"github.com/xmidt-org/wrpkafka"
)

// Config represents the YAML configuration for the WRP Kafka publisher.
// It can be unmarshaled via goschtalt and converted to functional options.
type Config struct {
	// Required fields
	Brokers []string `yaml:"brokers"`

	// Topic routes for WRP message routing
	TopicRoutes []TopicRoute `yaml:"topic_routes"`

	// Connection and retry settings
	RequestTimeout         time.Duration `yaml:"request_timeout,omitempty"`
	CleanupTimeout         time.Duration `yaml:"cleanup_timeout,omitempty"`
	RequestRetries         int           `yaml:"request_retries,omitempty"`
	MaxBufferedRecords     int           `yaml:"max_buffered_records,omitempty"`
	MaxBufferedBytes       int           `yaml:"max_buffered_bytes,omitempty"`
	AllowAutoTopicCreation bool          `yaml:"allow_auto_topic_creation,omitempty"`

	// SASL authentication
	SASL *SASLConfig `yaml:"sasl,omitempty"`

	// TLS configuration
	TLS *TLSConfig `yaml:"tls,omitempty"`
}

// TopicRoute represents a WRP message routing configuration
type TopicRoute struct {
	Topic   string `yaml:"topic"`
	Pattern string `yaml:"pattern"`
}

// ToWRPKafkaRoute converts a TopicRoute to a wrpkafka.TopicRoute
func (tr TopicRoute) ToWRPKafkaRoute() wrpkafka.TopicRoute {
	route := wrpkafka.TopicRoute{
		Topic:   tr.Topic,
		Pattern: wrpkafka.Pattern(tr.Pattern),
	}
	return route
}

// ToWRPKafkaRoutes converts all TopicRoutes to wrpkafka.TopicRoute slice
func (c Config) ToWRPKafkaRoutes() []wrpkafka.TopicRoute {
	routes := make([]wrpkafka.TopicRoute, len(c.TopicRoutes))
	for i, route := range c.TopicRoutes {
		routes[i] = route.ToWRPKafkaRoute()
	}
	return routes
}

// SASLConfig represents SASL authentication configuration
type SASLConfig struct {
	Mechanism string `yaml:"mechanism"`
	Username  string `yaml:"username"`
	Password  string `yaml:"password"`
}

// TLSConfig represents TLS configuration
type TLSConfig struct {
	Enabled            bool   `yaml:"enabled"`
	InsecureSkipVerify bool   `yaml:"insecure_skip_verify,omitempty"`
	CAFile             string `yaml:"ca_file,omitempty"`
	CertFile           string `yaml:"cert_file,omitempty"`
	KeyFile            string `yaml:"key_file,omitempty"`
}
