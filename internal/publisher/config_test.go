// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package publisher

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/xmidt-org/wrpkafka"
)

var testBroker = Brokers{
	RestartOnConfigChange: false,
	TargetRegion:          "us-east-1",
	Regions: map[string][]string{
		"us-east-1": {"localhost:9092"},
	},
}

var multiBroker = Brokers{
	RestartOnConfigChange: false,
	TargetRegion:          "us-east-1",
	Regions: map[string][]string{
		"us-east-1": {"localhost:9092", "localhost:9093", "localhost:9094"},
		"us-west-2": {"localhost:9095"},
	},
}

var emptyBroker = Brokers{
	RestartOnConfigChange: false,
	TargetRegion:          "us-east-1",
	Regions:               map[string][]string{},
}

// Test suite for Config
type ConfigTestSuite struct {
	suite.Suite
}

// Test TopicRoute conversion methods
func (suite *ConfigTestSuite) TestTopicRoute_ToWRPKafkaRoute() {
	tests := []struct {
		name        string
		topicRoute  TopicRoute
		expected    wrpkafka.TopicRoute
		expectError bool
		description string
	}{
		{
			name: "simple_route",
			topicRoute: TopicRoute{
				Topic:   "events",
				Pattern: "event:.*",
				HashKey: "source",
			},
			expected: wrpkafka.TopicRoute{
				Topic:   "events",
				Pattern: wrpkafka.Pattern("event:.*"),
				HashKey: wrpkafka.HashKey{Name: wrpkafka.HashKeySource},
			},
			description: "Should convert TopicRoute to wrpkafka.TopicRoute correctly",
		},
		{
			name: "command_route",
			topicRoute: TopicRoute{
				Topic:   "commands",
				Pattern: "mac:.*/command",
				HashKey: "metadata/hw-deviceid",
			},
			expected: wrpkafka.TopicRoute{
				Topic:   "commands",
				Pattern: wrpkafka.Pattern("mac:.*/command"),
				HashKey: wrpkafka.HashKey{Name: wrpkafka.HashKeyMetadata, MetadataField: "hw-deviceid"},
			},
			description: "Should handle complex routing patterns with metadata hash key",
		},
		{
			name: "wildcard_route",
			topicRoute: TopicRoute{
				Topic:   "all-messages",
				Pattern: ".*",
				HashKey: "none",
			},
			expected: wrpkafka.TopicRoute{
				Topic:   "all-messages",
				Pattern: wrpkafka.Pattern(".*"),
				HashKey: wrpkafka.HashKey{Name: wrpkafka.HashKeyNone},
			},
			description: "Should handle wildcard patterns with no hash key",
		},
		{
			name: "route_with_default_hash_key",
			topicRoute: TopicRoute{
				Topic:   "events",
				Pattern: "event:.*",
				HashKey: "", // Empty should default to metadata/hw-deviceid
			},
			expected: wrpkafka.TopicRoute{
				Topic:   "events",
				Pattern: wrpkafka.Pattern("event:.*"),
				HashKey: wrpkafka.HashKey{Name: wrpkafka.HashKeyMetadata, MetadataField: "hw-deviceid"},
			},
			description: "Should default to metadata/hw-deviceid when hash_key is empty",
		},
		{
			name: "invalid_hash_key",
			topicRoute: TopicRoute{
				Topic:   "events",
				Pattern: "event:.*",
				HashKey: "invalid",
			},
			expectError: true,
			description: "Should return error for invalid hash key",
		},
	}

	for _, tt := range tests {
		suite.Run(tt.name, func() {
			result, err := tt.topicRoute.ToWRPKafkaRoute()
			if tt.expectError {
				suite.Error(err, tt.description)
			} else {
				suite.NoError(err, tt.description)
				suite.Equal(tt.expected, result, tt.description)
			}
		})
	}
}

// Test Config ToWRPKafkaRoutes method
func (suite *ConfigTestSuite) TestConfig_ToWRPKafkaRoutes() {
	tests := []struct {
		name        string
		config      Config
		expected    []wrpkafka.TopicRoute
		expectError bool
		description string
	}{
		{
			name: "single_route",
			config: Config{
				TopicRoutes: []TopicRoute{
					{Topic: "events", Pattern: "event:.*", HashKey: "source"},
				},
			},
			expected: []wrpkafka.TopicRoute{
				{Topic: "events", Pattern: wrpkafka.Pattern("event:.*"), HashKey: wrpkafka.HashKey{Name: wrpkafka.HashKeySource}},
			},
			description: "Should convert single route correctly",
		},
		{
			name: "multiple_routes",
			config: Config{
				TopicRoutes: []TopicRoute{
					{Topic: "events", Pattern: "event:.*", HashKey: "source"},
					{Topic: "commands", Pattern: "mac:.*/command", HashKey: "metadata/hw-deviceid"},
					{Topic: "responses", Pattern: ".*response.*", HashKey: "none"},
				},
			},
			expected: []wrpkafka.TopicRoute{
				{Topic: "events", Pattern: wrpkafka.Pattern("event:.*"), HashKey: wrpkafka.HashKey{Name: wrpkafka.HashKeySource}},
				{Topic: "commands", Pattern: wrpkafka.Pattern("mac:.*/command"), HashKey: wrpkafka.HashKey{Name: wrpkafka.HashKeyMetadata, MetadataField: "hw-deviceid"}},
				{Topic: "responses", Pattern: wrpkafka.Pattern(".*response.*"), HashKey: wrpkafka.HashKey{Name: wrpkafka.HashKeyNone}},
			},
			description: "Should convert multiple routes correctly",
		},
		{
			name: "empty_routes",
			config: Config{
				TopicRoutes: []TopicRoute{},
			},
			expected:    []wrpkafka.TopicRoute{},
			description: "Should handle empty routes slice",
		},
		{
			name: "route_with_invalid_hash_key",
			config: Config{
				TopicRoutes: []TopicRoute{
					{Topic: "events", Pattern: "event:.*", HashKey: "invalid"},
				},
			},
			expectError: true,
			description: "Should return error for invalid hash key",
		},
	}

	for _, tt := range tests {
		suite.Run(tt.name, func() {
			result, err := tt.config.ToWRPKafkaRoutes()
			if tt.expectError {
				suite.Error(err, tt.description)
			} else {
				suite.NoError(err, tt.description)
				suite.Equal(tt.expected, result, tt.description)
			}
		})
	}
}

// Run the config test suite
func TestConfigTestSuite(t *testing.T) {
	suite.Run(t, new(ConfigTestSuite))
}

// Test suite for Options
type OptionsTestSuite struct {
	suite.Suite
}

// Test publisherConfig validation
func (suite *OptionsTestSuite) TestPublisherConfig_Validate() {
	tests := []struct {
		name        string
		config      *publisherConfig
		expectError bool
		expectedErr error
		description string
	}{
		{
			name: "valid_config",
			config: &publisherConfig{
				brokers: testBroker,
				topicRoutes: []wrpkafka.TopicRoute{
					{Topic: "test", Pattern: ".*"},
				},
			},
			expectError: false,
			description: "Should validate successfully with required fields",
		},
		{
			name: "missing_brokers",
			config: &publisherConfig{
				brokers: emptyBroker,
				topicRoutes: []wrpkafka.TopicRoute{
					{Topic: "test", Pattern: ".*"},
				},
			},
			expectError: true,
			description: "Should return error when brokers regions are empty",
		},
		{
			name: "invalid_target_region",
			config: &publisherConfig{
				brokers: Brokers{
					RestartOnConfigChange: false,
					TargetRegion:          "nonexistent-region",
					Regions: map[string][]string{
						"us-east-1": {"localhost:9092"},
					},
				},
				topicRoutes: []wrpkafka.TopicRoute{
					{Topic: "test", Pattern: ".*"},
				},
			},
			expectError: true,
			description: "Should return error when target region is not in regions map",
		},
		{
			name: "missing_topic_routes",
			config: &publisherConfig{
				brokers:     testBroker,
				topicRoutes: []wrpkafka.TopicRoute{},
			},
			expectError: true,
			expectedErr: ErrMissingTopicRoutes,
			description: "Should return error when topic routes are empty",
		},
		{
			name: "nil_topic_routes",
			config: &publisherConfig{
				brokers:     testBroker,
				topicRoutes: nil,
			},
			expectError: true,
			expectedErr: ErrMissingTopicRoutes,
			description: "Should return error when topic routes are nil",
		},
	}

	for _, tt := range tests {
		suite.Run(tt.name, func() {
			err := tt.config.validate()

			if tt.expectError {
				suite.Error(err, tt.description)
				if tt.expectedErr != nil {
					suite.ErrorIs(err, tt.expectedErr)
				}
			} else {
				suite.NoError(err, tt.description)
			}
		})
	}
}

// Test individual option functions
func (suite *OptionsTestSuite) TestOptions() {
	tests := []struct {
		name        string
		option      Option
		setupPub    func() *KafkaPublisher
		verifyPub   func(*KafkaPublisher)
		description string
	}{
		{
			name:   "WithBrokers_single",
			option: WithBrokers(testBroker),
			setupPub: func() *KafkaPublisher {
				return &KafkaPublisher{config: &publisherConfig{}}
			},
			verifyPub: func(p *KafkaPublisher) {
				suite.Equal(testBroker, p.config.brokers)
			},
			description: "Should set broker configuration correctly",
		},
		{
			name:   "WithBrokers_multiple",
			option: WithBrokers(multiBroker),
			setupPub: func() *KafkaPublisher {
				return &KafkaPublisher{config: &publisherConfig{}}
			},
			verifyPub: func(p *KafkaPublisher) {
				suite.Equal(multiBroker, p.config.brokers)
			},
			description: "Should set multi-region broker configuration correctly",
		},
		{
			name: "WithTopicRoutes_single",
			option: WithTopicRoutes(wrpkafka.TopicRoute{
				Topic:   "events",
				Pattern: "event:.*",
			}),
			setupPub: func() *KafkaPublisher {
				return &KafkaPublisher{config: &publisherConfig{}}
			},
			verifyPub: func(p *KafkaPublisher) {
				expected := []wrpkafka.TopicRoute{
					{Topic: "events", Pattern: "event:.*"},
				}
				suite.Equal(expected, p.config.topicRoutes)
			},
			description: "Should set single topic route correctly",
		},
		{
			name: "WithTopicRoutes_multiple",
			option: WithTopicRoutes(
				wrpkafka.TopicRoute{Topic: "events", Pattern: "event:.*"},
				wrpkafka.TopicRoute{Topic: "commands", Pattern: "mac:.*/command"},
			),
			setupPub: func() *KafkaPublisher {
				return &KafkaPublisher{config: &publisherConfig{}}
			},
			verifyPub: func(p *KafkaPublisher) {
				expected := []wrpkafka.TopicRoute{
					{Topic: "events", Pattern: "event:.*"},
					{Topic: "commands", Pattern: "mac:.*/command"},
				}
				suite.Equal(expected, p.config.topicRoutes)
			},
			description: "Should set multiple topic routes correctly",
		},
		{
			name:   "WithMaxBufferedRecords",
			option: WithMaxBufferedRecords(5000),
			setupPub: func() *KafkaPublisher {
				return &KafkaPublisher{config: &publisherConfig{}}
			},
			verifyPub: func(p *KafkaPublisher) {
				suite.Equal(5000, p.config.maxBufferedRecords)
			},
			description: "Should set max buffered records correctly",
		},
		{
			name:   "WithMaxBufferedBytes",
			option: WithMaxBufferedBytes(1024 * 1024 * 10), // 10MB
			setupPub: func() *KafkaPublisher {
				return &KafkaPublisher{config: &publisherConfig{}}
			},
			verifyPub: func(p *KafkaPublisher) {
				suite.Equal(1024*1024*10, p.config.maxBufferedBytes)
			},
			description: "Should set max buffered bytes correctly",
		},
		{
			name:   "WithRequestTimeout",
			option: WithRequestTimeout(45 * time.Second),
			setupPub: func() *KafkaPublisher {
				return &KafkaPublisher{config: &publisherConfig{}}
			},
			verifyPub: func(p *KafkaPublisher) {
				suite.Equal(45*time.Second, p.config.requestTimeout)
			},
			description: "Should set request timeout correctly",
		},
		{
			name:   "WithCleanupTimeout",
			option: WithCleanupTimeout(15 * time.Second),
			setupPub: func() *KafkaPublisher {
				return &KafkaPublisher{config: &publisherConfig{}}
			},
			verifyPub: func(p *KafkaPublisher) {
				suite.Equal(15*time.Second, p.config.cleanupTimeout)
			},
			description: "Should set cleanup timeout correctly",
		},
		{
			name:   "WithMaxRetries",
			option: WithMaxRetries(5),
			setupPub: func() *KafkaPublisher {
				return &KafkaPublisher{config: &publisherConfig{}}
			},
			verifyPub: func(p *KafkaPublisher) {
				suite.Equal(5, p.config.maxRetries)
			},
			description: "Should set max retries correctly",
		},
		{
			name:   "WithAllowAutoTopicCreation_true",
			option: WithAllowAutoTopicCreation(true),
			setupPub: func() *KafkaPublisher {
				return &KafkaPublisher{config: &publisherConfig{}}
			},
			verifyPub: func(p *KafkaPublisher) {
				suite.True(p.config.allowAutoTopicCreation)
			},
			description: "Should enable auto topic creation",
		},
		{
			name:   "WithAllowAutoTopicCreation_false",
			option: WithAllowAutoTopicCreation(false),
			setupPub: func() *KafkaPublisher {
				return &KafkaPublisher{config: &publisherConfig{}}
			},
			verifyPub: func(p *KafkaPublisher) {
				suite.False(p.config.allowAutoTopicCreation)
			},
			description: "Should disable auto topic creation",
		},
		{
			name:   "WithSASLPlain",
			option: WithSASLPlain("testuser", "testpass"),
			setupPub: func() *KafkaPublisher {
				return &KafkaPublisher{config: &publisherConfig{}}
			},
			verifyPub: func(p *KafkaPublisher) {
				suite.NotNil(p.config.sasl)
			},
			description: "Should set SASL Plain mechanism",
		},
		{
			name:   "WithSASLScram256",
			option: WithSASLScram256("testuser", "testpass"),
			setupPub: func() *KafkaPublisher {
				return &KafkaPublisher{config: &publisherConfig{}}
			},
			verifyPub: func(p *KafkaPublisher) {
				suite.NotNil(p.config.sasl)
			},
			description: "Should set SASL SCRAM-SHA-256 mechanism",
		},
		{
			name:   "WithSASLScram512",
			option: WithSASLScram512("testuser", "testpass"),
			setupPub: func() *KafkaPublisher {
				return &KafkaPublisher{config: &publisherConfig{}}
			},
			verifyPub: func(p *KafkaPublisher) {
				suite.NotNil(p.config.sasl)
			},
			description: "Should set SASL SCRAM-SHA-512 mechanism",
		},
		{
			name:   "WithTLS",
			option: WithTLS(),
			setupPub: func() *KafkaPublisher {
				return &KafkaPublisher{config: &publisherConfig{}}
			},
			verifyPub: func(p *KafkaPublisher) {
				suite.NotNil(p.config.tls)
			},
			description: "Should enable TLS with default config",
		},
		{
			name:   "WithPrometheusConfig_both_values",
			option: WithPrometheusConfig(&PrometheusConfig{Namespace: "xmidt", Subsystem: "splitter"}),
			setupPub: func() *KafkaPublisher {
				return &KafkaPublisher{config: &publisherConfig{}}
			},
			verifyPub: func(p *KafkaPublisher) {
				suite.NotNil(p.config.prometheus)
				suite.Equal("xmidt", p.config.prometheus.Namespace)
				suite.Equal("splitter", p.config.prometheus.Subsystem)
			},
			description: "Should set Prometheus namespace and subsystem correctly",
		},
		{
			name:   "WithPrometheusConfig_empty_values",
			option: WithPrometheusConfig(&PrometheusConfig{Namespace: "", Subsystem: ""}),
			setupPub: func() *KafkaPublisher {
				return &KafkaPublisher{config: &publisherConfig{}}
			},
			verifyPub: func(p *KafkaPublisher) {
				suite.NotNil(p.config.prometheus)
				suite.Equal("", p.config.prometheus.Namespace)
				suite.Equal("", p.config.prometheus.Subsystem)
			},
			description: "Should accept empty Prometheus namespace and subsystem",
		},
		{
			name:   "WithPrometheusConfig_namespace_only",
			option: WithPrometheusConfig(&PrometheusConfig{Namespace: "monitoring", Subsystem: ""}),
			setupPub: func() *KafkaPublisher {
				return &KafkaPublisher{config: &publisherConfig{}}
			},
			verifyPub: func(p *KafkaPublisher) {
				suite.NotNil(p.config.prometheus)
				suite.Equal("monitoring", p.config.prometheus.Namespace)
				suite.Equal("", p.config.prometheus.Subsystem)
			},
			description: "Should set Prometheus namespace with empty subsystem",
		},
		{
			name:   "WithPrometheusConfig_subsystem_only",
			option: WithPrometheusConfig(&PrometheusConfig{Namespace: "", Subsystem: "kafka"}),
			setupPub: func() *KafkaPublisher {
				return &KafkaPublisher{config: &publisherConfig{}}
			},
			verifyPub: func(p *KafkaPublisher) {
				suite.NotNil(p.config.prometheus)
				suite.Equal("", p.config.prometheus.Namespace)
				suite.Equal("kafka", p.config.prometheus.Subsystem)
			},
			description: "Should set Prometheus subsystem with empty namespace",
		},
	}

	for _, tt := range tests {
		suite.Run(tt.name, func() {
			pub := tt.setupPub()

			err := tt.option.apply(pub)

			suite.NoError(err, tt.description)
			tt.verifyPub(pub)
		})
	}
}

// Test SASL config options with error cases
func (suite *OptionsTestSuite) TestSASLConfigOptions() {
	tests := []struct {
		name        string
		saslConfig  *SASLConfig
		expectError bool
		description string
	}{
		{
			name: "valid_plain_config",
			saslConfig: &SASLConfig{
				Mechanism: "PLAIN",
				Username:  "user",
				Password:  "pass",
			},
			expectError: false,
			description: "Should handle valid PLAIN SASL config",
		},
		{
			name: "valid_scram256_config",
			saslConfig: &SASLConfig{
				Mechanism: "SCRAM-SHA-256",
				Username:  "user",
				Password:  "pass",
			},
			expectError: false,
			description: "Should handle valid SCRAM-SHA-256 config",
		},
		{
			name: "valid_scram512_config",
			saslConfig: &SASLConfig{
				Mechanism: "SCRAM-SHA-512",
				Username:  "user",
				Password:  "pass",
			},
			expectError: false,
			description: "Should handle valid SCRAM-SHA-512 config",
		},
		{
			name: "invalid_mechanism",
			saslConfig: &SASLConfig{
				Mechanism: "INVALID",
				Username:  "user",
				Password:  "pass",
			},
			expectError: true,
			description: "Should return error for invalid SASL mechanism",
		},
		{
			name: "missing_username",
			saslConfig: &SASLConfig{
				Mechanism: "PLAIN",
				Username:  "",
				Password:  "pass",
			},
			expectError: true,
			description: "Should return error when username is missing",
		},
		{
			name: "missing_password",
			saslConfig: &SASLConfig{
				Mechanism: "PLAIN",
				Username:  "user",
				Password:  "",
			},
			expectError: true,
			description: "Should return error when password is missing",
		},
	}

	for _, tt := range tests {
		suite.Run(tt.name, func() {
			pub := &KafkaPublisher{config: &publisherConfig{}}
			option := WithSASLConfig(tt.saslConfig)

			err := option.apply(pub)

			if tt.expectError {
				suite.Error(err, tt.description)
			} else {
				suite.NoError(err, tt.description)
				suite.NotNil(pub.config.sasl, "SASL mechanism should be set")
			}
		})
	}
}

// Test TLS config options
func (suite *OptionsTestSuite) TestTLSConfigOptions() {
	tests := []struct {
		name        string
		tlsConfig   *TLSConfig
		expectError bool
		description string
	}{
		{
			name: "tls_enabled",
			tlsConfig: &TLSConfig{
				Enabled: true,
			},
			expectError: false,
			description: "Should handle basic TLS config",
		},
		{
			name: "tls_with_insecure_skip",
			tlsConfig: &TLSConfig{
				Enabled:            true,
				InsecureSkipVerify: true,
			},
			expectError: false,
			description: "Should handle TLS with insecure skip verify",
		},
		{
			name: "tls_disabled",
			tlsConfig: &TLSConfig{
				Enabled: false,
			},
			expectError: false,
			description: "Should handle disabled TLS config",
		},
		{
			name: "tls_with_cert_files",
			tlsConfig: &TLSConfig{
				Enabled:  true,
				CAFile:   "/path/to/ca.pem",
				CertFile: "/path/to/cert.pem",
				KeyFile:  "/path/to/key.pem",
			},
			expectError: true,
			description: "Should return error for non-existent certificate files",
		},
		{
			name: "invalid_ca_file",
			tlsConfig: &TLSConfig{
				Enabled: true,
				CAFile:  "/nonexistent/ca.pem",
			},
			expectError: true,
			description: "Should return error for invalid CA file",
		},
		{
			name: "invalid_cert_file",
			tlsConfig: &TLSConfig{
				Enabled:  true,
				CertFile: "/nonexistent/cert.pem",
				KeyFile:  "/nonexistent/key.pem",
			},
			expectError: true,
			description: "Should return error for invalid cert files",
		},
	}

	for _, tt := range tests {
		suite.Run(tt.name, func() {
			pub := &KafkaPublisher{config: &publisherConfig{}}
			option := WithTLSConfig(tt.tlsConfig)

			err := option.apply(pub)

			if tt.expectError {
				suite.Error(err, tt.description)
			} else {
				suite.NoError(err, tt.description)
				if tt.tlsConfig.Enabled {
					suite.NotNil(pub.config.tls, "TLS config should be set when enabled")
				}
			}
		})
	}
}

// Test nil option handling
func (suite *OptionsTestSuite) TestNilOptions() {
	tests := []struct {
		name        string
		option      Option
		description string
	}{
		{
			name:        "WithSASLConfig_nil",
			option:      WithSASLConfig(nil),
			description: "Should handle nil SASL config gracefully",
		},
		{
			name:        "WithTLSConfig_nil",
			option:      WithTLSConfig(nil),
			description: "Should handle nil TLS config gracefully",
		},
	}

	for _, tt := range tests {
		suite.Run(tt.name, func() {
			pub := &KafkaPublisher{config: &publisherConfig{}}

			err := tt.option.apply(pub)

			suite.NoError(err, tt.description)
		})
	}
}

// Run the options test suite
func TestOptionsTestSuite(t *testing.T) {
	suite.Run(t, new(OptionsTestSuite))
}

// Benchmark tests for publisher operations
func BenchmarkPublisher_IsStarted(b *testing.B) {
	pub, _ := New(
		WithBrokers(testBroker),
		WithTopicRoutes(wrpkafka.TopicRoute{Topic: "test", Pattern: ".*"}),
	)
	pub.started = true

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pub.IsStarted()
	}
}

// Test error constants
func TestErrorConstants(t *testing.T) {
	assert.Equal(t, "publisher is not started", ErrPublisherNotStarted.Error())
	assert.Equal(t, "publisher is already started", ErrPublisherAlreadyStarted.Error())
	assert.Equal(t, "brokers cannot be empty", ErrMissingBrokers.Error())
	assert.Equal(t, "topic routes cannot be empty", ErrMissingTopicRoutes.Error())
}
