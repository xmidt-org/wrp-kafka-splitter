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
		description string
	}{
		{
			name: "simple_route",
			topicRoute: TopicRoute{
				Topic:   "events",
				Pattern: "event:.*",
			},
			expected: wrpkafka.TopicRoute{
				Topic:   "events",
				Pattern: wrpkafka.Pattern("event:.*"),
			},
			description: "Should convert TopicRoute to wrpkafka.TopicRoute correctly",
		},
		{
			name: "command_route",
			topicRoute: TopicRoute{
				Topic:   "commands",
				Pattern: "mac:.*/command",
			},
			expected: wrpkafka.TopicRoute{
				Topic:   "commands",
				Pattern: wrpkafka.Pattern("mac:.*/command"),
			},
			description: "Should handle complex routing patterns",
		},
		{
			name: "wildcard_route",
			topicRoute: TopicRoute{
				Topic:   "all-messages",
				Pattern: ".*",
			},
			expected: wrpkafka.TopicRoute{
				Topic:   "all-messages",
				Pattern: wrpkafka.Pattern(".*"),
			},
			description: "Should handle wildcard patterns",
		},
	}

	for _, tt := range tests {
		suite.Run(tt.name, func() {
			result := tt.topicRoute.ToWRPKafkaRoute()
			suite.Equal(tt.expected, result, tt.description)
		})
	}
}

// Test Config ToWRPKafkaRoutes method
func (suite *ConfigTestSuite) TestConfig_ToWRPKafkaRoutes() {
	tests := []struct {
		name        string
		config      Config
		expected    []wrpkafka.TopicRoute
		description string
	}{
		{
			name: "single_route",
			config: Config{
				TopicRoutes: []TopicRoute{
					{Topic: "events", Pattern: "event:.*"},
				},
			},
			expected: []wrpkafka.TopicRoute{
				{Topic: "events", Pattern: wrpkafka.Pattern("event:.*")},
			},
			description: "Should convert single route correctly",
		},
		{
			name: "multiple_routes",
			config: Config{
				TopicRoutes: []TopicRoute{
					{Topic: "events", Pattern: "event:.*"},
					{Topic: "commands", Pattern: "mac:.*/command"},
					{Topic: "responses", Pattern: ".*response.*"},
				},
			},
			expected: []wrpkafka.TopicRoute{
				{Topic: "events", Pattern: wrpkafka.Pattern("event:.*")},
				{Topic: "commands", Pattern: wrpkafka.Pattern("mac:.*/command")},
				{Topic: "responses", Pattern: wrpkafka.Pattern(".*response.*")},
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
	}

	for _, tt := range tests {
		suite.Run(tt.name, func() {
			result := tt.config.ToWRPKafkaRoutes()
			suite.Equal(tt.expected, result, tt.description)
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
				brokers: []string{"localhost:9092"},
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
				brokers: []string{},
				topicRoutes: []wrpkafka.TopicRoute{
					{Topic: "test", Pattern: ".*"},
				},
			},
			expectError: true,
			expectedErr: ErrMissingBrokers,
			description: "Should return error when brokers are empty",
		},
		{
			name: "nil_brokers",
			config: &publisherConfig{
				brokers: nil,
				topicRoutes: []wrpkafka.TopicRoute{
					{Topic: "test", Pattern: ".*"},
				},
			},
			expectError: true,
			expectedErr: ErrMissingBrokers,
			description: "Should return error when brokers are nil",
		},
		{
			name: "missing_topic_routes",
			config: &publisherConfig{
				brokers:     []string{"localhost:9092"},
				topicRoutes: []wrpkafka.TopicRoute{},
			},
			expectError: true,
			expectedErr: ErrMissingTopicRoutes,
			description: "Should return error when topic routes are empty",
		},
		{
			name: "nil_topic_routes",
			config: &publisherConfig{
				brokers:     []string{"localhost:9092"},
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
		setupPub    func() *Publisher
		verifyPub   func(*Publisher)
		description string
	}{
		{
			name:   "WithBrokers_single",
			option: WithBrokers("localhost:9092"),
			setupPub: func() *Publisher {
				return &Publisher{config: &publisherConfig{}}
			},
			verifyPub: func(p *Publisher) {
				suite.Equal([]string{"localhost:9092"}, p.config.brokers)
			},
			description: "Should set single broker correctly",
		},
		{
			name:   "WithBrokers_multiple",
			option: WithBrokers("localhost:9092", "localhost:9093", "localhost:9094"),
			setupPub: func() *Publisher {
				return &Publisher{config: &publisherConfig{}}
			},
			verifyPub: func(p *Publisher) {
				expected := []string{"localhost:9092", "localhost:9093", "localhost:9094"}
				suite.Equal(expected, p.config.brokers)
			},
			description: "Should set multiple brokers correctly",
		},
		{
			name: "WithTopicRoutes_single",
			option: WithTopicRoutes(wrpkafka.TopicRoute{
				Topic:   "events",
				Pattern: "event:.*",
			}),
			setupPub: func() *Publisher {
				return &Publisher{config: &publisherConfig{}}
			},
			verifyPub: func(p *Publisher) {
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
			setupPub: func() *Publisher {
				return &Publisher{config: &publisherConfig{}}
			},
			verifyPub: func(p *Publisher) {
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
			setupPub: func() *Publisher {
				return &Publisher{config: &publisherConfig{}}
			},
			verifyPub: func(p *Publisher) {
				suite.Equal(5000, p.config.maxBufferedRecords)
			},
			description: "Should set max buffered records correctly",
		},
		{
			name:   "WithMaxBufferedBytes",
			option: WithMaxBufferedBytes(1024 * 1024 * 10), // 10MB
			setupPub: func() *Publisher {
				return &Publisher{config: &publisherConfig{}}
			},
			verifyPub: func(p *Publisher) {
				suite.Equal(1024*1024*10, p.config.maxBufferedBytes)
			},
			description: "Should set max buffered bytes correctly",
		},
		{
			name:   "WithRequestTimeout",
			option: WithRequestTimeout(45 * time.Second),
			setupPub: func() *Publisher {
				return &Publisher{config: &publisherConfig{}}
			},
			verifyPub: func(p *Publisher) {
				suite.Equal(45*time.Second, p.config.requestTimeout)
			},
			description: "Should set request timeout correctly",
		},
		{
			name:   "WithCleanupTimeout",
			option: WithCleanupTimeout(15 * time.Second),
			setupPub: func() *Publisher {
				return &Publisher{config: &publisherConfig{}}
			},
			verifyPub: func(p *Publisher) {
				suite.Equal(15*time.Second, p.config.cleanupTimeout)
			},
			description: "Should set cleanup timeout correctly",
		},
		{
			name:   "WithMaxRetries",
			option: WithMaxRetries(5),
			setupPub: func() *Publisher {
				return &Publisher{config: &publisherConfig{}}
			},
			verifyPub: func(p *Publisher) {
				suite.Equal(5, p.config.maxRetries)
			},
			description: "Should set max retries correctly",
		},
		{
			name:   "WithAllowAutoTopicCreation_true",
			option: WithAllowAutoTopicCreation(true),
			setupPub: func() *Publisher {
				return &Publisher{config: &publisherConfig{}}
			},
			verifyPub: func(p *Publisher) {
				suite.True(p.config.allowAutoTopicCreation)
			},
			description: "Should enable auto topic creation",
		},
		{
			name:   "WithAllowAutoTopicCreation_false",
			option: WithAllowAutoTopicCreation(false),
			setupPub: func() *Publisher {
				return &Publisher{config: &publisherConfig{}}
			},
			verifyPub: func(p *Publisher) {
				suite.False(p.config.allowAutoTopicCreation)
			},
			description: "Should disable auto topic creation",
		},
		{
			name:   "WithSASLPlain",
			option: WithSASLPlain("testuser", "testpass"),
			setupPub: func() *Publisher {
				return &Publisher{config: &publisherConfig{}}
			},
			verifyPub: func(p *Publisher) {
				suite.NotNil(p.config.sasl)
			},
			description: "Should set SASL Plain mechanism",
		},
		{
			name:   "WithSASLScram256",
			option: WithSASLScram256("testuser", "testpass"),
			setupPub: func() *Publisher {
				return &Publisher{config: &publisherConfig{}}
			},
			verifyPub: func(p *Publisher) {
				suite.NotNil(p.config.sasl)
			},
			description: "Should set SASL SCRAM-SHA-256 mechanism",
		},
		{
			name:   "WithSASLScram512",
			option: WithSASLScram512("testuser", "testpass"),
			setupPub: func() *Publisher {
				return &Publisher{config: &publisherConfig{}}
			},
			verifyPub: func(p *Publisher) {
				suite.NotNil(p.config.sasl)
			},
			description: "Should set SASL SCRAM-SHA-512 mechanism",
		},
		{
			name:   "WithTLS",
			option: WithTLS(),
			setupPub: func() *Publisher {
				return &Publisher{config: &publisherConfig{}}
			},
			verifyPub: func(p *Publisher) {
				suite.NotNil(p.config.tls)
			},
			description: "Should enable TLS with default config",
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
			pub := &Publisher{config: &publisherConfig{}}
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
			pub := &Publisher{config: &publisherConfig{}}
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
			pub := &Publisher{config: &publisherConfig{}}

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
		WithBrokers("localhost:9092"),
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
