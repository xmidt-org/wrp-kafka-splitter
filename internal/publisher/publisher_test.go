// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package publisher

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"xmidt-org/splitter/internal/log"
	"xmidt-org/splitter/internal/metrics"
	"xmidt-org/splitter/internal/observe"

	"github.com/stretchr/testify/suite"
	"github.com/xmidt-org/wrp-go/v5"
	"github.com/xmidt-org/wrpkafka"
)

// Test suite for Publisher - focusing on testable aspects
type PublisherTestSuite struct {
	suite.Suite
	logEmitter    *observe.Subject[log.Event]
	metricEmitter *observe.Subject[metrics.Event]
}

func (suite *PublisherTestSuite) SetupTest() {
	suite.logEmitter = observe.NewSubject[log.Event]()
	suite.metricEmitter = observe.NewSubject[metrics.Event]()
}

// Test New function with different option combinations
func (suite *PublisherTestSuite) TestNew() {
	tests := []struct {
		name        string
		options     []Option
		expectError bool
		errorType   error
		description string
	}{
		{
			name:        "missing_brokers",
			options:     []Option{},
			expectError: true,
			errorType:   ErrMissingBrokers,
			description: "Should return error when no brokers provided",
		},
		{
			name: "missing_topic_routes",
			options: []Option{
				WithBrokers("localhost:9092"),
			},
			expectError: true,
			errorType:   ErrMissingTopicRoutes,
			description: "Should return error when no topic routes provided",
		},
		{
			name: "minimal_valid_config",
			options: []Option{
				WithBrokers("localhost:9092"),
				WithTopicRoutes(wrpkafka.TopicRoute{
					Topic:   "test-topic",
					Pattern: ".*",
				}),
			},
			expectError: false,
			description: "Should create publisher with minimal valid config",
		},
		{
			name: "complete_config",
			options: []Option{
				WithBrokers("localhost:9092", "localhost:9093"),
				WithTopicRoutes(
					wrpkafka.TopicRoute{Topic: "events", Pattern: "event:.*"},
					wrpkafka.TopicRoute{Topic: "requests", Pattern: "mac:.*"},
				),
				WithLogger(slog.Default()),
				WithLogEmitter(suite.logEmitter),
				WithMetricsEmitter(suite.metricEmitter),
				WithMaxBufferedRecords(1000),
				WithMaxBufferedBytes(1024*1024),
				WithRequestTimeout(30 * time.Second),
				WithCleanupTimeout(10 * time.Second),
				WithMaxRetries(3),
				WithAllowAutoTopicCreation(true),
			},
			expectError: false,
			description: "Should create publisher with complete config",
		},
		{
			name: "with_sasl_plain",
			options: []Option{
				WithBrokers("localhost:9092"),
				WithTopicRoutes(wrpkafka.TopicRoute{Topic: "test", Pattern: ".*"}),
				WithSASLPlain("user", "pass"),
			},
			expectError: false,
			description: "Should create publisher with SASL Plain authentication",
		},
		{
			name: "with_sasl_scram256",
			options: []Option{
				WithBrokers("localhost:9092"),
				WithTopicRoutes(wrpkafka.TopicRoute{Topic: "test", Pattern: ".*"}),
				WithSASLScram256("user", "pass"),
			},
			expectError: false,
			description: "Should create publisher with SASL SCRAM-SHA-256",
		},
		{
			name: "with_sasl_scram512",
			options: []Option{
				WithBrokers("localhost:9092"),
				WithTopicRoutes(wrpkafka.TopicRoute{Topic: "test", Pattern: ".*"}),
				WithSASLScram512("user", "pass"),
			},
			expectError: false,
			description: "Should create publisher with SASL SCRAM-SHA-512",
		},
		{
			name: "with_tls",
			options: []Option{
				WithBrokers("localhost:9092"),
				WithTopicRoutes(wrpkafka.TopicRoute{Topic: "test", Pattern: ".*"}),
				WithTLS(),
			},
			expectError: false,
			description: "Should create publisher with TLS enabled",
		},
		{
			name: "with_custom_tls_config",
			options: []Option{
				WithBrokers("localhost:9092"),
				WithTopicRoutes(wrpkafka.TopicRoute{Topic: "test", Pattern: ".*"}),
				WithTLSConfig(&TLSConfig{
					Enabled:            true,
					InsecureSkipVerify: true,
				}),
			},
			expectError: false,
			description: "Should create publisher with custom TLS config",
		},
	}

	for _, tt := range tests {
		suite.Run(tt.name, func() {
			publisher, err := New(tt.options...)

			if tt.expectError {
				suite.Error(err, tt.description)
				if tt.errorType != nil {
					suite.ErrorIs(err, tt.errorType)
				}
				suite.Nil(publisher)
			} else {
				suite.NoError(err, tt.description)
				suite.NotNil(publisher)
				suite.NotNil(publisher.config)
				suite.NotNil(publisher.logEmitter)
				suite.NotNil(publisher.metricEmitter)
				suite.False(publisher.started)
			}
		})
	}
}

// Test IsStarted method
func (suite *PublisherTestSuite) TestIsStarted() {
	tests := []struct {
		name        string
		started     bool
		expected    bool
		description string
	}{
		{
			name:        "publisher_started",
			started:     true,
			expected:    true,
			description: "Should return true when publisher is started",
		},
		{
			name:        "publisher_not_started",
			started:     false,
			expected:    false,
			description: "Should return false when publisher is not started",
		},
	}

	for _, tt := range tests {
		suite.Run(tt.name, func() {
			publisher, _ := New(
				WithBrokers("localhost:9092"),
				WithTopicRoutes(wrpkafka.TopicRoute{Topic: "test", Pattern: ".*"}),
			)
			publisher.started = tt.started
			
			result := publisher.IsStarted()
			
			suite.Equal(tt.expected, result, tt.description)
		})
	}
}

// Test error handling for not started publisher
func (suite *PublisherTestSuite) TestProduceWhenNotStarted() {
	publisher, err := New(
		WithBrokers("localhost:9092"),
		WithTopicRoutes(wrpkafka.TopicRoute{Topic: "test", Pattern: ".*"}),
		WithMetricsEmitter(suite.metricEmitter),
	)
	suite.NoError(err)
	suite.False(publisher.IsStarted())

	message := &wrp.Message{
		Type:   wrp.SimpleEventMessageType,
		Source: "test",
	}

	outcome, err := publisher.Produce(context.Background(), message)
	
	suite.Error(err)
	suite.ErrorIs(err, ErrPublisherNotStarted)
	suite.Equal(wrpkafka.Accepted, outcome) // Default outcome when not started
}

// Test concurrent access to IsStarted
func (suite *PublisherTestSuite) TestConcurrentIsStarted() {
	publisher, err := New(
		WithBrokers("localhost:9092"),
		WithTopicRoutes(wrpkafka.TopicRoute{Topic: "test", Pattern: ".*"}),
	)
	suite.NoError(err)
	
	// Test concurrent access
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				publisher.IsStarted()
			}
			done <- true
		}()
	}
	
	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}
	
	suite.False(publisher.IsStarted())
}

// Run the test suite
func TestPublisherTestSuite(t *testing.T) {
	suite.Run(t, new(PublisherTestSuite))
}