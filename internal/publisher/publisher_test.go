// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package publisher

import (
	"context"
	"errors"
	"log/slog"
	"sync"
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
	logEvents     []log.Event
	metricEvents  []metrics.Event
	eventMutex    sync.Mutex
}

func (suite *PublisherTestSuite) SetupTest() {
	suite.logEmitter = observe.NewSubject[log.Event]()
	suite.metricEmitter = observe.NewSubject[metrics.Event]()

	// Clear events
	suite.eventMutex.Lock()
	suite.logEvents = make([]log.Event, 0)
	suite.metricEvents = make([]metrics.Event, 0)
	suite.eventMutex.Unlock()

	// Attach event collectors
	suite.logEmitter.Attach(func(event log.Event) {
		suite.eventMutex.Lock()
		defer suite.eventMutex.Unlock()
		suite.logEvents = append(suite.logEvents, event)
	})

	suite.metricEmitter.Attach(func(event metrics.Event) {
		suite.eventMutex.Lock()
		defer suite.eventMutex.Unlock()
		suite.metricEvents = append(suite.metricEvents, event)
	})
}

func (suite *PublisherTestSuite) getMetricEvents() []metrics.Event {
	suite.eventMutex.Lock()
	defer suite.eventMutex.Unlock()
	events := make([]metrics.Event, len(suite.metricEvents))
	copy(events, suite.metricEvents)
	return events
}

func (suite *PublisherTestSuite) clearEvents() {
	suite.eventMutex.Lock()
	defer suite.eventMutex.Unlock()
	suite.logEvents = suite.logEvents[:0]
	suite.metricEvents = suite.metricEvents[:0]
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
				WithBrokers(testBroker),
			},
			expectError: true,
			errorType:   ErrMissingTopicRoutes,
			description: "Should return error when no topic routes provided",
		},
		{
			name: "minimal_valid_config",
			options: []Option{
				WithBrokers(testBroker),
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
				WithBrokers(multiBroker),
				WithTopicRoutes(
					wrpkafka.TopicRoute{Topic: "events", Pattern: "event:.*"},
					wrpkafka.TopicRoute{Topic: "requests", Pattern: "mac:.*"},
				),
				WithLogger(slog.Default()),
				WithLogEmitter(suite.logEmitter),
				WithMetricsEmitter(suite.metricEmitter),
				WithMaxBufferedRecords(1000),
				WithMaxBufferedBytes(1024 * 1024),
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
				WithBrokers(testBroker),
				WithTopicRoutes(wrpkafka.TopicRoute{Topic: "test", Pattern: ".*"}),
				WithSASLPlain("user", "pass"),
			},
			expectError: false,
			description: "Should create publisher with SASL Plain authentication",
		},
		{
			name: "with_sasl_scram256",
			options: []Option{
				WithBrokers(testBroker),
				WithTopicRoutes(wrpkafka.TopicRoute{Topic: "test", Pattern: ".*"}),
				WithSASLScram256("user", "pass"),
			},
			expectError: false,
			description: "Should create publisher with SASL SCRAM-SHA-256",
		},
		{
			name: "with_sasl_scram512",
			options: []Option{
				WithBrokers(testBroker),
				WithTopicRoutes(wrpkafka.TopicRoute{Topic: "test", Pattern: ".*"}),
				WithSASLScram512("user", "pass"),
			},
			expectError: false,
			description: "Should create publisher with SASL SCRAM-SHA-512",
		},
		{
			name: "with_tls",
			options: []Option{
				WithBrokers(testBroker),
				WithTopicRoutes(wrpkafka.TopicRoute{Topic: "test", Pattern: ".*"}),
				WithTLS(),
			},
			expectError: false,
			description: "Should create publisher with TLS enabled",
		},
		{
			name: "with_custom_tls_config",
			options: []Option{
				WithBrokers(testBroker),
				WithTopicRoutes(wrpkafka.TopicRoute{Topic: "test", Pattern: ".*"}),
				WithTLSConfig(&TLSConfig{
					Enabled:            true,
					InsecureSkipVerify: true,
				}),
			},
			expectError: false,
			description: "Should create publisher with custom TLS config",
		},
		{
			name: "with_prometheus_metrics",
			options: []Option{
				WithBrokers(testBroker),
				WithTopicRoutes(wrpkafka.TopicRoute{Topic: "test", Pattern: ".*"}),
				WithPrometheusMetrics("xmidt", "splitter", nil),
			},
			expectError: false,
			description: "Should create publisher with Prometheus metrics configuration",
		},
		{
			name: "with_prometheus_empty_values",
			options: []Option{
				WithBrokers(testBroker),
				WithTopicRoutes(wrpkafka.TopicRoute{Topic: "test", Pattern: ".*"}),
				WithPrometheusMetrics("", "", nil),
			},
			expectError: false,
			description: "Should create publisher with empty Prometheus namespace and subsystem",
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

// Test Prometheus metrics configuration
func (suite *PublisherTestSuite) TestPrometheusMetricsConfiguration() {
	tests := []struct {
		name              string
		namespace         string
		subsystem         string
		expectedNamespace string
		expectedSubsystem string
		description       string
	}{
		{
			name:              "valid_namespace_and_subsystem",
			namespace:         "xmidt",
			subsystem:         "splitter",
			expectedNamespace: "xmidt",
			expectedSubsystem: "splitter",
			description:       "Should configure Prometheus with valid namespace and subsystem",
		},
		{
			name:              "empty_namespace_and_subsystem",
			namespace:         "",
			subsystem:         "",
			expectedNamespace: "",
			expectedSubsystem: "",
			description:       "Should handle empty namespace and subsystem",
		},
		{
			name:              "namespace_only",
			namespace:         "monitoring",
			subsystem:         "",
			expectedNamespace: "monitoring",
			expectedSubsystem: "",
			description:       "Should configure with namespace but no subsystem",
		},
		{
			name:              "subsystem_only",
			namespace:         "",
			subsystem:         "kafka_publisher",
			expectedNamespace: "",
			expectedSubsystem: "kafka_publisher",
			description:       "Should configure with subsystem but no namespace",
		},
	}

	for _, tt := range tests {
		suite.Run(tt.name, func() {
			publisher, err := New(
				WithBrokers(testBroker),
				WithTopicRoutes(wrpkafka.TopicRoute{Topic: "test", Pattern: ".*"}),
				WithPrometheusMetrics(tt.namespace, tt.subsystem, nil),
			)

			suite.NoError(err, tt.description)
			suite.NotNil(publisher, "Publisher should be created")
			suite.NotNil(publisher.wrpPublisher, "WRP publisher should be initialized")

			// Verify that the Prometheus configuration was passed to the underlying wrpkafka.Publisher
			suite.Equal(tt.expectedNamespace, publisher.wrpPublisher.PrometheusNamespace, "Prometheus namespace should match")
			suite.Equal(tt.expectedSubsystem, publisher.wrpPublisher.PrometheusSubsystem, "Prometheus subsystem should match")
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
				WithBrokers(testBroker),
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
		WithBrokers(testBroker),
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
	suite.Equal(wrpkafka.Failed, outcome)
}

// Test concurrent access to IsStarted
func (suite *PublisherTestSuite) TestConcurrentIsStarted() {
	publisher, err := New(
		WithBrokers(testBroker),
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

// Test Start method behavior
func (suite *PublisherTestSuite) TestStart() {
	tests := []struct {
		name           string
		setupPublisher func() *KafkaPublisher
		expectError    bool
		errorType      error
		description    string
	}{
		{
			name: "start_already_started_publisher",
			setupPublisher: func() *KafkaPublisher {
				p, _ := New(
					WithBrokers(testBroker),
					WithTopicRoutes(wrpkafka.TopicRoute{Topic: "test", Pattern: ".*"}),
					WithLogEmitter(suite.logEmitter),
				)
				p.started = true // Manually set as started to test error condition
				return p
			},
			expectError: true,
			errorType:   ErrPublisherAlreadyStarted,
			description: "Should return error when publisher is already started",
		},
	}

	for _, tt := range tests {
		suite.Run(tt.name, func() {
			publisher := tt.setupPublisher()

			err := publisher.Start()

			if tt.expectError {
				suite.Error(err, tt.description)
				if tt.errorType != nil {
					suite.ErrorIs(err, tt.errorType)
				}
			} else {
				suite.NoError(err, tt.description)
				suite.True(publisher.IsStarted())
			}
		})
	}
}

// Test Stop method behavior
func (suite *PublisherTestSuite) TestStop() {
	tests := []struct {
		name           string
		setupPublisher func() *KafkaPublisher
		description    string
	}{
		{
			name: "stop_not_started_publisher",
			setupPublisher: func() *KafkaPublisher {
				p, _ := New(
					WithBrokers(testBroker),
					WithTopicRoutes(wrpkafka.TopicRoute{Topic: "test", Pattern: ".*"}),
					WithLogEmitter(suite.logEmitter),
				)
				return p
			},
			description: "Should do nothing when stopping a publisher that's not started",
		},
		{
			name: "stop_started_publisher",
			setupPublisher: func() *KafkaPublisher {
				p, _ := New(
					WithBrokers(testBroker),
					WithTopicRoutes(wrpkafka.TopicRoute{Topic: "test", Pattern: ".*"}),
					WithLogEmitter(suite.logEmitter),
				)
				p.started = true // Manually set as started for this test
				return p
			},
			description: "Should successfully stop a started publisher",
		},
	}

	for _, tt := range tests {
		suite.Run(tt.name, func() {
			suite.clearEvents()
			publisher := tt.setupPublisher()
			ctx := context.Background()

			err := publisher.Stop(ctx)

			suite.NoError(err, tt.description)
			suite.False(publisher.IsStarted(), "Publisher should not be started after Stop()")
		})
	}
}

// Test different WRP message types
func (suite *PublisherTestSuite) TestProduceMessageTypes() {
	tests := []struct {
		name        string
		message     *wrp.Message
		description string
	}{
		{
			name: "simple_event_message",
			message: &wrp.Message{
				Type:        wrp.SimpleEventMessageType,
				Source:      "mac:112233445566/service",
				Destination: "event:device-status/online",
				Payload:     []byte(`{"status": "online"}`),
			},
			description: "Should handle SimpleEventMessageType",
		},
		{
			name: "simple_request_response_message",
			message: &wrp.Message{
				Type:            wrp.SimpleRequestResponseMessageType,
				Source:          "mac:aabbccddeeff/service",
				Destination:     "mac:112233445566/command",
				TransactionUUID: "txn-123",
				Payload:         []byte(`{"command": "reboot"}`),
			},
			description: "Should handle SimpleRequestResponseMessageType",
		},
		{
			name: "create_message",
			message: &wrp.Message{
				Type:        wrp.CreateMessageType,
				Source:      "dns:webpa-server.example.com/api/v2/device",
				Destination: "mac:112233445566/config",
				Payload:     []byte(`{"parameters": {"Device.WiFi.SSID": "MyNetwork"}}`),
			},
			description: "Should handle CreateMessageType",
		},
		{
			name: "retrieve_message",
			message: &wrp.Message{
				Type:        wrp.RetrieveMessageType,
				Source:      "dns:webpa-server.example.com/api/v2/device",
				Destination: "mac:112233445566/config",
				Payload:     []byte(`{"names": ["Device.WiFi.SSID"]}`),
			},
			description: "Should handle RetrieveMessageType",
		},
		{
			name: "update_message",
			message: &wrp.Message{
				Type:        wrp.UpdateMessageType,
				Source:      "dns:webpa-server.example.com/api/v2/device",
				Destination: "mac:112233445566/config",
				Payload:     []byte(`{"parameters": {"Device.WiFi.SSID": "UpdatedNetwork"}}`),
			},
			description: "Should handle UpdateMessageType",
		},
		{
			name: "delete_message",
			message: &wrp.Message{
				Type:        wrp.DeleteMessageType,
				Source:      "dns:webpa-server.example.com/api/v2/device",
				Destination: "mac:112233445566/config",
				Payload:     []byte(`{"names": ["Device.WiFi.SSID"]}`),
			},
			description: "Should handle DeleteMessageType",
		},
	}

	for _, tt := range tests {
		suite.Run(tt.name, func() {
			suite.clearEvents()

			publisher, err := New(
				WithBrokers(testBroker),
				WithTopicRoutes(wrpkafka.TopicRoute{Topic: "test", Pattern: ".*"}),
				WithMetricsEmitter(suite.metricEmitter),
			)
			suite.NoError(err)
			suite.False(publisher.IsStarted())

			outcome, err := publisher.Produce(context.Background(), tt.message)

			suite.Error(err, tt.description+" - should fail when not started")
			suite.ErrorIs(err, ErrPublisherNotStarted)
			suite.Equal(wrpkafka.Failed, outcome) // Default outcome when not started

			// Verify metrics were emitted
			time.Sleep(10 * time.Millisecond)
			metricEvents := suite.getMetricEvents()
			suite.Len(metricEvents, 1)
			suite.Equal("publish_errors_total", metricEvents[0].Name)
		})
	}
}

// Test edge cases and error conditions
func (suite *PublisherTestSuite) TestEdgeCases() {
	tests := []struct {
		name        string
		setupTest   func() (*KafkaPublisher, *wrp.Message, context.Context)
		expectError bool
		description string
	}{
		{
			name: "produce_with_nil_message",
			setupTest: func() (*KafkaPublisher, *wrp.Message, context.Context) {
				p, _ := New(
					WithBrokers(testBroker),
					WithTopicRoutes(wrpkafka.TopicRoute{Topic: "test", Pattern: ".*"}),
				)
				return p, nil, context.Background()
			},
			expectError: true,
			description: "Should handle nil message gracefully",
		},
		{
			name: "produce_with_canceled_context",
			setupTest: func() (*KafkaPublisher, *wrp.Message, context.Context) {
				p, _ := New(
					WithBrokers(testBroker),
					WithTopicRoutes(wrpkafka.TopicRoute{Topic: "test", Pattern: ".*"}),
				)
				ctx, cancel := context.WithCancel(context.Background())
				cancel() // Cancel immediately

				message := &wrp.Message{
					Type:   wrp.SimpleEventMessageType,
					Source: "test",
				}

				return p, message, ctx
			},
			expectError: true,
			description: "Should handle canceled context appropriately",
		},
		{
			name: "produce_with_empty_message_fields",
			setupTest: func() (*KafkaPublisher, *wrp.Message, context.Context) {
				p, _ := New(
					WithBrokers(testBroker),
					WithTopicRoutes(wrpkafka.TopicRoute{Topic: "test", Pattern: ".*"}),
				)

				// Message with minimal/empty fields
				message := &wrp.Message{
					Type: wrp.SimpleEventMessageType,
					// No source, destination, or payload
				}

				return p, message, context.Background()
			},
			expectError: true,
			description: "Should handle messages with empty fields",
		},
	}

	for _, tt := range tests {
		suite.Run(tt.name, func() {
			publisher, message, ctx := tt.setupTest()

			outcome, err := publisher.Produce(ctx, message)

			if tt.expectError {
				suite.Error(err, tt.description)
			} else {
				suite.NoError(err, tt.description)
			}

			// When not started, should get default outcome
			if !publisher.IsStarted() {
				suite.Equal(wrpkafka.Failed, outcome)
			}
		})
	}
}

// Test concurrent Start/Stop operations
func (suite *PublisherTestSuite) TestConcurrentStartStop() {
	publisher, err := New(
		WithBrokers(testBroker),
		WithTopicRoutes(wrpkafka.TopicRoute{Topic: "test", Pattern: ".*"}),
		WithLogEmitter(suite.logEmitter),
	)
	suite.NoError(err)

	const numGoroutines = 10
	var wg sync.WaitGroup
	errorlist := make(chan error, numGoroutines*2)

	// Launch multiple goroutines trying to start
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := publisher.Start(); err != nil {
				errorlist <- err
			}
		}()
	}

	// Launch multiple goroutines trying to stop
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := publisher.Stop(context.Background()); err != nil {
				errorlist <- err
			}
		}()
	}

	wg.Wait()
	close(errorlist)

	// Check that we got expected errors (multiple start attempts)
	errorCount := 0
	for err := range errorlist {
		if errors.Is(err, ErrPublisherAlreadyStarted) {
			errorCount++
		}
	}

	// We should have gotten some "already started" errors
	suite.Greater(errorCount, 0, "Should have received some 'already started' errors from concurrent operations")
}

// Test publisher configuration validation
func (suite *PublisherTestSuite) TestPublisherValidation() {
	tests := []struct {
		name        string
		options     []Option
		expectError bool
		errorCheck  func(error) bool
		description string
	}{
		{
			name: "empty_broker_list",
			options: []Option{
				WithBrokers(Brokers{
					RestartOnConfigChange: false,
					TargetRegion:          "us-east-1",
					Regions:               map[string][]string{}, // Empty regions map
				}),
				WithTopicRoutes(wrpkafka.TopicRoute{Topic: "test", Pattern: ".*"}),
			},
			expectError: true,
			errorCheck:  func(err error) bool { return err.Error() == "brokers cannot be empty" },
			description: "Should reject empty broker regions",
		},
		{
			name: "empty_topic_routes",
			options: []Option{
				WithBrokers(testBroker),
				WithTopicRoutes(), // Empty topic routes
			},
			expectError: true,
			errorCheck:  func(err error) bool { return err.Error() == "topic routes cannot be empty" },
			description: "Should reject empty topic routes",
		},
		{
			name: "valid_minimal_config",
			options: []Option{
				WithBrokers(testBroker),
				WithTopicRoutes(wrpkafka.TopicRoute{Topic: "test", Pattern: ".*"}),
			},
			expectError: false,
			description: "Should accept minimal valid configuration",
		},
	}

	for _, tt := range tests {
		suite.Run(tt.name, func() {
			publisher, err := New(tt.options...)

			if tt.expectError {
				suite.Error(err, tt.description)
				suite.Nil(publisher)
				if tt.errorCheck != nil {
					suite.True(tt.errorCheck(err), "Error should match expected pattern")
				}
			} else {
				suite.NoError(err, tt.description)
				suite.NotNil(publisher)
			}
		})
	}
}

// Run the test suite
func TestPublisherTestSuite(t *testing.T) {
	suite.Run(t, new(PublisherTestSuite))
}
