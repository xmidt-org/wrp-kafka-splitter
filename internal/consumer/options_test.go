// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package consumer

import (
	"context"
	"crypto/tls"
	"log/slog"
	"os"
	"testing"
	"time"

	"xmidt-org/splitter/internal/log"
	"xmidt-org/splitter/internal/metrics"

	kit "github.com/go-kit/kit/metrics"
	"github.com/stretchr/testify/suite"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/xmidt-org/wrpkafka"
)

type OptionsTestSuite struct {
	suite.Suite
}

func TestOptionsTestSuite(t *testing.T) {
	suite.Run(t, new(OptionsTestSuite))
}

// Helper to create a basic consumer for testing
func (s *OptionsTestSuite) createTestConsumer(opts ...Option) (*Consumer, error) {
	// Start with required options
	baseOpts := []Option{
		WithBrokers("localhost:9092"),
		WithTopics("test-topic"),
		WithGroupID("test-group"),
		WithMessageHandler(MessageHandlerFunc(func(ctx context.Context, record *kgo.Record) (wrpkafka.Outcome, error) {
			return wrpkafka.Attempted, nil
		})),
	}

	allOpts := append(baseOpts, opts...)
	return New(allOpts...)
}

// Test Required Options

func (s *OptionsTestSuite) TestWithBrokers() {
	consumer, err := s.createTestConsumer()
	s.NoError(err)
	s.NotNil(consumer)
	s.Equal([]string{"localhost:9092"}, consumer.config.brokers)
}

func (s *OptionsTestSuite) TestWithBrokers_Multiple() {
	consumer, err := New(
		WithBrokers("broker1:9092", "broker2:9092", "broker3:9092"),
		WithTopics("test-topic"),
		WithGroupID("test-group"),
		WithMessageHandler(MessageHandlerFunc(func(ctx context.Context, record *kgo.Record) (wrpkafka.Outcome, error) {
			return wrpkafka.Attempted, nil
		})),
	)
	s.NoError(err)
	s.NotNil(consumer)
	s.Equal([]string{"broker1:9092", "broker2:9092", "broker3:9092"}, consumer.config.brokers)
}

func (s *OptionsTestSuite) TestWithTopics() {
	consumer, err := s.createTestConsumer()
	s.NoError(err)
	s.Equal([]string{"test-topic"}, consumer.config.topics)
}

func (s *OptionsTestSuite) TestWithTopics_Multiple() {
	consumer, err := New(
		WithBrokers("localhost:9092"),
		WithTopics("topic1", "topic2", "topic3"),
		WithGroupID("test-group"),
		WithMessageHandler(MessageHandlerFunc(func(ctx context.Context, record *kgo.Record) (wrpkafka.Outcome, error) {
			return wrpkafka.Attempted, nil
		})),
	)
	s.NoError(err)
	s.Equal([]string{"topic1", "topic2", "topic3"}, consumer.config.topics)
}

func (s *OptionsTestSuite) TestWithGroupID() {
	consumer, err := s.createTestConsumer()
	s.NoError(err)
	s.Equal("test-group", consumer.config.groupID)
}

func (s *OptionsTestSuite) TestWithMessageHandler() {
	called := false
	handler := MessageHandlerFunc(func(ctx context.Context, record *kgo.Record) (wrpkafka.Outcome, error) {
		called = true
		return wrpkafka.Attempted, nil
	})

	consumer, err := New(
		WithBrokers("localhost:9092"),
		WithTopics("test-topic"),
		WithGroupID("test-group"),
		WithMessageHandler(handler),
	)
	s.NoError(err)
	s.NotNil(consumer.config.handler)

	// Test that the handler works
	var outcome wrpkafka.Outcome
	outcome, err = consumer.config.handler.HandleMessage(context.Background(), &kgo.Record{})
	s.NoError(err)
	s.True(called)
	s.Equal(wrpkafka.Attempted, outcome)
}

func (s *OptionsTestSuite) TestWithMessageHandler_Nil() {
	consumer, err := New(
		WithBrokers("localhost:9092"),
		WithTopics("test-topic"),
		WithGroupID("test-group"),
		WithMessageHandler(nil),
	)
	s.Error(err)
	s.Nil(consumer)
}

// Test Session and Heartbeat Options

func (s *OptionsTestSuite) TestWithSessionTimeout() {
	consumer, err := s.createTestConsumer(
		WithSessionTimeout(30 * time.Second),
	)
	s.NoError(err)
	s.NotNil(consumer)
	// Verify option was added to kgoOpts
	s.NotEmpty(consumer.config.kgoOpts)
}

func (s *OptionsTestSuite) TestWithSessionTimeout_Zero() {
	consumer, err := s.createTestConsumer(
		WithSessionTimeout(0),
	)
	s.NoError(err)
	s.NotNil(consumer)
}

func (s *OptionsTestSuite) TestWithHeartbeatInterval() {
	consumer, err := s.createTestConsumer(
		WithHeartbeatInterval(3 * time.Second),
	)
	s.NoError(err)
	s.NotNil(consumer)
	s.NotEmpty(consumer.config.kgoOpts)
}

func (s *OptionsTestSuite) TestWithRebalanceTimeout() {
	consumer, err := s.createTestConsumer(
		WithRebalanceTimeout(60 * time.Second),
	)
	s.NoError(err)
	s.NotNil(consumer)
	s.NotEmpty(consumer.config.kgoOpts)
}

// Test Fetch Options

func (s *OptionsTestSuite) TestWithFetchMinBytes() {
	consumer, err := s.createTestConsumer(
		WithFetchMinBytes(1024),
	)
	s.NoError(err)
	s.NotNil(consumer)
	s.NotEmpty(consumer.config.kgoOpts)
}

func (s *OptionsTestSuite) TestWithFetchMinBytes_Zero() {
	consumer, err := s.createTestConsumer(
		WithFetchMinBytes(0),
	)
	s.NoError(err)
	s.NotNil(consumer)
}

func (s *OptionsTestSuite) TestWithFetchMaxBytes() {
	consumer, err := s.createTestConsumer(
		WithFetchMaxBytes(52428800), // 50MB
	)
	s.NoError(err)
	s.NotNil(consumer)
	s.NotEmpty(consumer.config.kgoOpts)
}

func (s *OptionsTestSuite) TestWithFetchMaxWait() {
	consumer, err := s.createTestConsumer(
		WithFetchMaxWait(500 * time.Millisecond),
	)
	s.NoError(err)
	s.NotNil(consumer)
	s.NotEmpty(consumer.config.kgoOpts)
}

func (s *OptionsTestSuite) TestWithFetchMaxPartitionBytes() {
	consumer, err := s.createTestConsumer(
		WithFetchMaxPartitionBytes(1048576), // 1MB
	)
	s.NoError(err)
	s.NotNil(consumer)
	s.NotEmpty(consumer.config.kgoOpts)
}

func (s *OptionsTestSuite) TestWithMaxConcurrentFetches() {
	consumer, err := s.createTestConsumer(
		WithMaxConcurrentFetches(10),
	)
	s.NoError(err)
	s.NotNil(consumer)
	s.NotEmpty(consumer.config.kgoOpts)
}

// Test Auto-Commit Options

func (s *OptionsTestSuite) TestWithAutoCommitInterval() {
	consumer, err := s.createTestConsumer(
		WithAutoCommitInterval(5 * time.Second),
	)
	s.NoError(err)
	s.NotNil(consumer)
	s.NotEmpty(consumer.config.kgoOpts)
}

// func (s *OptionsTestSuite) TestWithDisableAutoCommit() {
// 	consumer, err := s.createTestConsumer(
// 		WithDisableAutoCommit(true),
// 	)
// 	s.NoError(err)
// 	s.NotNil(consumer)
// 	s.True(consumer.config.autocommitDisabled)
// 	s.NotEmpty(consumer.config.kgoOpts)
// }

// func (s *OptionsTestSuite) TestWithDisableAutoCommit_False() {
// 	consumer, err := s.createTestConsumer(
// 		WithDisableAutoCommit(false),
// 	)
// 	s.NoError(err)
// 	s.NotNil(consumer)
// 	s.False(consumer.config.autocommitDisabled)
// }

// Test SASL Authentication Options

func (s *OptionsTestSuite) TestWithSASLPlain() {
	consumer, err := s.createTestConsumer(
		WithSASLPlain("user", "password"),
	)
	s.NoError(err)
	s.NotNil(consumer)
	s.NotEmpty(consumer.config.kgoOpts)
}

func (s *OptionsTestSuite) TestWithSASLScram256() {
	consumer, err := s.createTestConsumer(
		WithSASLScram256("user", "password"),
	)
	s.NoError(err)
	s.NotNil(consumer)
	s.NotEmpty(consumer.config.kgoOpts)
}

func (s *OptionsTestSuite) TestWithSASLScram512() {
	consumer, err := s.createTestConsumer(
		WithSASLScram512("user", "password"),
	)
	s.NoError(err)
	s.NotNil(consumer)
	s.NotEmpty(consumer.config.kgoOpts)
}

func (s *OptionsTestSuite) TestWithSASLConfig_Nil() {
	consumer, err := s.createTestConsumer(
		WithSASLConfig(nil),
	)
	s.NoError(err)
	s.NotNil(consumer)
}

func (s *OptionsTestSuite) TestWithSASLConfig_PLAIN() {
	consumer, err := s.createTestConsumer(
		WithSASLConfig(&SASLConfig{
			Mechanism: "PLAIN",
			Username:  "user",
			Password:  "password",
		}),
	)
	s.NoError(err)
	s.NotNil(consumer)
}

func (s *OptionsTestSuite) TestWithSASLConfig_SCRAM256() {
	consumer, err := s.createTestConsumer(
		WithSASLConfig(&SASLConfig{
			Mechanism: "SCRAM-SHA-256",
			Username:  "user",
			Password:  "password",
		}),
	)
	s.NoError(err)
	s.NotNil(consumer)
}

func (s *OptionsTestSuite) TestWithSASLConfig_SCRAM512() {
	consumer, err := s.createTestConsumer(
		WithSASLConfig(&SASLConfig{
			Mechanism: "SCRAM-SHA-512",
			Username:  "user",
			Password:  "password",
		}),
	)
	s.NoError(err)
	s.NotNil(consumer)
}

func (s *OptionsTestSuite) TestWithSASLConfig_InvalidMechanism() {
	consumer, err := s.createTestConsumer(
		WithSASLConfig(&SASLConfig{
			Mechanism: "INVALID",
			Username:  "user",
			Password:  "password",
		}),
	)
	s.Error(err)
	s.Nil(consumer)
	s.Contains(err.Error(), "unsupported sasl mechanism")
}

func (s *OptionsTestSuite) TestWithSASLConfig_MissingUsername() {
	consumer, err := s.createTestConsumer(
		WithSASLConfig(&SASLConfig{
			Mechanism: "PLAIN",
			Username:  "",
			Password:  "password",
		}),
	)
	s.Error(err)
	s.Nil(consumer)
	s.Contains(err.Error(), "username is required")
}

func (s *OptionsTestSuite) TestWithSASLConfig_MissingPassword() {
	consumer, err := s.createTestConsumer(
		WithSASLConfig(&SASLConfig{
			Mechanism: "PLAIN",
			Username:  "user",
			Password:  "",
		}),
	)
	s.Error(err)
	s.Nil(consumer)
	s.Contains(err.Error(), "password is required")
}

// Test TLS Options

func (s *OptionsTestSuite) TestWithTLS() {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true, // #nosec G402 - testing only
	}

	consumer, err := s.createTestConsumer(
		WithTLS(tlsConfig),
	)
	s.NoError(err)
	s.NotNil(consumer)
	s.NotEmpty(consumer.config.kgoOpts)
}

func (s *OptionsTestSuite) TestWithTLS_Nil() {
	consumer, err := s.createTestConsumer(
		WithTLS(nil),
	)
	s.NoError(err)
	s.NotNil(consumer)
	s.NotEmpty(consumer.config.kgoOpts)
}

func (s *OptionsTestSuite) TestWithTLSConfig_Nil() {
	consumer, err := s.createTestConsumer(
		WithTLSConfig(nil),
	)
	s.NoError(err)
	s.NotNil(consumer)
}

func (s *OptionsTestSuite) TestWithTLSConfig_Disabled() {
	consumer, err := s.createTestConsumer(
		WithTLSConfig(&TLSConfig{
			Enabled: false,
		}),
	)
	s.NoError(err)
	s.NotNil(consumer)
}

func (s *OptionsTestSuite) TestWithTLSConfig_InsecureSkipVerify() {
	consumer, err := s.createTestConsumer(
		WithTLSConfig(&TLSConfig{
			Enabled:            true,
			InsecureSkipVerify: true,
		}),
	)
	s.NoError(err)
	s.NotNil(consumer)
	s.NotEmpty(consumer.config.kgoOpts)
}

func (s *OptionsTestSuite) TestWithTLSConfig_InvalidCAFile() {
	consumer, err := s.createTestConsumer(
		WithTLSConfig(&TLSConfig{
			Enabled: true,
			CAFile:  "/nonexistent/ca.pem",
		}),
	)
	s.Error(err)
	s.Nil(consumer)
	s.Contains(err.Error(), "failed to read ca file")
}

func (s *OptionsTestSuite) TestWithTLSConfig_InvalidCertFile() {
	consumer, err := s.createTestConsumer(
		WithTLSConfig(&TLSConfig{
			Enabled:  true,
			CertFile: "/nonexistent/cert.pem",
			KeyFile:  "/nonexistent/key.pem",
		}),
	)
	s.Error(err)
	s.Nil(consumer)
	s.Contains(err.Error(), "failed to load client certificate")
}

// Test Retry and Backoff Options

func (s *OptionsTestSuite) TestWithRequestRetries() {
	consumer, err := s.createTestConsumer(
		WithRequestRetries(3),
	)
	s.NoError(err)
	s.NotNil(consumer)
	s.NotEmpty(consumer.config.kgoOpts)
}

func (s *OptionsTestSuite) TestWithRetryBackoff() {
	backoffFn := func(tries int) time.Duration {
		return time.Duration(tries) * time.Second
	}

	consumer, err := s.createTestConsumer(
		WithRetryBackoff(backoffFn),
	)
	s.NoError(err)
	s.NotNil(consumer)
	s.NotEmpty(consumer.config.kgoOpts)
}

func (s *OptionsTestSuite) TestWithRetryBackoff_Nil() {
	consumer, err := s.createTestConsumer(
		WithRetryBackoff(nil),
	)
	s.NoError(err)
	s.NotNil(consumer)
}

// Test Connection Options

func (s *OptionsTestSuite) TestWithConnIdleTimeout() {
	consumer, err := s.createTestConsumer(
		WithConnIdleTimeout(10 * time.Minute),
	)
	s.NoError(err)
	s.NotNil(consumer)
	s.NotEmpty(consumer.config.kgoOpts)
}

func (s *OptionsTestSuite) TestWithRequestTimeoutOverhead() {
	consumer, err := s.createTestConsumer(
		WithRequestTimeoutOverhead(10 * time.Second),
	)
	s.NoError(err)
	s.NotNil(consumer)
	s.NotEmpty(consumer.config.kgoOpts)
}

// Test Logger Options

func (s *OptionsTestSuite) TestWithKafkaLogger() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	consumer, err := s.createTestConsumer(
		WithKafkaLogger(logger),
	)
	s.NoError(err)
	s.NotNil(consumer)
	s.NotEmpty(consumer.config.kgoOpts)
}

func (s *OptionsTestSuite) TestWithKafkaLogger_Nil() {
	consumer, err := s.createTestConsumer(
		WithKafkaLogger(nil),
	)
	s.NoError(err)
	s.NotNil(consumer)
}

func (s *OptionsTestSuite) TestWithLogEmitter() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	emitter := log.New(logger)

	consumer, err := s.createTestConsumer(
		WithLogEmitter(emitter),
	)
	s.NoError(err)
	s.NotNil(consumer)
	s.NotNil(consumer.logEmitter)
}

func (s *OptionsTestSuite) TestWithLogEmitter_Nil() {
	consumer, err := s.createTestConsumer(
		WithLogEmitter(nil),
	)
	s.NoError(err)
	s.NotNil(consumer)
}

// Test Metrics Options

func (s *OptionsTestSuite) TestWithPrometheusMetrics() {
	consumer, err := s.createTestConsumer(
		WithPrometheusMetrics("test_namespace", "test_subsystem"),
	)
	s.NoError(err)
	s.NotNil(consumer)
	s.NotEmpty(consumer.config.kgoOpts)
}

func (s *OptionsTestSuite) TestWithPrometheusMetrics_EmptyNamespace() {
	consumer, err := s.createTestConsumer(
		WithPrometheusMetrics("", "subsystem"),
	)
	s.Error(err)
	s.Nil(consumer)
	s.Contains(err.Error(), "namespace cannot be empty")
}

func (s *OptionsTestSuite) TestWithPrometheusMetrics_EmptySubsystem() {
	consumer, err := s.createTestConsumer(
		WithPrometheusMetrics("namespace", ""),
	)
	s.Error(err)
	s.Nil(consumer)
	s.Contains(err.Error(), "subsystem cannot be empty")
}

func (s *OptionsTestSuite) TestWithMetricsEmitter() {
	counter := &mockCounter{}
	m := metrics.Metrics{
		ConsumerFetchErrors: counter,
	}
	emitter := metrics.New(m)

	consumer, err := s.createTestConsumer(
		WithMetricsEmitter(emitter),
	)
	s.NoError(err)
	s.NotNil(consumer)
	s.NotNil(consumer.metricEmitter)
}

func (s *OptionsTestSuite) TestWithMetricsEmitter_Nil() {
	consumer, err := s.createTestConsumer(
		WithMetricsEmitter(nil),
	)
	s.NoError(err)
	s.NotNil(consumer)
}

func (s *OptionsTestSuite) TestWithHooks() {
	var hookCalled bool
	hook := &testHook{
		onFetchRecordBuffered: func(r *kgo.Record) {
			hookCalled = true
		},
	}

	consumer, err := s.createTestConsumer(
		WithHooks(hook),
	)
	s.NoError(err)
	s.NotNil(consumer)
	s.NotEmpty(consumer.config.kgoOpts)
	s.False(hookCalled) // Hook will be called by franz-go, not during construction
}

// Test Offset Management Options

func (s *OptionsTestSuite) TestWithConsumeFromTheBeginning_True() {
	consumer, err := s.createTestConsumer(
		WithConsumeFromTheBeginning(true),
	)
	s.NoError(err)
	s.NotNil(consumer)
	s.NotEmpty(consumer.config.kgoOpts)
}

func (s *OptionsTestSuite) TestWithConsumeFromTheBeginning_False() {
	consumer, err := s.createTestConsumer(
		WithConsumeFromTheBeginning(false),
	)
	s.NoError(err)
	s.NotNil(consumer)
}

func (s *OptionsTestSuite) TestWithConsumeResetOffset() {
	offset := kgo.NewOffset().AtStart()

	consumer, err := s.createTestConsumer(
		WithConsumeResetOffset(&offset),
	)
	s.NoError(err)
	s.NotNil(consumer)
	s.NotEmpty(consumer.config.kgoOpts)
}

func (s *OptionsTestSuite) TestWithConsumeResetOffset_Nil() {
	consumer, err := s.createTestConsumer(
		WithConsumeResetOffset(nil),
	)
	s.NoError(err)
	s.NotNil(consumer)
}

// Test Partition Assignment Options

func (s *OptionsTestSuite) TestWithOnPartitionsAssigned() {
	var called bool
	fn := func(ctx context.Context, client *kgo.Client, partitions map[string][]int32) {
		called = true
	}

	consumer, err := s.createTestConsumer(
		WithOnPartitionsAssigned(fn),
	)
	s.NoError(err)
	s.NotNil(consumer)
	s.NotEmpty(consumer.config.kgoOpts)
	s.False(called) // Will be called by franz-go during partition assignment
}

func (s *OptionsTestSuite) TestWithOnPartitionsRevoked() {
	var called bool
	fn := func(ctx context.Context, client *kgo.Client, partitions map[string][]int32) {
		called = true
	}

	consumer, err := s.createTestConsumer(
		WithOnPartitionsRevoked(fn),
	)
	s.NoError(err)
	s.NotNil(consumer)
	s.NotEmpty(consumer.config.kgoOpts)
	s.False(called)
}

func (s *OptionsTestSuite) TestWithOnPartitionsLost() {
	var called bool
	fn := func(ctx context.Context, client *kgo.Client, partitions map[string][]int32) {
		called = true
	}

	consumer, err := s.createTestConsumer(
		WithOnPartitionsLost(fn),
	)
	s.NoError(err)
	s.NotNil(consumer)
	s.NotEmpty(consumer.config.kgoOpts)
	s.False(called)
}

// Test Client ID, Rack, and Instance ID Options

func (s *OptionsTestSuite) TestWithClientID() {
	consumer, err := s.createTestConsumer(
		WithClientID("my-client-id"),
	)
	s.NoError(err)
	s.NotNil(consumer)
	s.NotEmpty(consumer.config.kgoOpts)
}

func (s *OptionsTestSuite) TestWithClientID_Empty() {
	consumer, err := s.createTestConsumer(
		WithClientID(""),
	)
	s.NoError(err)
	s.NotNil(consumer)
}

func (s *OptionsTestSuite) TestWithRack() {
	consumer, err := s.createTestConsumer(
		WithRack("rack-1"),
	)
	s.NoError(err)
	s.NotNil(consumer)
	s.NotEmpty(consumer.config.kgoOpts)
}

func (s *OptionsTestSuite) TestWithRack_Empty() {
	consumer, err := s.createTestConsumer(
		WithRack(""),
	)
	s.NoError(err)
	s.NotNil(consumer)
}

func (s *OptionsTestSuite) TestWithInstanceID() {
	consumer, err := s.createTestConsumer(
		WithInstanceID("instance-1"),
	)
	s.NoError(err)
	s.NotNil(consumer)
	s.NotEmpty(consumer.config.kgoOpts)
}

func (s *OptionsTestSuite) TestWithInstanceID_Empty() {
	consumer, err := s.createTestConsumer(
		WithInstanceID(""),
	)
	s.NoError(err)
	s.NotNil(consumer)
}

// Test validate function

func (s *OptionsTestSuite) TestValidate_MissingBrokers() {
	cfg := &consumerConfig{
		topics:  []string{"topic"},
		groupID: "group",
		handler: MessageHandlerFunc(func(ctx context.Context, record *kgo.Record) (wrpkafka.Outcome, error) {
			return wrpkafka.Attempted, nil
		}),
	}

	err := validate(cfg)
	s.Error(err)
	s.Contains(err.Error(), "brokers are required")
}

func (s *OptionsTestSuite) TestValidate_MissingTopics() {
	cfg := &consumerConfig{
		brokers: []string{"localhost:9092"},
		groupID: "group",
		handler: MessageHandlerFunc(func(ctx context.Context, record *kgo.Record) (wrpkafka.Outcome, error) {
			return wrpkafka.Attempted, nil
		}),
	}

	err := validate(cfg)
	s.Error(err)
	s.Contains(err.Error(), "topics are required")
}

func (s *OptionsTestSuite) TestValidate_MissingGroupID() {
	cfg := &consumerConfig{
		brokers: []string{"localhost:9092"},
		topics:  []string{"topic"},
		handler: MessageHandlerFunc(func(ctx context.Context, record *kgo.Record) (wrpkafka.Outcome, error) {
			return wrpkafka.Attempted, nil
		}),
	}

	err := validate(cfg)
	s.Error(err)
	s.Contains(err.Error(), "group ID is required")
}

func (s *OptionsTestSuite) TestValidate_MissingHandler() {
	cfg := &consumerConfig{
		brokers: []string{"localhost:9092"},
		topics:  []string{"topic"},
		groupID: "group",
	}

	err := validate(cfg)
	s.Error(err)
	s.Contains(err.Error(), "message handler is required")
}

func (s *OptionsTestSuite) TestValidate_Success() {
	cfg := &consumerConfig{
		brokers: []string{"localhost:9092"},
		topics:  []string{"topic"},
		groupID: "group",
		handler: MessageHandlerFunc(func(ctx context.Context, record *kgo.Record) (wrpkafka.Outcome, error) {
			return wrpkafka.Attempted, nil
		}),
	}

	err := validate(cfg)
	s.NoError(err)
}

// Test slogAdapter

func (s *OptionsTestSuite) TestSlogAdapter_Level() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	adapter := &slogAdapter{logger: logger}

	level := adapter.Level()
	s.Equal(kgo.LogLevelInfo, level)
}

func (s *OptionsTestSuite) TestSlogAdapter_Log() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	adapter := &slogAdapter{logger: logger}

	// Test all log levels - just verify they don't panic
	s.NotPanics(func() {
		adapter.Log(kgo.LogLevelDebug, "debug message", "key", "value")
		adapter.Log(kgo.LogLevelInfo, "info message", "key", "value")
		adapter.Log(kgo.LogLevelWarn, "warn message", "key", "value")
		adapter.Log(kgo.LogLevelError, "error message", "key", "value")
	})
}

// Helper types for testing

type mockCounter struct {
	addCalled  bool
	addValue   float64
	withLabels []string
}

func (m *mockCounter) With(labelValues ...string) kit.Counter {
	m.withLabels = append([]string{}, labelValues...)
	return m
}

func (m *mockCounter) Add(delta float64) {
	m.addCalled = true
	m.addValue += delta
}

// testHook is a minimal implementation of kgo.Hook for testing
type testHook struct {
	onFetchRecordBuffered func(*kgo.Record)
}

func (h *testHook) OnBrokerConnect(meta kgo.BrokerMetadata, dialDur time.Duration, conn any, err error) {
}
func (h *testHook) OnBrokerDisconnect(meta kgo.BrokerMetadata, conn any) {}
func (h *testHook) OnBrokerWrite(meta kgo.BrokerMetadata, key int16, bytesWritten int, writeWait, timeToWrite time.Duration, err error) {
}
func (h *testHook) OnBrokerRead(meta kgo.BrokerMetadata, key int16, bytesRead int, readWait, timeToRead time.Duration, err error) {
}
func (h *testHook) OnBrokerThrottle(meta kgo.BrokerMetadata, throttleInterval time.Duration, throttledAfterResponse bool) {
}
func (h *testHook) OnProduceBatchWritten(meta kgo.BrokerMetadata, topic string, partition int32, metrics kgo.ProduceBatchMetrics) {
}
func (h *testHook) OnFetchRecordBuffered(r *kgo.Record) {
	if h.onFetchRecordBuffered != nil {
		h.onFetchRecordBuffered(r)
	}
}
func (h *testHook) OnFetchRecordUnbuffered(r *kgo.Record) {}
func (h *testHook) OnPartitionsAssigned(ctx context.Context, c *kgo.Client, assigned map[string][]int32) {
}
func (h *testHook) OnPartitionsRevoked(ctx context.Context, c *kgo.Client, revoked map[string][]int32) {
}
func (h *testHook) OnPartitionsLost(ctx context.Context, c *kgo.Client, lost map[string][]int32) {}
func (h *testHook) OnFetchBatchRead(meta kgo.BrokerMetadata, topic string, partition int32, metrics kgo.FetchBatchMetrics) {
}
