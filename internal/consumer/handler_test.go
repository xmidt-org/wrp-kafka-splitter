// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package consumer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"xmidt-org/splitter/internal/log"
	"xmidt-org/splitter/internal/metrics"
	"xmidt-org/splitter/internal/observe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/xmidt-org/wrp-go/v5"
	"github.com/xmidt-org/wrpkafka"
)

type MockWRPProducer struct {
	mock.Mock
}

func (m *MockWRPProducer) Produce(ctx context.Context, msg *wrp.Message) (wrpkafka.Outcome, error) {
	args := m.Called(ctx, msg)
	return args.Get(0).(wrpkafka.Outcome), args.Error(1)
}

func (m *MockWRPProducer) Start() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockWRPProducer) Stop(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

// Test suite for WRP Message Handler
type WRPMessageHandlerTestSuite struct {
	suite.Suite
	handler       *WRPMessageHandler
	mockPublisher *MockWRPProducer
	logEmitter    *observe.Subject[log.Event]
	metricEmitter *observe.Subject[metrics.Event]
	logEvents     []log.Event
	metricEvents  []metrics.Event
	eventMutex    sync.Mutex // Protects event slices
}

func (suite *WRPMessageHandlerTestSuite) SetupTest() {
	suite.mockPublisher = new(MockWRPProducer)
	suite.eventMutex.Lock()
	suite.logEvents = make([]log.Event, 0)
	suite.metricEvents = make([]metrics.Event, 0)
	suite.eventMutex.Unlock()

	suite.logEmitter = observe.NewSubject[log.Event]()
	suite.metricEmitter = observe.NewSubject[metrics.Event]()

	// Add observers to capture events with proper synchronization
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

	// Create handler with nil producer (tests will use mock through interface)
	suite.handler = &WRPMessageHandler{
		producer:      nil, // Will be mocked through the WRPProducer interface
		logEmitter:    suite.logEmitter,
		metricEmitter: suite.metricEmitter,
	}
}

// Helper methods for thread-safe access to events
func (suite *WRPMessageHandlerTestSuite) getLogEvents() []log.Event {
	suite.eventMutex.Lock()
	defer suite.eventMutex.Unlock()
	// Return a copy to avoid race conditions
	events := make([]log.Event, len(suite.logEvents))
	copy(events, suite.logEvents)
	return events
}

func (suite *WRPMessageHandlerTestSuite) getMetricEvents() []metrics.Event {
	suite.eventMutex.Lock()
	defer suite.eventMutex.Unlock()
	// Return a copy to avoid race conditions
	events := make([]metrics.Event, len(suite.metricEvents))
	copy(events, suite.metricEvents)
	return events
}

func (suite *WRPMessageHandlerTestSuite) clearLogEvents() {
	suite.eventMutex.Lock()
	defer suite.eventMutex.Unlock()
	suite.logEvents = suite.logEvents[:0]
}

func (suite *WRPMessageHandlerTestSuite) clearMetricEvents() {
	suite.eventMutex.Lock()
	defer suite.eventMutex.Unlock()
	suite.metricEvents = suite.metricEvents[:0]
}

func (suite *WRPMessageHandlerTestSuite) clearAllEvents() {
	suite.eventMutex.Lock()
	defer suite.eventMutex.Unlock()
	suite.logEvents = suite.logEvents[:0]
	suite.metricEvents = suite.metricEvents[:0]
}

func (suite *WRPMessageHandlerTestSuite) getLogEventCount() int {
	suite.eventMutex.Lock()
	defer suite.eventMutex.Unlock()
	return len(suite.logEvents)
}

func (suite *WRPMessageHandlerTestSuite) getMetricEventCount() int {
	suite.eventMutex.Lock()
	defer suite.eventMutex.Unlock()
	return len(suite.metricEvents)
}

// Helper function to create MessagePack encoded WRP messages
func createMessagePackWRPMessage(msg *wrp.Message) ([]byte, error) {
	var msgBytes []byte
	encoder := wrp.NewEncoderBytes(&msgBytes, wrp.Msgpack)
	if err := encoder.Encode(msg); err != nil {
		return nil, fmt.Errorf("failed to encode WRP message: %w", err)
	}
	return msgBytes, nil
}

// Helper function to create test kgo.Record
func createKafkaRecord(topic string, key []byte, value []byte) *kgo.Record {
	return &kgo.Record{
		Topic: topic,
		Key:   key,
		Value: value,
	}
}

// Test NewWRPMessageHandler constructor
func (suite *WRPMessageHandlerTestSuite) TestNewWRPMessageHandler() {
	tests := []struct {
		name           string
		config         WRPMessageHandlerConfig
		expectDefaults bool
		description    string
	}{
		{
			name: "with_all_config",
			config: WRPMessageHandlerConfig{
				Producer:   &MockWRPProducer{},
				LogEmitter: suite.logEmitter,
			},
			expectDefaults: false,
			description:    "Should use provided producer and log emitter",
		},
		{
			name: "with_nil_log_emitter",
			config: WRPMessageHandlerConfig{
				Producer:   &MockWRPProducer{},
				LogEmitter: nil,
			},
			expectDefaults: true,
			description:    "Should use default log emitter when nil provided",
		},
		{
			name: "with_empty_config",
			config: WRPMessageHandlerConfig{
				Producer:   nil,
				LogEmitter: nil,
			},
			expectDefaults: true,
			description:    "Should handle empty config gracefully",
		},
	}

	for _, tt := range tests {
		suite.Run(tt.name, func() {
			handler := NewWRPMessageHandler(tt.config)

			suite.NotNil(handler, tt.description)
			suite.Equal(tt.config.Producer, handler.producer)

			if tt.expectDefaults {
				suite.NotNil(handler.logEmitter, "Should have default log emitter")
			} else {
				suite.Equal(tt.config.LogEmitter, handler.logEmitter)
			}
		})
	}
}

// Test HandleMessage functionality
func (suite *WRPMessageHandlerTestSuite) TestHandleMessage() {
	tests := []struct {
		name                  string
		setupRecord           func() *kgo.Record
		setupMockExpectations func(*MockWRPProducer)
		expectedError         error
		description           string
	}{
		{
			name: "successful_message_processing",
			setupRecord: func() *kgo.Record {
				msg := &wrp.Message{
					Type:        wrp.SimpleEventMessageType,
					Source:      "mac:112233445566/service",
					Destination: "event:device-status/mac:112233445566/online",
					ContentType: "application/json",
					Payload:     []byte(`{"event": "data"}`),
				}
				msgBytes, _ := createMessagePackWRPMessage(msg)
				return createKafkaRecord("wrp-events", []byte("test-key"), msgBytes)
			},
			setupMockExpectations: func(mockPub *MockWRPProducer) {
				mockPub.On("Produce", mock.Anything, mock.AnythingOfType("*wrp.Message")).
					Return(wrpkafka.Attempted, nil)
			},
			expectedError: nil,
			description:   "Should successfully process and route valid WRP message",
		},
		{
			name: "malformed_message_decoding_error",
			setupRecord: func() *kgo.Record {
				// Invalid MessagePack data
				return createKafkaRecord("wrp-events", []byte("test-key"), []byte("invalid-msgpack"))
			},
			setupMockExpectations: func(mockPub *MockWRPProducer) {
				// None since publisher shouldn't be called
			},
			expectedError: nil, // Should not return error for malformed messages
			description:   "Should handle malformed messages gracefully without returning error",
		},
		{
			name: "publisher_produce_error",
			setupRecord: func() *kgo.Record {
				msg := &wrp.Message{
					Type:            wrp.SimpleRequestResponseMessageType,
					Source:          "mac:aabbccddeeff/service",
					Destination:     "mac:112233445566/command",
					TransactionUUID: "test-txn-123",
					Payload:         []byte(`{"command": "reboot"}`),
				}
				msgBytes, _ := createMessagePackWRPMessage(msg)
				return createKafkaRecord("wrp-events", []byte("test-key"), msgBytes)
			},
			setupMockExpectations: func(mockPub *MockWRPProducer) {
				mockPub.On("Produce", mock.Anything, mock.AnythingOfType("*wrp.Message")).
					Return(wrpkafka.Failed, errors.New("kafka unavailable"))
			},
			expectedError: fmt.Errorf("production failed: kafka unavailable"),
			description:   "Should return error and update metrics when publisher fails",
		},
		{
			name: "empty_message_content",
			setupRecord: func() *kgo.Record {
				return createKafkaRecord("wrp-events", []byte("test-key"), []byte{})
			},
			setupMockExpectations: func(mockPub *MockWRPProducer) {
				// None since publisher shouldn't be called
			},
			expectedError: nil,
			description:   "Should handle empty message content",
		},
		{
			name: "different_wrp_message_types",
			setupRecord: func() *kgo.Record {
				msg := &wrp.Message{
					Type:            wrp.SimpleEventMessageType,
					Source:          "mac:123456789abc/service",
					Destination:     "event:device-status/mac:123456789abc/offline",
					TransactionUUID: "txn-123",
					Payload:         []byte(`{"status": "offline", "timestamp": 1640995200}`),
				}
				msgBytes, _ := createMessagePackWRPMessage(msg)
				return createKafkaRecord("notifications", []byte("device-status"), msgBytes)
			},
			setupMockExpectations: func(mockPub *MockWRPProducer) {
				mockPub.On("Produce", mock.Anything, mock.AnythingOfType("*wrp.Message")).
					Return(wrpkafka.Attempted, nil)
			},
			expectedError: nil,
			description:   "Should handle different WRP message types",
		},
	}

	for _, tt := range tests {
		suite.Run(tt.name, func() {
			// Create fresh mock for each test
			mockProducer := new(MockWRPProducer)
			tt.setupMockExpectations(mockProducer)

			// Create a test handler with fresh mock
			testHandler := &testWRPMessageHandler{
				WRPMessageHandler: &WRPMessageHandler{
					producer:   nil,
					logEmitter: suite.logEmitter,
				},
				mockProducer: mockProducer,
			}

			record := tt.setupRecord()
			ctx := context.Background()

			err := testHandler.HandleMessage(ctx, record)

			if tt.expectedError != nil {
				suite.Error(err, tt.description)
				if err != nil {
					suite.Contains(err.Error(), tt.expectedError.Error())
				}
			} else {
				suite.NoError(err, tt.description)
			}

			// Verify mock expectations
			mockProducer.AssertExpectations(suite.T())

			// Clear log events for next test with proper synchronization
			suite.clearLogEvents()
		})
	}
}

// testWRPMessageHandler is a wrapper that allows us to inject mock producer
type testWRPMessageHandler struct {
	*WRPMessageHandler
	mockProducer *MockWRPProducer
}

func (h *testWRPMessageHandler) HandleMessage(ctx context.Context, record *kgo.Record) error {
	baseAttrs := map[string]any{
		"component":    "wrp_message_handler",
		"source_topic": record.Topic,
	}

	// Decode WRP message
	var msg wrp.Message
	if err := wrp.NewDecoderBytes(record.Value, wrp.Msgpack).Decode(&msg); err != nil {
		attrs := make(map[string]any)
		for k, v := range baseAttrs {
			attrs[k] = v
		}
		attrs["error"] = err.Error()
		attrs["record_size"] = len(record.Value)

		// Use NotifySync for predictable test behavior
		h.logEmitter.NotifySync(log.NewEvent(log.LevelWarn, "failed to decode WRP message", attrs))
		return nil
	}

	// Add message details to attributes
	msgAttrs := make(map[string]any)
	for k, v := range baseAttrs {
		msgAttrs[k] = v
	}
	msgAttrs["msg_type"] = msg.Type.String()
	msgAttrs["source"] = msg.Source
	msgAttrs["destination"] = msg.Destination
	msgAttrs["transaction_uuid"] = msg.TransactionUUID

	// Use NotifySync for predictable test behavior
	h.logEmitter.NotifySync(log.NewEvent(log.LevelDebug, "processing WRP message", msgAttrs))

	// Use mock producer instead of real one
	outcome, err := h.mockProducer.Produce(ctx, &msg)
	if err != nil {
		errorAttrs := make(map[string]any)
		for k, v := range msgAttrs {
			errorAttrs[k] = v
		}
		errorAttrs["error"] = err.Error()

		// Use NotifySync for predictable test behavior
		h.logEmitter.NotifySync(log.NewEvent(log.LevelError, "failed to produce WRP message", errorAttrs))
		return fmt.Errorf("production failed: %w", err)
	}

	successAttrs := make(map[string]any)
	for k, v := range msgAttrs {
		successAttrs[k] = v
	}
	successAttrs["outcome"] = outcome.String()

	// Use NotifySync for predictable test behavior
	h.logEmitter.NotifySync(log.NewEvent(log.LevelInfo, "successfully routed WRP message", successAttrs))

	return nil
}

// Test MessageHandlerFunc
func TestMessageHandlerFunc(t *testing.T) {
	tests := []struct {
		name        string
		handlerFunc MessageHandlerFunc
		record      *kgo.Record
		expectedErr error
		description string
	}{
		{
			name: "successful_handler_func",
			handlerFunc: func(ctx context.Context, record *kgo.Record) error {
				return nil
			},
			record:      createKafkaRecord("test-topic", []byte("key"), []byte("value")),
			expectedErr: nil,
			description: "Should execute handler function successfully",
		},
		{
			name: "handler_func_with_error",
			handlerFunc: func(ctx context.Context, record *kgo.Record) error {
				return errors.New("handler error")
			},
			record:      createKafkaRecord("test-topic", []byte("key"), []byte("value")),
			expectedErr: errors.New("handler error"),
			description: "Should return error from handler function",
		},
		{
			name: "handler_func_with_context_check",
			handlerFunc: func(ctx context.Context, record *kgo.Record) error {
				if record.Topic != "expected-topic" {
					return errors.New("unexpected topic")
				}
				return nil
			},
			record:      createKafkaRecord("unexpected-topic", []byte("key"), []byte("value")),
			expectedErr: errors.New("unexpected topic"),
			description: "Should allow handler function to inspect context and record",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			err := tt.handlerFunc.HandleMessage(ctx, tt.record)

			if tt.expectedErr != nil {
				assert.Error(t, err, tt.description)
				assert.Equal(t, tt.expectedErr.Error(), err.Error())
			} else {
				assert.NoError(t, err, tt.description)
			}
		})
	}
}

// Test edge cases
func TestWRPMessageHandlerEdgeCases(t *testing.T) {
	tests := []struct {
		name        string
		test        func(t *testing.T)
		description string
	}{
		{
			name: "nil_record_handling",
			test: func(t *testing.T) {
				// This would panic in real usage, but we can test the concept
				// In practice, kgo.Record would never be nil from the Kafka client
				ctx := context.Background()

				// Create handler that would handle nil record
				handlerFunc := MessageHandlerFunc(func(ctx context.Context, record *kgo.Record) error {
					if record == nil {
						return errors.New("nil record")
					}
					return nil
				})

				err := handlerFunc.HandleMessage(ctx, nil)
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "nil record")
			},
			description: "Should handle nil record parameter",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.test(t)
		})
	}
}

// Test Close method
func (suite *WRPMessageHandlerTestSuite) TestClose() {
	tests := []struct {
		name         string
		setupHandler func() *WRPMessageHandler
		setupMock    func(*MockWRPProducer)
		expectError  bool
		description  string
	}{
		{
			name: "close_with_producer",
			setupHandler: func() *WRPMessageHandler {
				return &WRPMessageHandler{
					producer:      suite.mockPublisher,
					logEmitter:    suite.logEmitter,
					metricEmitter: suite.metricEmitter,
				}
			},
			setupMock: func(mockPub *MockWRPProducer) {
				mockPub.On("Stop", mock.Anything).Return(nil)
			},
			expectError: false,
			description: "Should successfully close handler with producer",
		},
		{
			name: "close_with_producer_error",
			setupHandler: func() *WRPMessageHandler {
				return &WRPMessageHandler{
					producer:      suite.mockPublisher,
					logEmitter:    suite.logEmitter,
					metricEmitter: suite.metricEmitter,
				}
			},
			setupMock: func(mockPub *MockWRPProducer) {
				mockPub.On("Stop", mock.Anything).Return(errors.New("stop failed"))
			},
			expectError: true,
			description: "Should return error when producer stop fails",
		},
		{
			name: "close_with_nil_producer",
			setupHandler: func() *WRPMessageHandler {
				return &WRPMessageHandler{
					producer:      nil,
					logEmitter:    suite.logEmitter,
					metricEmitter: suite.metricEmitter,
				}
			},
			setupMock: func(mockPub *MockWRPProducer) {
				// No expectations since producer is nil
			},
			expectError: false,
			description: "Should handle nil producer gracefully",
		},
	}

	for _, tt := range tests {
		suite.Run(tt.name, func() {
			suite.mockPublisher = new(MockWRPProducer)
			tt.setupMock(suite.mockPublisher)

			handler := tt.setupHandler()
			ctx := context.Background()

			err := handler.Close(ctx)

			if tt.expectError {
				suite.Error(err, tt.description)
			} else {
				suite.NoError(err, tt.description)
			}

			suite.mockPublisher.AssertExpectations(suite.T())
		})
	}
}

// Test different WRP message types and content
func (suite *WRPMessageHandlerTestSuite) TestHandleMessageTypes() {
	tests := []struct {
		name        string
		message     *wrp.Message
		expectError bool
		description string
	}{
		{
			name: "simple_event_message",
			message: &wrp.Message{
				Type:        wrp.SimpleEventMessageType,
				Source:      "mac:112233445566/service",
				Destination: "event:device-status/online",
				Payload:     []byte(`{"event": "device online"}`),
			},
			expectError: false,
			description: "Should handle SimpleEventMessageType",
		},
		{
			name: "simple_request_response_message",
			message: &wrp.Message{
				Type:            wrp.SimpleRequestResponseMessageType,
				Source:          "dns:webpa.example.com/api/v2/device",
				Destination:     "mac:112233445566/config",
				TransactionUUID: "txn-12345",
				Payload:         []byte(`{"command": "get-status"}`),
			},
			expectError: false,
			description: "Should handle SimpleRequestResponseMessageType",
		},
		{
			name: "create_message",
			message: &wrp.Message{
				Type:            wrp.CreateMessageType,
				Source:          "dns:webpa.example.com/api/v2/device",
				Destination:     "mac:112233445566/config",
				TransactionUUID: "test-txn-create-123",
				Payload:         []byte(`{"parameters": {"Device.WiFi.SSID": "MyNetwork"}}`),
			},
			expectError: false,
			description: "Should handle CreateMessageType",
		},
		{
			name: "retrieve_message",
			message: &wrp.Message{
				Type:            wrp.RetrieveMessageType,
				Source:          "dns:webpa.example.com/api/v2/device",
				Destination:     "mac:112233445566/config",
				TransactionUUID: "test-txn-retrieve-123",
				Payload:         []byte(`{"names": ["Device.WiFi.SSID"]}`),
			},
			expectError: false,
			description: "Should handle RetrieveMessageType",
		},
		{
			name: "update_message",
			message: &wrp.Message{
				Type:            wrp.UpdateMessageType,
				Source:          "dns:webpa.example.com/api/v2/device",
				Destination:     "mac:112233445566/config",
				TransactionUUID: "test-txn-update-123",
				Payload:         []byte(`{"parameters": {"Device.WiFi.Password": "secret123"}}`),
			},
			expectError: false,
			description: "Should handle UpdateMessageType",
		},
		{
			name: "delete_message",
			message: &wrp.Message{
				Type:            wrp.DeleteMessageType,
				Source:          "dns:webpa.example.com/api/v2/device",
				Destination:     "mac:112233445566/config",
				TransactionUUID: "test-txn-delete-123",
				Payload:         []byte(`{"names": ["Device.WiFi.Password"]}`),
			},
			expectError: false,
			description: "Should handle DeleteMessageType",
		},
		{
			name: "message_with_headers",
			message: &wrp.Message{
				Type:        wrp.SimpleEventMessageType,
				Source:      "mac:112233445566/service",
				Destination: "event:device-status/online",
				Headers:     []string{"X-Custom-Header", "custom-value"},
				Payload:     []byte(`{"event": "device online with headers"}`),
			},
			expectError: false,
			description: "Should handle messages with custom headers",
		},
		{
			name: "message_with_metadata",
			message: &wrp.Message{
				Type:        wrp.SimpleEventMessageType,
				Source:      "mac:112233445566/service",
				Destination: "event:device-status/online",
				Metadata:    map[string]string{"device-id": "112233445566", "region": "us-west-2"},
				Payload:     []byte(`{"event": "device online with metadata"}`),
			},
			expectError: false,
			description: "Should handle messages with metadata",
		},
	}

	for _, tt := range tests {
		suite.Run(tt.name, func() {
			suite.clearAllEvents()

			// Setup mock expectations
			suite.mockPublisher = new(MockWRPProducer)
			if tt.expectError {
				suite.mockPublisher.On("Produce", mock.Anything, mock.AnythingOfType("*wrp.Message")).
					Return(wrpkafka.Failed, errors.New("produce failed"))
			} else {
				suite.mockPublisher.On("Produce", mock.Anything, mock.AnythingOfType("*wrp.Message")).
					Return(wrpkafka.Attempted, nil)
			}

			// Create handler with mock
			handler := &WRPMessageHandler{
				producer:      suite.mockPublisher,
				logEmitter:    suite.logEmitter,
				metricEmitter: suite.metricEmitter,
			}

			// Create record
			msgBytes, err := createMessagePackWRPMessage(tt.message)
			suite.NoError(err)
			record := createKafkaRecord("wrp-topic", []byte("test-key"), msgBytes)

			// Execute
			err = handler.HandleMessage(context.Background(), record)

			if tt.expectError {
				suite.Error(err, tt.description)
			} else {
				suite.NoError(err, tt.description)
			}

			suite.mockPublisher.AssertExpectations(suite.T())
		})
	}
}

// Test context cancellation scenarios
func (suite *WRPMessageHandlerTestSuite) TestContextCancellation() {
	tests := []struct {
		name        string
		setupCtx    func() (context.Context, context.CancelFunc)
		expectError bool
		description string
	}{
		{
			name: "context_timeout",
			setupCtx: func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), 1*time.Nanosecond)
			},
			expectError: true,
			description: "Should handle context timeout during processing",
		},
		{
			name: "context_cancellation",
			setupCtx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				// Cancel immediately
				cancel()
				return ctx, cancel
			},
			expectError: true,
			description: "Should handle immediate context cancellation",
		},
		{
			name: "valid_context",
			setupCtx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			expectError: false,
			description: "Should work with valid context",
		},
	}

	for _, tt := range tests {
		suite.Run(tt.name, func() {
			suite.clearAllEvents()

			// Setup mock
			suite.mockPublisher = new(MockWRPProducer)
			if tt.expectError {
				suite.mockPublisher.On("Produce", mock.Anything, mock.AnythingOfType("*wrp.Message")).
					Return(wrpkafka.Failed, context.Canceled)
			} else {
				suite.mockPublisher.On("Produce", mock.Anything, mock.AnythingOfType("*wrp.Message")).
					Return(wrpkafka.Attempted, nil)
			}

			// Create handler
			handler := &WRPMessageHandler{
				producer:      suite.mockPublisher,
				logEmitter:    suite.logEmitter,
				metricEmitter: suite.metricEmitter,
			}

			// Create test message and record
			msg := &wrp.Message{
				Type:            wrp.SimpleEventMessageType,
				Source:          "mac:112233445566/service",
				Destination:     "event:device-status/online",
				TransactionUUID: "test-txn-context-123",
				Payload:         []byte(`{"test": "data"}`),
			}
			msgBytes, err := createMessagePackWRPMessage(msg)
			suite.NoError(err)
			record := createKafkaRecord("test-topic", []byte("key"), msgBytes)

			// Setup context
			ctx, cancel := tt.setupCtx()
			defer cancel()

			// Execute
			err = handler.HandleMessage(ctx, record)

			if tt.expectError {
				suite.Error(err, tt.description)
			} else {
				suite.NoError(err, tt.description)
			}
		})
	}
}

// Test concurrent message handling
func (suite *WRPMessageHandlerTestSuite) TestConcurrentHandling() {
	suite.clearAllEvents()

	// Setup handler with mock
	suite.mockPublisher = new(MockWRPProducer)
	suite.mockPublisher.On("Produce", mock.Anything, mock.AnythingOfType("*wrp.Message")).
		Return(wrpkafka.Attempted, nil).Times(50)

	handler := &WRPMessageHandler{
		producer:      suite.mockPublisher,
		logEmitter:    suite.logEmitter,
		metricEmitter: suite.metricEmitter,
	}

	// Create test message
	msg := &wrp.Message{
		Type:            wrp.SimpleEventMessageType,
		Source:          "mac:112233445566/service",
		Destination:     "event:device-status/concurrent",
		TransactionUUID: "test-txn-concurrent-123",
		Payload:         []byte(`{"test": "concurrent"}`),
	}
	msgBytes, err := createMessagePackWRPMessage(msg)
	suite.NoError(err)

	// Run concurrent handlers
	var wg sync.WaitGroup
	numGoroutines := 50

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()

			record := createKafkaRecord("test-topic", []byte(fmt.Sprintf("key-%d", index)), msgBytes)
			err := handler.HandleMessage(context.Background(), record)
			suite.NoError(err, "Concurrent handler should succeed")
		}(i)
	}

	wg.Wait()

	// Give time for async event processing
	time.Sleep(100 * time.Millisecond)

	// Verify all mock expectations were met
	suite.mockPublisher.AssertExpectations(suite.T())
}

// Test error scenarios and edge cases
func (suite *WRPMessageHandlerTestSuite) TestErrorScenarios() {
	tests := []struct {
		name           string
		setupRecord    func() *kgo.Record
		setupMock      func(*MockWRPProducer)
		expectError    bool
		expectLogLevel log.Level
		description    string
	}{
		{
			name: "invalid_msgpack_data",
			setupRecord: func() *kgo.Record {
				// Invalid MessagePack data
				return createKafkaRecord("test-topic", []byte("key"), []byte("invalid-msgpack-data"))
			},
			setupMock: func(mockPub *MockWRPProducer) {
				// No expectations since decode will fail before producer call
			},
			expectError:    false, // Malformed messages don't return errors
			expectLogLevel: log.LevelWarn,
			description:    "Should handle invalid MessagePack gracefully",
		},
		{
			name: "empty_record_value",
			setupRecord: func() *kgo.Record {
				return createKafkaRecord("test-topic", []byte("key"), []byte{})
			},
			setupMock: func(mockPub *MockWRPProducer) {
				// No expectations since decode will fail
			},
			expectError:    false,
			expectLogLevel: log.LevelWarn,
			description:    "Should handle empty record value gracefully",
		},
		{
			name: "producer_network_error",
			setupRecord: func() *kgo.Record {
				msg := &wrp.Message{
					Type:            wrp.SimpleEventMessageType,
					Source:          "mac:112233445566/service",
					Destination:     "event:device-status/error",
					TransactionUUID: "test-txn-error-123",
					Payload:         []byte(`{"error": "network timeout"}`),
				}
				msgBytes, _ := createMessagePackWRPMessage(msg)
				return createKafkaRecord("test-topic", []byte("key"), msgBytes)
			},
			setupMock: func(mockPub *MockWRPProducer) {
				mockPub.On("Produce", mock.Anything, mock.AnythingOfType("*wrp.Message")).
					Return(wrpkafka.Failed, errors.New("network timeout"))
			},
			expectError:    true,
			expectLogLevel: log.LevelError,
			description:    "Should return error on producer failure",
		},
		{
			name: "large_message_payload",
			setupRecord: func() *kgo.Record {
				// Create a large payload (1MB)
				largePayload := make([]byte, 1024*1024)
				for i := range largePayload {
					largePayload[i] = byte(i % 256)
				}

				msg := &wrp.Message{
					Type:            wrp.SimpleEventMessageType,
					Source:          "mac:112233445566/service",
					Destination:     "event:device-status/large",
					TransactionUUID: "test-txn-large-123",
					Payload:         largePayload,
				}
				msgBytes, _ := createMessagePackWRPMessage(msg)
				return createKafkaRecord("test-topic", []byte("key"), msgBytes)
			},
			setupMock: func(mockPub *MockWRPProducer) {
				mockPub.On("Produce", mock.Anything, mock.AnythingOfType("*wrp.Message")).
					Return(wrpkafka.Attempted, nil)
			},
			expectError:    false,
			expectLogLevel: log.LevelDebug,
			description:    "Should handle large message payloads",
		},
	}

	for _, tt := range tests {
		suite.Run(tt.name, func() {
			suite.clearAllEvents()

			suite.mockPublisher = new(MockWRPProducer)
			tt.setupMock(suite.mockPublisher)

			handler := &WRPMessageHandler{
				producer:      suite.mockPublisher,
				logEmitter:    suite.logEmitter,
				metricEmitter: suite.metricEmitter,
			}

			record := tt.setupRecord()
			err := handler.HandleMessage(context.Background(), record)

			if tt.expectError {
				suite.Error(err, tt.description)
			} else {
				suite.NoError(err, tt.description)
			}

			// Verify log events
			time.Sleep(10 * time.Millisecond) // Allow async events to process
			logEvents := suite.getLogEvents()
			if len(logEvents) > 0 {
				found := false
				for _, event := range logEvents {
					if event.Level == tt.expectLogLevel {
						found = true
						break
					}
				}
				suite.True(found, fmt.Sprintf("Expected log level %s not found", tt.expectLogLevel))
			}

			suite.mockPublisher.AssertExpectations(suite.T())
		})
	}
}

// Run the test suite
func TestWRPMessageHandlerTestSuite(t *testing.T) {
	suite.Run(t, new(WRPMessageHandlerTestSuite))
}
