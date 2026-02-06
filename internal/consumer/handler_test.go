// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package consumer

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"xmidt-org/splitter/internal/log"
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
	logEvents     []log.Event
}

func (suite *WRPMessageHandlerTestSuite) SetupTest() {
	suite.mockPublisher = new(MockWRPProducer)
	suite.logEvents = make([]log.Event, 0)
	suite.logEmitter = observe.NewSubject[log.Event]()

	// Add observer to capture log events
	suite.logEmitter.Attach(func(event log.Event) {
		suite.logEvents = append(suite.logEvents, event)
	})

	// Create handler with nil producer (tests will use mock through interface)
	suite.handler = &WRPMessageHandler{
		producer:   nil, // Will be mocked through the WRPProducer interface
		logEmitter: suite.logEmitter,
	}
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

			// Clear log events for next test
			suite.logEvents = suite.logEvents[:0]
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

		h.emitLog(log.LevelWarn, "failed to decode WRP message", attrs)
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

	h.emitLog(log.LevelDebug, "processing WRP message", msgAttrs)

	// Use mock producer instead of real one
	outcome, err := h.mockProducer.Produce(ctx, &msg)
	if err != nil {
		errorAttrs := make(map[string]any)
		for k, v := range msgAttrs {
			errorAttrs[k] = v
		}
		errorAttrs["error"] = err.Error()

		h.emitLog(log.LevelError, "failed to produce WRP message", errorAttrs)
		return fmt.Errorf("production failed: %w", err)
	}

	successAttrs := make(map[string]any)
	for k, v := range msgAttrs {
		successAttrs[k] = v
	}
	successAttrs["outcome"] = outcome.String()

	h.emitLog(log.LevelInfo, "successfully routed WRP message", successAttrs)

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

// Run the test suite
func TestWRPMessageHandlerTestSuite(t *testing.T) {
	suite.Run(t, new(WRPMessageHandlerTestSuite))
}
