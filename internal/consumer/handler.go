// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/xmidt-org/wrp-go/v5"
	"github.com/xmidt-org/wrpkafka"
)

// MessageHandler defines the interface for handling Kafka messages.
// Implementations should process the message and return an error if processing fails.
// When an error is returned, the offset is NOT committed, allowing for retry.
// When nil is returned, the offset is committed.  Malformed messages should
// not return an error.
//
// The provided context includes cancellation signals and tracing information.
// Handlers should respect context cancellation for graceful shutdown.
type MessageHandler interface {
	// HandleMessage processes a single Kafka message.
	// Returns nil on successful processing, or an error if processing fails.
	HandleMessage(ctx context.Context, record *kgo.Record) error
}

// MessageHandlerFunc is a function adapter that implements MessageHandler.
type MessageHandlerFunc func(ctx context.Context, record *kgo.Record) error

// HandleMessage implements the MessageHandler interface.
func (f MessageHandlerFunc) HandleMessage(ctx context.Context, record *kgo.Record) error {
	return f(ctx, record)
}

// WRPProducer defines the interface for producing WRP messages to Kafka topics.
type WRPProducer interface {
	Produce(ctx context.Context, msg *wrp.Message) (wrpkafka.Outcome, error)
	Start() error
	Stop(ctx context.Context)
}

// WRPMessageHandler implements MessageHandler for routing WRP messages to different Kafka topics.
type WRPMessageHandler struct {
	publisher *wrpkafka.Publisher
	logger    *slog.Logger

	// metrics counters
	messagesProcessed int64
	messagesRouted    int64
	routingErrors     int64
	decodingErrors    int64
}

// WRPMessageHandlerConfig contains configuration for the WRP message handler.
type WRPMessageHandlerConfig struct {
	Publisher *wrpkafka.Publisher
	Logger    *slog.Logger
}

// NewWRPMessageHandler creates a new WRP message handler with the given configuration.
func NewWRPMessageHandler(config WRPMessageHandlerConfig) *WRPMessageHandler {
	if config.Logger == nil {
		config.Logger = slog.Default()
	}

	return &WRPMessageHandler{
		publisher: config.Publisher,
		logger:    config.Logger,
	}
}

// HandleMessage processes a Kafka message containing a WRP message and routes it
// to the appropriate topic
func (h *WRPMessageHandler) HandleMessage(ctx context.Context, record *kgo.Record) error {
	h.messagesProcessed++

	logger := h.logger.With("component", "wrp_message_handler", "source_topic")

	// Decode WRP message
	var msg wrp.Message
	if err := wrp.NewDecoderBytes(record.Value, wrp.Msgpack).Decode(&msg); err != nil {
		h.decodingErrors++
		// Don't return error for malformed messages - just log and continue
		logger.Warn("failed to decode WRP message",
			"error", err,
			"record_size", len(record.Value))
		return nil
	}

	// Log message details
	logger = logger.With("msg_type", msg.Type.String(), "source", msg.Source, "destination", msg.Destination, "transaction_uuid", msg.TransactionUUID)

	logger.Debug("processing WRP message")

	// Use wrpkafka publisher to route the message
	outcome, err := h.publisher.Produce(ctx, &msg)
	if err != nil {
		h.routingErrors++
		logger.Error("failed to produce WRP message", "error", err)
		return fmt.Errorf("production failed: %w", err)
	}

	h.messagesRouted++
	logger.Info("successfully routed WRP message", "outcome", outcome.String())

	return nil
}

// Close shuts down the message handler and its publisher.
func (h *WRPMessageHandler) Close(ctx context.Context) error {
	if h.publisher != nil {
		h.publisher.Stop(ctx)
	}
	return nil
}

// GetMetrics returns current handler metrics.
func (h *WRPMessageHandler) GetMetrics() map[string]int64 {
	return map[string]int64{
		"messages_processed": h.messagesProcessed,
		"messages_routed":    h.messagesRouted,
		"routing_errors":     h.routingErrors,
		"decoding_errors":    h.decodingErrors,
	}
}

// CreateWRPMessageHandler creates a WRP message handler function.
func CreateWRPMessageHandler(brokers []string, topicRoutes []wrpkafka.TopicRoute, logger *slog.Logger) (MessageHandlerFunc, error) {
	if logger == nil {
		logger = slog.Default()
	}

	// Create wrpkafka publisher with configs
	publisher := &wrpkafka.Publisher{
		Brokers: brokers,
		InitialDynamicConfig: wrpkafka.DynamicConfig{
			TopicMap: topicRoutes,
		},
		// Use BasicLogger adapter for kgo.Logger interface
		Logger: kgo.BasicLogger(os.Stderr, kgo.LogLevelInfo, func() string { return "wrpkafka: " }),
	}

	// Start the publisher
	if err := publisher.Start(); err != nil {
		return nil, fmt.Errorf("failed to start wrpkafka publisher: %w", err)
	}

	// Create the handler with wrpkafka publisher and logger
	handler := NewWRPMessageHandler(WRPMessageHandlerConfig{
		Publisher: publisher,
		Logger:    logger,
	})

	// Return as MessageHandlerFunc
	return MessageHandlerFunc(handler.HandleMessage), nil
}
