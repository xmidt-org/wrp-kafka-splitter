// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package consumer

import (
	"context"
	"fmt"

	"xmidt-org/splitter/internal/log"
	"xmidt-org/splitter/internal/metrics"
	"xmidt-org/splitter/internal/observe"

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
	Stop(ctx context.Context) error
}

// WRPMessageHandler implements MessageHandler for routing WRP messages to different Kafka topics.
type WRPMessageHandler struct {
	producer      WRPProducer
	logEmitter    *observe.Subject[log.Event]
	metricEmitter *observe.Subject[metrics.Event]
}

// WRPMessageHandlerConfig contains configuration for the WRP message handler.
type WRPMessageHandlerConfig struct {
	Producer       WRPProducer
	LogEmitter     *observe.Subject[log.Event]
	MetricsEmitter *observe.Subject[metrics.Event]
}

// NewWRPMessageHandler creates a new WRP message handler with the given configuration.
func NewWRPMessageHandler(config WRPMessageHandlerConfig) *WRPMessageHandler {
	logEmitter := config.LogEmitter
	if logEmitter == nil {
		logEmitter = log.NewNoop()
	}

	metricEmitter := config.MetricsEmitter
	if metricEmitter == nil {
		metricEmitter = metrics.NewNoop()
	}

	return &WRPMessageHandler{
		producer:      config.Producer,
		logEmitter:    logEmitter,
		metricEmitter: metricEmitter,
	}
}

// HandleMessage processes a Kafka message containing a WRP message and routes it
// to the appropriate topic
func (h *WRPMessageHandler) HandleMessage(ctx context.Context, record *kgo.Record) error {

	// Decode WRP message
	var msg wrp.Message
	if err := wrp.NewDecoderBytes(record.Value, wrp.Msgpack).Decode(&msg); err != nil {

		// Don't return error for malformed messages - just log and continue
		h.emitLog(log.LevelWarn, "decode WRP message", map[string]any{
			"error": err.Error(),
		})
		return nil
	}

	// Log the message being processed
	h.emitLog(log.LevelDebug, "processing WRP message", map[string]any{
		"msg_type":         msg.Type.String(),
		"source":           msg.Source,
		"destination":      msg.Destination,
		"transaction_uuid": msg.TransactionUUID,
	})

	// Use wrpkafka publisher to route the message
	outcome, err := h.producer.Produce(ctx, &msg)
	if err != nil {
		h.emitLog(log.LevelError, "failed to produce WRP message", map[string]any{
			"error": err.Error(),
		})
		return fmt.Errorf("production failed: %w", err)
	}

	h.emitLog(log.LevelInfo, "successfully routed WRP message", map[string]any{
		"outcome": outcome.String(),
	})

	return nil
}

// Close shuts down the message handler and its producer.
func (h *WRPMessageHandler) Close(ctx context.Context) error {
	if h.producer != nil {
		return h.producer.Stop(ctx)
	}
	return nil
}

// emitLog emits a log event to the configured emitter.
// The emitter is never nil (defaults to no-op if not configured).
func (h *WRPMessageHandler) emitLog(level log.Level, message string, attrs map[string]any) {
	h.logEmitter.Notify(log.NewEvent(level, message, attrs))
}
