// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package consumer

import (
	"context"
	"errors"
	"fmt"

	"xmidt-org/splitter/internal/bucket"
	"xmidt-org/splitter/internal/log"
	"xmidt-org/splitter/internal/metrics"
	"xmidt-org/splitter/internal/observe"
	"xmidt-org/splitter/internal/publisher"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/xmidt-org/wrp-go/v5"
	"github.com/xmidt-org/wrpkafka"
)

type Outcome int

const (
	Attempted Outcome = iota
	Queued
	Failed
	Accepted
	Skipped
	UnknownOutcome
)

// String returns the string representation of the Outcome.
func (o Outcome) String() string {
	switch o {
	case Accepted:
		return "Accepted"
	case Queued:
		return "Queued"
	case Attempted:
		return "Attempted"
	case Skipped:
		return "Skipped"
	case Failed:
		return "Failed"
	default:
		return "Unknown"
	}
}

var (
	ErrMalformedMsg = errors.New("malformed wrp message")
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
	HandleMessage(ctx context.Context, record *kgo.Record) (Outcome, error)
}

// MessageHandlerFunc is a function adapter that implements MessageHandler.
type MessageHandlerFunc func(ctx context.Context, record *kgo.Record) (Outcome, error)

// HandleMessage implements the MessageHandler interface.
func (f MessageHandlerFunc) HandleMessage(ctx context.Context, record *kgo.Record) (Outcome, error) {
	return f(ctx, record)
}

// WRPMessageHandler implements MessageHandler for routing WRP messages to different Kafka topics.
type WRPMessageHandler struct {
	producer      publisher.Publisher
	logEmitter    *observe.Subject[log.Event]
	metricEmitter *observe.Subject[metrics.Event]
	buckets       bucket.Bucket
}

// NewWRPMessageHandler creates a new WRP message handler with the given configuration.
func NewWRPMessageHandler(opts ...HandlerOption) (*WRPMessageHandler, error) {
	handler := &WRPMessageHandler{
		logEmitter:    log.NewNoop(),
		metricEmitter: metrics.NewNoop(),
	}

	// Apply all options to the handler
	for _, opt := range opts {
		if err := opt.apply(handler); err != nil {
			return nil, err
		}
	}

	err := validateHandler(handler)
	if err != nil {
		return nil, fmt.Errorf("invalid handler configuration: %w", err)
	}

	return handler, nil
}

// HandleMessage processes a Kafka message containing a WRP message and routes it
// to the appropriate topic
func (h *WRPMessageHandler) HandleMessage(ctx context.Context, record *kgo.Record) (Outcome, error) {

	// Decode WRP message
	var msg wrp.Message
	if err := wrp.NewDecoderBytes(record.Value, wrp.Msgpack).Decode(&msg); err != nil {

		// Don't return error for malformed messages - just log and continue
		h.emitLog(log.LevelWarn, "decode WRP message", map[string]any{
			"error": err.Error(),
		})
		return Failed, ErrMalformedMsg
	}

	// Log the message being processed
	h.emitLog(log.LevelDebug, "processing WRP message", map[string]any{
		"msg_type":         msg.Type.String(),
		"source":           msg.Source,
		"destination":      msg.Destination,
		"metadata":         msg.Metadata,
		"transaction_uuid": msg.TransactionUUID,
	})

	inTargetBucket, err := h.buckets.IsInTargetBucket(&msg)
	if err != nil {
		h.emitLog(log.LevelError, "failed to determine target bucket", map[string]any{
			"error": err.Error(),
		})
		// add a metric here for NoBucketKeyFound
	}

	if !inTargetBucket {
		h.emitLog(log.LevelDebug, "message not in target bucket, skipping", map[string]any{
			"source":      msg.Source,
			"destination": msg.Destination,
		})

		return Skipped, nil
	}

	// Use wrpkafka publisher to route the message
	outcome, err := h.producer.Produce(ctx, &msg)
	if err != nil {
		h.emitLog(log.LevelError, "failed to produce WRP message", map[string]any{
			"error": err.Error(),
		})
		return getOutcome(outcome), fmt.Errorf("produce failed: %w", err)
	}

	h.emitLog(log.LevelDebug, "successfully routed WRP message", map[string]any{
		"outcome": outcome,
	})

	return getOutcome(outcome), nil
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

// getOutcome converts wrpkafka.Outcome to Outcome.
func getOutcome(o wrpkafka.Outcome) Outcome {
	switch o {
	case wrpkafka.Attempted:
		return Attempted
	case wrpkafka.Failed:
		return Failed
	case wrpkafka.Queued:
		return Queued
	case wrpkafka.Accepted:
		return Accepted
	default:
		return UnknownOutcome
	}
}
