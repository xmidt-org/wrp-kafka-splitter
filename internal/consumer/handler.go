// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package consumer

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
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
	HandleMessage(ctx context.Context, record *kgo.Record) (wrpkafka.Outcome, error)
}

// MessageHandlerFunc is a function adapter that implements MessageHandler.
type MessageHandlerFunc func(ctx context.Context, record *kgo.Record) (wrpkafka.Outcome, error)

// HandleMessage implements the MessageHandler interface.
func (f MessageHandlerFunc) HandleMessage(ctx context.Context, record *kgo.Record) (wrpkafka.Outcome, error) {
	return f(ctx, record)
}
