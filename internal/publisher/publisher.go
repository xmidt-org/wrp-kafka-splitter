// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package publisher

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"xmidt-org/splitter/internal/log"
	"xmidt-org/splitter/internal/metrics"
	"xmidt-org/splitter/internal/observe"

	"github.com/xmidt-org/wrp-go/v5"
	"github.com/xmidt-org/wrpkafka"
)

var (
	ErrPublisherNotStarted     = errors.New("publisher is not started")
	ErrPublisherAlreadyStarted = errors.New("publisher is already started")
)

// Publisher implements the publisher using wrpkafka.
// It manages the publisher lifecycle, message production, and graceful shutdown.
type Publisher struct {
	wrpPublisher  *wrpkafka.Publisher
	config        *publisherConfig
	logger        *slog.Logger
	logEmitter    *observe.Subject[log.Event]
	metricEmitter *observe.Subject[metrics.Event]

	mu      sync.RWMutex
	started bool
}

// New creates a new Publisher with the provided options.
// Required options: WithBrokers, WithTopicRoutes
func New(opts ...Option) (*Publisher, error) {
	// Create publisher with initial config
	publisher := &Publisher{
		config:        &publisherConfig{},
		logger:        slog.Default(),
		logEmitter:    log.NewNoop(),
		metricEmitter: metrics.NewNoop(),
	}

	// Apply all provided options
	for _, opt := range opts {
		if err := opt.apply(publisher); err != nil {
			return nil, fmt.Errorf("failed to apply option: %w", err)
		}
	}

	// Validate required options
	if err := publisher.config.validate(); err != nil {
		return nil, err
	}

	// Create the underlying wrpkafka publisher
	wrpPublisher := &wrpkafka.Publisher{
		Brokers: publisher.config.brokers,
		InitialDynamicConfig: wrpkafka.DynamicConfig{
			TopicMap: publisher.config.topicRoutes,
		},
		SASL:                         publisher.config.sasl,
		TLS:                          publisher.config.tls,
		MaxBufferedRecords:           publisher.config.maxBufferedRecords,
		MaxBufferedBytes:             publisher.config.maxBufferedBytes,
		RequestTimeout:               publisher.config.requestTimeout,
		CleanupTimeout:               publisher.config.cleanupTimeout,
		MaxRetries:                   publisher.config.maxRetries,
		AllowAutoTopicCreation:       publisher.config.allowAutoTopicCreation,
		Logger:                       publisher.config.logger,
		InitialPublishEventListeners: publisher.config.publishEventListeners,
	}

	publisher.wrpPublisher = wrpPublisher

	return publisher, nil
}

// Start initializes and starts the publisher.
// This must be called before producing messages.
func (p *Publisher) Start() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.started {
		return ErrPublisherAlreadyStarted
	}

	if err := p.wrpPublisher.Start(); err != nil {
		p.logEmitter.Notify(log.NewEvent(log.LevelError, "Failed to start WRP publisher", map[string]any{
			"error": err.Error(),
		}))
		return fmt.Errorf("failed to start wrpkafka publisher: %w", err)
	}

	p.started = true
	p.logEmitter.Notify(log.NewEvent(log.LevelInfo, "WRP publisher started successfully", nil))

	return nil
}

// Stop shuts down the publisher.
func (p *Publisher) Stop(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.started {
		return nil
	}

	p.wrpPublisher.Stop(ctx)
	p.started = false
	p.logEmitter.Notify(log.NewEvent(log.LevelInfo, "WRP publisher stopped", nil))

	return nil
}

func (p *Publisher) IsStarted() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.started
}

// Produce sends a WRP message to the appropriate Kafka topic based on routing rules.
// Returns the outcome of the produce operation and any error encountered.
func (p *Publisher) Produce(ctx context.Context, msg *wrp.Message) (wrpkafka.Outcome, error) {
	p.mu.RLock()
	started := p.started
	p.mu.RUnlock()

	if !started {
		p.metricEmitter.Notify(metrics.Event{
			Name:  "publisher_errors",
			Value: 1,
			Labels: []string{
				"error_type", "not_started",
			},
		})
		return 0, ErrPublisherNotStarted // Return 0 (which corresponds to wrpkafka.Accepted) as default
	}

	// Emit metrics about the message being produced
	p.metricEmitter.Notify(metrics.Event{
		Name:  "messages_produced_total",
		Value: 1,
		Labels: []string{
			"message_type", msg.Type.String(),
			"source", msg.Source,
		},
	})

	outcome, err := p.wrpPublisher.Produce(ctx, msg)
	if err != nil {
		p.logEmitter.Notify(log.NewEvent(log.LevelError, "Failed to produce WRP message", map[string]any{
			"error":        err.Error(),
			"message_type": msg.Type.String(),
			"source":       msg.Source,
			"destination":  msg.Destination,
		}))
		return outcome, fmt.Errorf("failed to produce message: %w", err)
	}

	p.metricEmitter.Notify(metrics.Event{
		Name:  "messages_produced",
		Value: 1,
		Labels: []string{
			"message_type", msg.Type.String(),
			"outcome", outcome.String(),
		},
	})

	p.logEmitter.Notify(log.NewEvent(log.LevelDebug, "WRP message produced successfully", map[string]any{
		"message_type": msg.Type.String(),
		"source":       msg.Source,
		"destination":  msg.Destination,
		"outcome":      outcome.String(),
	}))

	return outcome, nil
}
