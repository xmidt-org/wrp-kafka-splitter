// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package consumer

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"xmidt-org/splitter/internal/log"
	"xmidt-org/splitter/internal/metrics"
	"xmidt-org/splitter/internal/observe"

	"github.com/twmb/franz-go/pkg/kgo"
)

var ErrPingingBroker = errors.New("error pinging kafka broker")

// Consumer represents a high-throughput Kafka consumer using franz-go.
// It manages the consumer lifecycle, message polling, and graceful shutdown.
type Consumer struct {
	client        Client
	handler       MessageHandler
	config        *consumerConfig
	logEmitter    *observe.Subject[log.Event]
	metricEmitter *observe.Subject[metrics.Event]

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu      sync.RWMutex
	running bool
}

// New creates a new Consumer with the provided options.
// Required options: WithBrokers, WithTopics, WithGroupID, WithMessageHandler
// Returns an error if required options are missing or if the client cannot be created.
func New(opts ...Option) (*Consumer, error) {
	// Create consumer with initial config
	ctx, cancel := context.WithCancel(context.Background())

	consumer := &Consumer{
		config: &consumerConfig{
			kgoOpts: make([]kgo.Opt, 0),
		},
		logEmitter: log.NewNoop(), // Default to no-op emitter
		ctx:        ctx,
		cancel:     cancel,
	}

	// Apply all options to the consumer
	for _, opt := range opts {
		if err := opt.apply(consumer); err != nil {
			cancel()
			return nil, fmt.Errorf("failed to apply option: %w", err)
		}
	}

	// Validate required options
	if err := validate(consumer.config); err != nil {
		cancel()
		return nil, fmt.Errorf("missing required options: %w", err)
	}

	// Build franz-go client options
	kgoOpts := make([]kgo.Opt, 0, 3+len(consumer.config.kgoOpts))
	kgoOpts = append(kgoOpts,
		kgo.SeedBrokers(consumer.config.brokers...),
		kgo.ConsumerGroup(consumer.config.groupID),
		kgo.ConsumeTopics(consumer.config.topics...),
	)

	// Append user-provided options
	kgoOpts = append(kgoOpts, consumer.config.kgoOpts...)

	// Create franz-go client
	client, err := kgo.NewClient(kgoOpts...)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create kafka client: %w", err)
	}

	consumer.client = client
	consumer.handler = consumer.config.handler

	return consumer, nil
}

// Start begins consuming messages from Kafka.
// It starts a background goroutine that polls for messages and invokes the handler.
// Returns an error if the consumer is already running or if Kafka connection fails.
func (c *Consumer) Start() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.running {
		return errors.New("consumer is already running")
	}

	// Ping Kafka to ensure connectivity
	if err := c.client.Ping(c.ctx); err != nil {
		c.emitLog(log.LevelError, "failed to start consumer", map[string]any{
			"error": err,
		})
		return fmt.Errorf("failed to ping kafka brokers: %s, %w", err.Error(), ErrPingingBroker)
	}

	c.running = true
	c.wg.Add(1)

	c.emitLog(log.LevelInfo, "consumer started", map[string]any{
		"topics":   c.config.topics,
		"group_id": c.config.groupID,
	})

	go c.pollLoop()

	return nil
}

// Stop gracefully stops the consumer.
// It waits for the current message processing to complete and commits final offsets.
// The provided context controls the shutdown timeout.
func (c *Consumer) Stop(ctx context.Context) error {
	c.mu.Lock()
	if !c.running {
		c.mu.Unlock()
		return errors.New("consumer is not running")
	}
	c.running = false
	c.mu.Unlock()

	c.emitLog(log.LevelInfo, "stopping consumer", map[string]any{
		"topics":   c.config.topics,
		"group_id": c.config.groupID,
	})

	// Signal polling loop to stop
	c.cancel()

	// Wait for polling loop to finish with timeout
	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Graceful shutdown completed
		c.emitLog(log.LevelInfo, "consumer stopped gracefully", nil)
	case <-ctx.Done():
		// Timeout exceeded, force close
		c.emitLog(log.LevelWarn, "consumer shutdown timeout exceeded", map[string]any{
			"error": ctx.Err(),
		})
		return fmt.Errorf("shutdown timeout exceeded: %w", ctx.Err())
	}

	// Close the client (commits final offsets)
	c.client.Close()

	return nil
}

// IsRunning returns true if the consumer is currently running.
func (c *Consumer) IsRunning() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.running
}

// pollLoop continuously polls for messages and processes them.
// It runs in a background goroutine started by Start().
func (c *Consumer) pollLoop() {
	defer c.wg.Done()

	for {
		// Check if we should stop
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		// Poll for messages
		fetches := c.client.PollFetches(c.ctx)

		// The only error returned that is potentially actionable is ErrClientClosed.  That
		// would most likely happen if Stop() was called.  franz-go has built in automatic
		// reconnection logic if the client disconnects for other reasons.  For now, just log and
		// continue processing.
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, err := range errs {
				c.emitLog(log.LevelError, "kafka fetch error", map[string]any{
					"error":     err.Err,
					"topic":     err.Topic,
					"partition": err.Partition,
				})
				c.metricEmitter.Notify(metrics.Event{
					Name: "consumer_errors",
					Labels: []string{
						"partition", fmt.Sprintf("%d", err.Partition),
						"topic", err.Topic,
						"type", "record",
					},
					Value: 1,
				})
			}
		}

		// Process each record
		fetches.EachRecord(func(record *kgo.Record) {
			if err := c.handleRecord(record); err != nil {
				c.emitLog(log.LevelError, "message handling error", map[string]any{
					"error":     err,
					"topic":     record.Topic,
					"partition": record.Partition,
					"offset":    record.Offset,
				})
				c.metricEmitter.Notify(metrics.Event{
					Name: "consumer_errors",
					Labels: []string{
						"partition", fmt.Sprintf("%d", record.Partition),
						"topic", record.Topic,
						"type", "record",
					},
					Value: 1,
				})
			}
		})

		// the only time autocommit really comes into play is if the consuming application
		// itself crashes.  Producer errors returned above are either non-retryable kafka errors or
		// bad wrp messages errors.  Likewise, Fetch errors are logged but do not prevent commits.
		if c.config.autocommitDisabled {
			if err := c.client.CommitUncommittedOffsets(context.Background()); err != nil {
				c.emitLog(log.LevelError, "commit failed", map[string]any{
					"error": err,
				})
				c.metricEmitter.Notify(metrics.Event{
					Name: metrics.ConsumerErrors,
					Labels: []string{
						metrics.PartitionLabel, "unknown",
						metrics.TopicLabel, "unknown",
						metrics.ErrorTypeLabel, "commit",
					},
					Value: 1,
				})
			}
		}

	}
}

// handleRecord processes a single record using the configured handler.
func (c *Consumer) handleRecord(record *kgo.Record) error {
	// Create a context for this message
	// In a real implementation, this would include tracing context
	ctx := context.Background()

	// Invoke the handler
	if err := c.handler.HandleMessage(ctx, record); err != nil {
		return fmt.Errorf("handler error for topic=%s partition=%d offset=%d: %w",
			record.Topic, record.Partition, record.Offset, err)
	}

	return nil
}

// emitEvent emits a log event to the configured emitter.
// The emitter is never nil (defaults to no-op if not configured).
func (c *Consumer) emitLog(level log.Level, message string, attrs map[string]any) {
	c.logEmitter.Notify(log.NewEvent(level, message, attrs))
}
