// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package consumer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"xmidt-org/splitter/internal/log"
	"xmidt-org/splitter/internal/metrics"
	"xmidt-org/splitter/internal/observe"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/xmidt-org/wrpkafka"
)

var ErrPingingBroker = errors.New("error pinging kafka broker")

var tickerInterval = 5 * time.Second

// Consumer represents a high-throughput Kafka consumer using franz-go.

type Consumer interface {
	Start() error
	Stop(ctx context.Context) error
	IsRunning() bool
}

// It manages the consumer lifecycle, message polling, and graceful shutdown.
type KafkaConsumer struct {
	client        Client
	clientId      string
	handler       MessageHandler
	config        *consumerConfig
	logEmitter    *observe.Subject[log.Event]
	metricEmitter *observe.Subject[metrics.Event]

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu                          sync.RWMutex
	running                     bool
	consecutiveFailures         atomic.Int64
	consecutiveFailureThreshold int
	resumeDelaySeconds          int
	isPaused                    atomic.Bool
	unPauseAt                   atomic.Int64
}

// New creates a new Consumer with the provided options.
// Required options: WithBrokers, WithTopics, WithGroupID, WithMessageHandler
// Returns an error if required options are missing or if the client cannot be created.
func New(opts ...Option) (Consumer, error) {
	// Create consumer with initial config
	ctx, cancel := context.WithCancel(context.Background())

	consumer := &KafkaConsumer{
		config: &consumerConfig{
			kgoOpts: make([]kgo.Opt, 0),
		},
		logEmitter:    log.NewNoop(),
		metricEmitter: metrics.NewNoop(),
		ctx:           ctx,
		cancel:        cancel,
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
		kgo.AutoCommitMarks(), // commit marked offsets automatically
		kgo.AutoCommitCallback(func(c *kgo.Client, req *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
			consumer.metricEmitter.Notify(metrics.Event{
				Name: metrics.ConsumerCommitErrors,
				Labels: []string{
					metrics.MemberIdLabel, req.MemberID,
					metrics.GroupLabel, consumer.config.groupID,
					metrics.ClientIdLabel, consumer.clientId,
				},
				Value: 1,
			})
		}),
	)

	// Append user-provided options
	kgoOpts = append(kgoOpts, consumer.config.kgoOpts...)

	// Create franz-go client
	client, err := kgo.NewClient(kgoOpts...)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create kafka client: %w", err)
	}

	consumer.client = &kgoClientAdapter{Client: client}
	return consumer, nil
}

// Start begins consuming messages from Kafka.
// It starts a background goroutine that polls for messages and invokes the handler.
// Returns an error if the consumer is already running or if Kafka connection fails.
func (c *KafkaConsumer) Start() error {
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

	c.emitLog(log.LevelInfo, "consumer started", map[string]any{
		"topics":   c.config.topics,
		"group_id": c.config.groupID,
	})

	c.consecutiveFailures.Store(0)

	// Start polling loops in background
	c.running = true

	c.wg.Add(1)
	go c.pollLoop()

	// only start fetch state manager if thresholds are configured
	if (c.consecutiveFailureThreshold > 0) && (c.resumeDelaySeconds > 0) {
		c.wg.Add(1)
		go c.startManageFetchState()
	}

	return nil
}

// Stop gracefully stops the consumer.
// It waits for the current message processing to complete and commits final offsets.
// The provided context controls the shutdown timeout.
func (c *KafkaConsumer) Stop(ctx context.Context) error {
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
func (c *KafkaConsumer) IsRunning() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.running
}

// pollLoop continuously polls for messages and processes them.
// It runs in a background goroutine started by Start().
func (c *KafkaConsumer) pollLoop() {
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
					Name: metrics.ConsumerFetchErrors,
					Labels: []string{
						metrics.PartitionLabel, fmt.Sprintf("%d", err.Partition),
						metrics.GroupLabel, c.config.groupID,
						metrics.ClientIdLabel, c.clientId,
					},
					Value: 1,
				})
			}
		}

		// Process each record
		fetches.EachRecord(func(record *kgo.Record) {
			if c.isPaused.Load() {
				// Stop processing remaining records in this batch
				return
			}

			var err error
			var outcome Outcome
			if outcome, err = c.handleRecord(record); err != nil {
				c.emitLog(log.LevelError, "message handling error", map[string]any{
					"error":     err,
					"topic":     record.Topic,
					"partition": record.Partition,
					"offset":    record.Offset,
				})
			}
			c.handleOutcome(outcome, err, record)

			c.metricEmitter.Notify(metrics.Event{
				Name:  metrics.PublisherOutcomes,
				Value: 1,
				Labels: []string{
					metrics.OutcomeLabel, outcome.String(),
				},
			})
		})
	}
}

// Commit Logic:
//
// 1. if outcome is Attempted, Queued or Accepted, the record is marked for commit.
// 2. if the outcome is Failed and the error is not retryable, the record is marked for commit.
// 3. if the outcome is Failed and the error is retryable, the record is NOT marked for commit.
//
// Notes and Shortcomings On Commit Logic:
//
// 1. number 3 does not do much of anything, since any subsequent records
// that are successfully Queued or Attempted will commit all previous records anyway.
// The partitions we are reading from contain a mix of QOS levels, so while the records may
// be treated differently in the producer, they all share the same offsets
// in the consumer. If we really want to avoid losing hign QOS, we would probably need
// to create a local retry queue just for those records.
//
// 2. as a side note, the callbacks also cannot be used to control offset commits,
// because there is no good way to tie the producer record to the consumer record.
//
// 4. We could all or nothing transactions but wrpkafka does not currently support them.
// We would need to pass in our own kafka client for the session.  Also, if the all or nothing
// fails, how long do we want to keep the messages from being committed? What is the criteria?  If
// ANY of the errors from any of the records are retriable, we don't commit the whole lot?
//
// 5. This code currently pause fetches after a configurable number of consecutive, retryable errors.
// But we will lose all records committed but not produced prior to the pause.
//
// 6.  TODO - add transaction support to wrpkafka library
func (c *KafkaConsumer) handleOutcome(outcome Outcome, err error, record *kgo.Record) {
	// if queued, attempted, accepted or a permanent error, mark for commit
	if outcome != Failed {
		c.client.MarkCommitRecords(record)
	} else if err != nil && !isRetryable(err) {
		c.client.MarkCommitRecords(record)
	}

	// Accepted means the producer synchronously received an ack from the broker
	// TODO - consider implementing a local retry queue for high QOS
	if outcome == Accepted {
		c.consecutiveFailures.Store(0)
	}

	c.handleRetryableError(err)
}

func (c *KafkaConsumer) handleRetryableError(err error) {
	// if the error is retryable, increment consecutive failures
	if isRetryable(err) {
		c.consecutiveFailures.Add(1)
		c.emitLog(log.LevelWarn, "retryable error from producer", map[string]any{
			"error": err,
			"count": c.consecutiveFailures.Load(),
		})
	}
}

func (c *KafkaConsumer) startManageFetchState() {
	defer c.wg.Done() // Decrement when this goroutine exits

	ticker := time.NewTicker(tickerInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			// The context was canceled, time to exit the goroutine.
			c.emitLog(log.LevelInfo, "Context canceled, stopping manageFetchState loop.", map[string]any{})
			return
		case <-ticker.C:
			c.manageFetchState()
		}
	}
}

func (c *KafkaConsumer) manageFetchState() {
	consecutiveFailures := c.consecutiveFailures.Load()

	if c.isPaused.Load() {
		// Resume if: 1) timer expired OR 2) recent success(es)
		shouldResume := time.Now().After(time.Unix(c.unPauseAt.Load(), 0)) ||
			consecutiveFailures < int64(c.consecutiveFailureThreshold)

		if shouldResume {
			c.resumeFetchTopics()
		}
		return
	}

	// Not paused - check if we should pause
	if consecutiveFailures >= int64(c.consecutiveFailureThreshold) {
		c.pauseFetchTopics()
	}
}

func (c *KafkaConsumer) pauseFetchTopics() {
	if c.isPaused.Load() {
		return
	}
	c.emitLog(log.LevelWarn, "pausing fetches due to sustained publish failures", map[string]any{})
	c.client.PauseFetchTopics(c.config.topics...)
	c.isPaused.Store(true)
	c.unPauseAt.Store(time.Now().Add(time.Duration(c.resumeDelaySeconds) * time.Second).Unix())
	c.metricEmitter.Notify(metrics.Event{
		Name: metrics.ConsumerPauses,
		Labels: []string{
			metrics.ClientIdLabel, c.clientId,
			metrics.GroupLabel, c.config.groupID,
		},
		Value: 1,
	})
}

func (c *KafkaConsumer) resumeFetchTopics() {
	c.emitLog(log.LevelInfo, "resuming fetches after pause", map[string]any{})
	c.client.ResumeFetchTopics(c.config.topics...)
	c.isPaused.Store(false)
	c.metricEmitter.Notify(metrics.Event{
		Name: metrics.ConsumerPauses,
		Labels: []string{
			metrics.ClientIdLabel, c.clientId,
		},
		Value: 0,
	})
}

// handleRecord processes a single record using the configured handler.
// not that if the buffer is full, franz-go will block on produce until the delivery timeout,
// , so this call should be synchonous and also block to avoid consuming more messages
func (c *KafkaConsumer) handleRecord(record *kgo.Record) (Outcome, error) {
	// Create a context for this message
	// In a real implementation, this would include tracing context
	ctx := context.Background()

	// Invoke the handler
	return c.handler.HandleMessage(ctx, record)
}

func (c *KafkaConsumer) HandlePublishEvent(event *wrpkafka.PublishEvent) {
	if event.Error != nil {
		c.emitLog(log.LevelError, "message publish error", map[string]any{
			"topic":     event.Topic,
			"error":     event.Error,
			"errorType": event.ErrorType,
		})
	} else {
		c.emitLog(log.LevelDebug, "message published successfully", map[string]any{
			"topic": event.Topic,
			"qos":   event.EventType,
		})
	}

	// clean consecutive failures on success
	if event.Error == nil {
		c.consecutiveFailures.Store(0)
	}

	c.handleRetryableError(event.Error)
}

// emitEvent emits a log event to the configured emitter.
// The emitter is never nil (defaults to no-op if not configured).
func (c *KafkaConsumer) emitLog(level log.Level, message string, attrs map[string]any) {
	c.logEmitter.Notify(log.NewEvent(level, message, attrs))
}

func isRetryable(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, kerr.RequestTimedOut) ||
		errors.Is(err, kgo.ErrMaxBuffered) ||
		errors.Is(err, net.ErrClosed) ||
		errors.Is(err, io.EOF)
}
