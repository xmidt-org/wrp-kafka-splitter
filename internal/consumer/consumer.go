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
// It manages the consumer lifecycle, message polling, and graceful shutdown.
type Consumer struct {
	client        Client
	clientId      string
	handler       MessageHandler
	config        *consumerConfig
	logEmitter    *observe.Subject[log.Event]
	metricEmitter *observe.Subject[metrics.Event]

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu                    sync.RWMutex
	running               bool
	timeSinceLastSuccess  atomic.Int64
	pauseThresholdSeconds int64
	resumeDelaySeconds    int64
	isPaused              atomic.Bool
	unPauseAt             atomic.Int64
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
					metrics.GroupLabel, req.Group,
					metrics.MemberIdLabel, req.MemberID,
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

	//client.MarkCommitRecords() // add to interface
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create kafka client: %w", err)
	}

	consumer.client = client

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

	c.emitLog(log.LevelInfo, "consumer started", map[string]any{
		"topics":   c.config.topics,
		"group_id": c.config.groupID,
	})

	c.timeSinceLastSuccess.Store(time.Now().Unix())

	// Start polling loops in background
	c.running = true

	c.wg.Add(1)
	go c.pollLoop()
	c.wg.Add(1)
	go c.startManageFetchState()

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
					Name: metrics.ConsumerFetchErrors,
					Labels: []string{
						metrics.PartitionLabel, fmt.Sprintf("%d", err.Partition),
						metrics.TopicLabel, err.Topic,
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
			var outcome wrpkafka.Outcome
			if outcome, err = c.handleRecord(record); err != nil {
				c.emitLog(log.LevelError, "message handling error", map[string]any{
					"error":     err,
					"topic":     record.Topic,
					"partition": record.Partition,
					"offset":    record.Offset,
				})
			}
			c.handleOutcome(outcome, err, record)
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
// in the consumer. If we really want to avoid losing QOS > 74, we would probably need
// to create a local retry queue just for those records.
//
// 2. If we don't want to mark ANY records for commit because the producer is
// failing due to loss of network connectivity and full buffers, we would need to modify
// wrpkafka library to return synchronous errors for queued and attempted outcomes.
// However, the current assumption is that these errors are not returned
// because we are not interested in reprocessing records with QOS < 74.
//
// 3. as a side note, the callbacks also cannot be used to control offset commits,
// because there is no good way to tie the producer record to the consumer record.
//
// 4. We could potentially use transactions but wrpkafka does not currently support them and
// we would probably want to batch the same QOS together which gets more complicated.
//
// 5. One viable improvement could be to pause fetches if we see a lot of retryable errors
// in the callbacks. (i.e. network issues or full buffers).
func (c *Consumer) handleOutcome(outcome wrpkafka.Outcome, err error, record *kgo.Record) {
	// if queued, attempted or accepted, mark for commit
	if outcome != wrpkafka.Failed {
		c.client.MarkCommitRecords(record)
	} else if err != nil && !isRetryable(err) {
		// if ProduceSync failed but is not retryable, mark for commit
		c.client.MarkCommitRecords(record)
	}
	// do not mark failures with retryable errors for commit, but see notes above that this does not do much
	// TODO - potentially implement a local retry queue for high QOS

	if outcome == wrpkafka.Accepted {
		c.timeSinceLastSuccess.Store(time.Now().Unix())
	}
}

func isRetryable(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, kerr.RequestTimedOut) ||
		errors.Is(err, kgo.ErrMaxBuffered) ||
		errors.Is(err, net.ErrClosed) ||
		errors.Is(err, io.EOF)
}

func (c *Consumer) startManageFetchState() {
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

func (c *Consumer) manageFetchState() {
	now := time.Now().Unix()
	lastSuccess := c.timeSinceLastSuccess.Load()
	elapsed := now - lastSuccess

	if c.isPaused.Load() {
		// Resume if: 1) timer expired OR 2) recent success
		shouldResume := time.Now().After(time.Unix(c.unPauseAt.Load(), 0)) ||
			elapsed < c.pauseThresholdSeconds

		if shouldResume {
			c.resumeFetchTopics()
		}
		return
	}

	// Not paused - check if we should pause
	if elapsed >= c.pauseThresholdSeconds {
		c.pauseFetchTopics()
	}
}

func (c *Consumer) pauseFetchTopics() {
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
		},
		Value: 1,
	})
}

func (c *Consumer) resumeFetchTopics() {
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
func (c *Consumer) handleRecord(record *kgo.Record) (wrpkafka.Outcome, error) {
	// Create a context for this message
	// In a real implementation, this would include tracing context
	ctx := context.Background()

	// Invoke the handler
	return c.handler.HandleMessage(ctx, record)
}

func (c *Consumer) HandlePublishEvent(event *wrpkafka.PublishEvent) {

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

	// update last success time
	if event.Error == nil {
		c.timeSinceLastSuccess.Store(time.Now().Unix())
	}
}

// emitEvent emits a log event to the configured emitter.
// The emitter is never nil (defaults to no-op if not configured).
func (c *Consumer) emitLog(level log.Level, message string, attrs map[string]any) {
	c.logEmitter.Notify(log.NewEvent(level, message, attrs))
}
