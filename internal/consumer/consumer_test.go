// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package consumer

import (
	"context"
	"errors"
	"testing"
	"time"

	"xmidt-org/splitter/internal/log"
	"xmidt-org/splitter/internal/metrics"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/xmidt-org/wrpkafka"
)

// ConsumerTestSuite is the test suite for Consumer functionality
type ConsumerTestSuite struct {
	suite.Suite
	mockClient *MockClient
	consumer   *KafkaConsumer
}

func TestConsumerTestSuite(t *testing.T) {
	suite.Run(t, new(ConsumerTestSuite))
}

func (s *ConsumerTestSuite) SetupTest() {
	s.mockClient = &MockClient{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.consumer = &KafkaConsumer{
		client:  s.mockClient,
		handler: &MockHandler{},
		config: &consumerConfig{
			brokers: []string{"localhost:9092"},
			topics:  []string{"test-topic"},
			groupID: "test-group",
		},
		logEmitter:                  log.NewNoop(),
		metricEmitter:               metrics.NewNoop(),
		ctx:                         ctx,
		cancel:                      cancel,
		consecutiveFailureThreshold: 60,
		resumeDelaySeconds:          30,
	}
	s.consumer.consecutiveFailures.Store(time.Now().Unix())
}

func GetConsumer() *KafkaConsumer {
	mockClient := &MockClient{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	consumer := &KafkaConsumer{
		client:  mockClient,
		handler: &MockHandler{},
		config: &consumerConfig{
			brokers: []string{"localhost:9092"},
			topics:  []string{"test-topic"},
			groupID: "test-group",
		},
		logEmitter:                  log.NewNoop(),
		metricEmitter:               metrics.NewNoop(),
		ctx:                         ctx,
		cancel:                      cancel,
		consecutiveFailureThreshold: 60,
		resumeDelaySeconds:          30,
	}
	consumer.consecutiveFailures.Store(time.Now().Unix())
	return consumer
}

func (s *ConsumerTestSuite) TearDownTest() {
	if s.consumer != nil && s.consumer.cancel != nil {
		s.consumer.cancel()
	}
}

// TestNew tests consumer creation with various option combinations
func (s *ConsumerTestSuite) TestNew() {
	tests := []struct {
		name        string
		opts        []Option
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid consumer with all required options",
			opts: []Option{
				WithBrokers("localhost:9092"),
				WithTopics("topic1", "topic2"),
				WithGroupID("test-group"),
				WithMessageHandler(&MockHandler{}),
			},
			expectError: false,
		},
		{
			name: "missing brokers",
			opts: []Option{
				WithTopics("topic1"),
				WithGroupID("test-group"),
				WithMessageHandler(&MockHandler{}),
			},
			expectError: true,
			errorMsg:    "brokers",
		},
		{
			name: "missing topics",
			opts: []Option{
				WithBrokers("localhost:9092"),
				WithGroupID("test-group"),
				WithMessageHandler(&MockHandler{}),
			},
			expectError: true,
			errorMsg:    "topics",
		},
		{
			name: "missing group ID",
			opts: []Option{
				WithBrokers("localhost:9092"),
				WithTopics("topic1"),
				WithMessageHandler(&MockHandler{}),
			},
			expectError: true,
			errorMsg:    "group",
		},
		{
			name: "missing handler",
			opts: []Option{
				WithBrokers("localhost:9092"),
				WithTopics("topic1"),
				WithGroupID("test-group"),
			},
			expectError: true,
			errorMsg:    "handler",
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			consumer, err := New(tt.opts...)
			var kafkaConsumer *KafkaConsumer
			if consumer != nil {
				kafkaConsumer, _ = consumer.(*KafkaConsumer)
			}

			if tt.expectError {
				s.Error(err)
				if tt.errorMsg != "" {
					s.Contains(err.Error(), tt.errorMsg)
				}
				s.Nil(consumer)
			} else {
				s.NoError(err)
				s.NotNil(kafkaConsumer)
				if kafkaConsumer != nil {
					kafkaConsumer.cancel()
				}
			}
		})
	}
}

// TestStart tests consumer startup scenarios
func (s *ConsumerTestSuite) TestStart() {
	tests := []struct {
		name        string
		setupMock   func(*MockClient)
		isRunning   bool
		expectError bool
		errorType   error
	}{
		{
			name: "successful start",
			setupMock: func(m *MockClient) {
				m.On("Ping", mock.Anything).Return(nil)
				// PollFetches will be called by pollLoop goroutine after successful start
				mockFetches := &MockFetches{}
				mockFetches.On("Errors").Return([]*kgo.FetchError(nil))
				mockFetches.On("EachRecord", mock.Anything).Return()
				m.On("PollFetches", mock.Anything).Return(mockFetches).Maybe()
			},
			isRunning:   false,
			expectError: false,
		},
		{
			name: "already running",
			setupMock: func(m *MockClient) {
				// No ping expected
			},
			isRunning:   true,
			expectError: true,
		},
		{
			name: "ping fails",
			setupMock: func(m *MockClient) {
				m.On("Ping", mock.Anything).Return(errors.New("connection failed"))
			},
			isRunning:   false,
			expectError: true,
			errorType:   ErrPingingBroker,
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			// Reset mock for each test
			consumer := GetConsumer()
			mockClient := &MockClient{}
			if tt.setupMock != nil {
				tt.setupMock(mockClient)
			}
			consumer.client = mockClient
			consumer.running = tt.isRunning

			err := consumer.Start()

			if tt.expectError {
				s.Error(err)
				if tt.errorType != nil {
					s.ErrorIs(err, tt.errorType)
				}
			} else {
				s.NoError(err)
				s.True(consumer.IsRunning())
			}

			mockClient.AssertExpectations(s.T())
		})
	}
}

// TestStop tests consumer shutdown scenarios
func (s *ConsumerTestSuite) TestStop() {
	tests := []struct {
		name        string
		setupMock   func(*MockClient)
		isRunning   bool
		timeout     time.Duration
		expectError bool
	}{
		{
			name: "graceful stop",
			setupMock: func(m *MockClient) {
				m.On("Close").Return()
			},
			isRunning:   true,
			timeout:     5 * time.Second,
			expectError: false,
		},
		{
			name: "not running",
			setupMock: func(m *MockClient) {
				// No close expected
			},
			isRunning:   false,
			timeout:     5 * time.Second,
			expectError: true,
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			// Create a fresh consumer for each test
			consumer := GetConsumer()
			mockClient := &MockClient{}
			consumer.client = mockClient
			consumer.running = tt.isRunning

			if tt.setupMock != nil {
				tt.setupMock(mockClient)
			}

			ctx, cancel := context.WithTimeout(context.Background(), tt.timeout)
			defer cancel()

			err := consumer.Stop(ctx)

			if tt.expectError {
				s.Error(err)
			} else {
				s.NoError(err)
				s.False(consumer.IsRunning())
			}

			consumer.cancel()
			consumer.wg.Wait()

			mockClient.AssertExpectations(s.T())
		})
	}
}

// TestHandleOutcome tests offset commit logic based on different outcomes
func (s *ConsumerTestSuite) TestHandleOutcome() {
	record := &kgo.Record{
		Topic:     "test-topic",
		Partition: 0,
		Offset:    100,
	}

	tests := []struct {
		name                     string
		outcome                  Outcome
		err                      error
		expectCommit             bool
		expectSuccess            bool
		expectHigherFailureCount bool
	}{
		{
			name:                     "accepted outcome - commit and update success time",
			outcome:                  Accepted,
			err:                      nil,
			expectCommit:             true,
			expectSuccess:            true,
			expectHigherFailureCount: false,
		},
		{
			name:                     "attempted outcome - commit",
			outcome:                  Attempted,
			err:                      nil,
			expectCommit:             true,
			expectSuccess:            false,
			expectHigherFailureCount: false,
		},
		{
			name:                     "queued outcome - commit",
			outcome:                  Queued,
			err:                      nil,
			expectCommit:             true,
			expectSuccess:            false,
			expectHigherFailureCount: false,
		},
		{
			name:                     "failed with non-retryable error - commit",
			outcome:                  Failed,
			err:                      errors.New("permanent error"),
			expectCommit:             true,
			expectSuccess:            false,
			expectHigherFailureCount: false,
		},
		{
			name:                     "failed with timeout - no commit",
			outcome:                  Failed,
			err:                      context.DeadlineExceeded,
			expectCommit:             false,
			expectSuccess:            false,
			expectHigherFailureCount: true,
		},
		{
			name:                     "failed with request timeout - no commit",
			outcome:                  Failed,
			err:                      kerr.RequestTimedOut,
			expectCommit:             false,
			expectSuccess:            false,
			expectHigherFailureCount: true,
		},
		{
			name:                     "failed with buffer full - no commit",
			outcome:                  Failed,
			err:                      kgo.ErrMaxBuffered,
			expectCommit:             false,
			expectSuccess:            false,
			expectHigherFailureCount: true,
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			s.mockClient = &MockClient{}
			s.consumer.client = s.mockClient

			// for testing purposes, make sure we have non-zero consecutive failures
			failuresBefore := s.consumer.consecutiveFailures.Add(1)

			// Sleep to ensure Unix second advances for success tests
			if tt.expectSuccess {
				time.Sleep(1100 * time.Millisecond)
			}

			if tt.expectCommit {
				// MarkCommitRecords is variadic, so mock expects a slice
				s.mockClient.On("MarkCommitRecords", mock.MatchedBy(func(records []*kgo.Record) bool {
					return len(records) == 1 && records[0] == record
				})).Return()
			}

			s.consumer.handleOutcome(tt.outcome, tt.err, record)

			// Check if consecutive failures were cleared
			failuresAfter := s.consumer.consecutiveFailures.Load()
			if tt.expectSuccess {
				s.Equal(int64(0), failuresAfter, "consecutive failures should be cleared on success")
			} else if tt.expectHigherFailureCount {
				s.Greater(failuresAfter, failuresBefore, "consecutive failures should be incremented")
			}

			s.mockClient.AssertExpectations(s.T())
		})
	}
}

// TestIsRetryable tests retry error classification
func (s *ConsumerTestSuite) TestIsRetryable() {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "context deadline exceeded",
			err:      context.DeadlineExceeded,
			expected: true,
		},
		{
			name:     "request timed out",
			err:      kerr.RequestTimedOut,
			expected: true,
		},
		{
			name:     "max buffered",
			err:      kgo.ErrMaxBuffered,
			expected: true,
		},
		{
			name:     "generic error",
			err:      errors.New("some error"),
			expected: false,
		},
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			result := isRetryable(tt.err)
			s.Equal(tt.expected, result)
		})
	}
}

// TestManageFetchState tests pause/resume state management
func (s *ConsumerTestSuite) TestManageFetchState() {
	tests := []struct {
		name                        string
		isPaused                    bool
		consecutiveFailures         int64
		consecutiveFailureThreshold int
		unPauseAt                   int64
		expectPause                 bool
		expectResume                bool
	}{
		{
			name:                        "should pause when threshold exceeded",
			isPaused:                    false,
			consecutiveFailures:         120,
			consecutiveFailureThreshold: 60,
			expectPause:                 true,
			expectResume:                false,
		},
		{
			name:                        "should not pause when under threshold",
			isPaused:                    false,
			consecutiveFailures:         30,
			consecutiveFailureThreshold: 60,
			expectPause:                 false,
			expectResume:                false,
		},
		{
			name:                        "should resume when timer expired",
			isPaused:                    true,
			consecutiveFailures:         120,
			consecutiveFailureThreshold: 60,
			unPauseAt:                   time.Now().Add(-5 * time.Second).Unix(), // Expired
			expectPause:                 false,
			expectResume:                true,
		},
		{
			name:                        "should resume when recent success while paused",
			isPaused:                    true,
			consecutiveFailures:         0,                                       // Recent success
			consecutiveFailureThreshold: 60,                                      // Under threshold
			unPauseAt:                   time.Now().Add(30 * time.Second).Unix(), // Not expired
			expectPause:                 false,
			expectResume:                true,
		},
		{
			name:                        "should stay paused when timer not expired",
			isPaused:                    true,
			consecutiveFailures:         120,
			consecutiveFailureThreshold: 60,
			unPauseAt:                   time.Now().Add(30 * time.Second).Unix(), // Future
			expectPause:                 false,
			expectResume:                false,
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			s.mockClient = &MockClient{}
			s.consumer.client = s.mockClient

			// Setup state
			s.consumer.isPaused.Store(tt.isPaused)
			s.consumer.consecutiveFailures.Store(tt.consecutiveFailures)
			s.consumer.consecutiveFailureThreshold = tt.consecutiveFailureThreshold
			s.consumer.unPauseAt.Store(tt.unPauseAt)

			if tt.expectPause {
				s.mockClient.On("PauseFetchTopics", s.consumer.config.topics).Return()
			}
			if tt.expectResume {
				s.mockClient.On("ResumeFetchTopics", s.consumer.config.topics).Return()
			}

			s.consumer.manageFetchState()

			// Verify pause state
			if tt.expectPause {
				s.True(s.consumer.isPaused.Load(), "consumer should be paused")
			}
			if tt.expectResume {
				s.False(s.consumer.isPaused.Load(), "consumer should be resumed")
			}

			s.mockClient.AssertExpectations(s.T())
		})
	}
}

// TestPauseFetchTopics tests the pause functionality
func (s *ConsumerTestSuite) TestPauseFetchTopics() {
	tests := []struct {
		name          string
		initialPaused bool
		expectCall    bool
	}{
		{
			name:          "pause when not paused",
			initialPaused: false,
			expectCall:    true,
		},
		{
			name:          "no-op when already paused",
			initialPaused: true,
			expectCall:    false,
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			s.mockClient = &MockClient{}
			s.consumer.client = s.mockClient
			s.consumer.isPaused.Store(tt.initialPaused)
			s.consumer.resumeDelaySeconds = 30

			if tt.expectCall {
				s.mockClient.On("PauseFetchTopics", s.consumer.config.topics).Return()
			}

			s.consumer.pauseFetchTopics()

			if tt.expectCall {
				s.True(s.consumer.isPaused.Load())
				s.Greater(s.consumer.unPauseAt.Load(), int64(0))
			}

			s.consumer.cancel()
			s.consumer.wg.Wait()

			s.mockClient.AssertExpectations(s.T())
		})
	}
}

// TestResumeFetchTopics tests the resume functionality
func (s *ConsumerTestSuite) TestResumeFetchTopics() {
	s.Run("resume topics", func() {
		s.mockClient = &MockClient{}
		s.consumer.client = s.mockClient
		s.consumer.isPaused.Store(true)

		s.mockClient.On("ResumeFetchTopics", s.consumer.config.topics).Return()

		s.consumer.resumeFetchTopics()

		s.False(s.consumer.isPaused.Load())
		s.mockClient.AssertExpectations(s.T())
	})
}

// TestHandlePublishEvent tests publish event handling
func (s *ConsumerTestSuite) TestHandlePublishEvent() {
	tests := []struct {
		name                     string
		event                    *wrpkafka.PublishEvent
		expectSuccess            bool
		expectHigherFailureCount bool
	}{
		{
			name: "successful publish",
			event: &wrpkafka.PublishEvent{
				Topic: "test-topic",
				Error: nil,
			},
			expectSuccess:            true,
			expectHigherFailureCount: false,
		},
		{
			name: "failed publish with permanent error",
			event: &wrpkafka.PublishEvent{
				Topic: "test-topic",
				Error: errors.New("permanent error"),
			},
			expectSuccess:            false,
			expectHigherFailureCount: false,
		},
		{
			name: "failed publish with retryable error",
			event: &wrpkafka.PublishEvent{
				Topic: "test-topic",
				Error: kerr.RequestTimedOut,
			},
			expectSuccess:            false,
			expectHigherFailureCount: true,
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			failuresBefore := s.consumer.consecutiveFailures.Add(1)

			// Sleep to ensure Unix second advances
			time.Sleep(1100 * time.Millisecond)

			s.consumer.HandlePublishEvent(tt.event)

			failuresAfter := s.consumer.consecutiveFailures.Load()
			if tt.expectSuccess {
				s.Equal(int64(0), failuresAfter, "consecutive failures should be cleared on success")
			} else if tt.expectHigherFailureCount {
				s.Greater(failuresAfter, failuresBefore, "consecutive failures should be incremented")
			}
		})
	}
}

// TestIsRunning tests the running state check
func (s *ConsumerTestSuite) TestIsRunning() {
	tests := []struct {
		name     string
		running  bool
		expected bool
	}{
		{
			name:     "consumer running",
			running:  true,
			expected: true,
		},
		{
			name:     "consumer not running",
			running:  false,
			expected: false,
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			s.consumer.running = tt.running
			result := s.consumer.IsRunning()
			s.Equal(tt.expected, result)
		})
	}
}

// TestPollLoop covers context done, no errors, and fetch error cases
func (s *ConsumerTestSuite) TestPollLoop() {
	tests := []struct {
		name    string
		setup   func(*MockClient, *MockFetches, *MockHandler)
		ctxDone bool
	}{
		{
			name:    "context done exits loop",
			setup:   func(m *MockClient, mf *MockFetches, mh *MockHandler) {},
			ctxDone: true,
		},
		{
			name: "no errors, processes records",
			setup: func(m *MockClient, mf *MockFetches, mh *MockHandler) {
				m.On("MarkCommitRecords", mock.Anything).Return()
				mf.On("Errors").Return([]*kgo.FetchError{})
				m.On("PollFetches", mock.Anything).Return(mf)
				mf.On("EachRecord", mock.Anything).Run(func(args mock.Arguments) {
					fn := args.Get(0).(func(*kgo.Record))
					rec1 := &kgo.Record{Topic: "test", Partition: 0, Offset: 0}
					rec2 := &kgo.Record{Topic: "test", Partition: 0, Offset: 1}
					mh.On("HandleMessage", mock.Anything, rec1).Return(Accepted, nil).Once()
					mh.On("HandleMessage", mock.Anything, rec2).Return(Accepted, nil).Once()
					fn(rec1)
					fn(rec2)
				})
			},
		},
		{
			name: "fetch error logs and emits metric",
			setup: func(m *MockClient, mf *MockFetches, mh *MockHandler) {
				mf.On("Errors").Return([]*kgo.FetchError{{Err: errors.New("fail"), Topic: "t", Partition: 1}})
				mf.On("EachRecord", mock.Anything).Return()
				m.On("PollFetches", mock.Anything).Return(mf)
			},
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			// Create a fresh consumer for each test
			consumer := GetConsumer()
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			consumer.ctx = ctx
			consumer.cancel = cancel

			mockClient := &MockClient{}
			mf := &MockFetches{}
			mh := &MockHandler{}
			consumer.client = mockClient
			consumer.handler = mh

			if tt.setup != nil {
				tt.setup(mockClient, mf, mh)
			}

			done := make(chan struct{})
			consumer.wg.Add(1)
			go func() {
				consumer.pollLoop()
				close(done)
			}()

			if tt.ctxDone {
				cancel()
			}

			select {
			case <-done:
				// exited as expected
			case <-time.After(1 * time.Second):
				s.Fail("pollLoop did not exit in time")
			}

			consumer.cancel()
			consumer.wg.Wait()

			mockClient.AssertExpectations(s.T())
			mf.AssertExpectations(s.T())
			mh.AssertExpectations(s.T())
		})
	}
}

// TestPollLoopPanic tests panic recovery in the poll loop
func (s *ConsumerTestSuite) TestPollLoopPanic() {
	tests := []struct {
		name          string
		setup         func(*MockClient, *MockFetches)
		expectedState bool // expected value of running after panic
	}{
		{
			name: "panic in poll loop sets running to false",
			setup: func(m *MockClient, mf *MockFetches) {
				// First call returns normally, second call panics
				m.On("PollFetches", mock.Anything).Return(mf).Once().Run(func(args mock.Arguments) {
					// Return normally first time
				})
				m.On("PollFetches", mock.Anything).Run(func(args mock.Arguments) {
					panic("simulated poll loop panic")
				})
				mf.On("Errors").Return([]*kgo.FetchError{})
				mf.On("EachRecord", mock.Anything).Return()
			},
			expectedState: false, // running should be false after panic
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			// Create a fresh consumer for each test
			consumer := GetConsumer()
			ctx, cancel := context.WithCancel(context.Background())
			consumer.ctx = ctx
			consumer.cancel = cancel
			consumer.running = true // Set to running initially

			mockClient := &MockClient{}
			mf := &MockFetches{}
			consumer.client = mockClient

			if tt.setup != nil {
				tt.setup(mockClient, mf)
			}

			done := make(chan struct{})
			consumer.wg.Add(1)
			go func() {
				consumer.pollLoop()
				close(done)
			}()

			// Wait for the goroutine to exit (should recover from panic)
			select {
			case <-done:
				// Goroutine exited (recovered from panic)
			case <-time.After(2 * time.Second):
				s.Fail("pollLoop did not exit in time after panic")
			}

			// Verify that running was set to false after panic
			s.Equal(tt.expectedState, consumer.IsRunning(), "running state should be false after panic recovery")

			cancel()
			consumer.wg.Wait()
		})
	}
}

// TestMessageProcessingPanic tests panic recovery during individual message processing
func (s *ConsumerTestSuite) TestMessageProcessingPanic() {
	tests := []struct {
		name                string
		setup               func(*MockClient, *MockFetches, *MockHandler)
		expectContinue      bool // expect poll loop to continue after message panic
		commitCalledForRec1 bool // expect commit to be called for record 1
		commitCalledForRec2 bool // expect commit to be called for record 2
	}{
		{
			name: "panic in message handler does not stop poll loop",
			setup: func(m *MockClient, mf *MockFetches, mh *MockHandler) {
				m.On("MarkCommitRecords", mock.Anything).Return().Once() // Only for record 2
				mf.On("Errors").Return([]*kgo.FetchError{})

				// Mock PollFetches to return the same fetches object repeatedly
				// so the loop continues after processing
				emptyFetches := &MockFetches{}
				emptyFetches.On("Errors").Return([]*kgo.FetchError{})
				emptyFetches.On("EachRecord", mock.Anything).Return()

				m.On("PollFetches", mock.Anything).Return(mf).Once()
				m.On("PollFetches", mock.Anything).Return(emptyFetches).Maybe()

				mf.On("EachRecord", mock.Anything).Run(func(args mock.Arguments) {
					fn := args.Get(0).(func(*kgo.Record))
					rec1 := &kgo.Record{Topic: "test", Partition: 0, Offset: 0}
					rec2 := &kgo.Record{Topic: "test", Partition: 0, Offset: 1}

					// First record causes panic in handler
					mh.On("HandleMessage", mock.Anything, rec1).Run(func(args mock.Arguments) {
						panic("handler panic")
					}).Return(Failed, errors.New("not reached")).Once()

					// Second record should still be processed
					mh.On("HandleMessage", mock.Anything, rec2).Return(Accepted, nil).Once()

					fn(rec1) // Should panic and recover
					fn(rec2) // Should process normally
				})
			},
			expectContinue:      true,
			commitCalledForRec1: false, // Should NOT commit panicked record
			commitCalledForRec2: true,  // Should commit successful record
		},
		{
			name: "panic while marking commit is recovered",
			setup: func(m *MockClient, mf *MockFetches, mh *MockHandler) {
				mf.On("Errors").Return([]*kgo.FetchError{})

				// Allow continued polling
				emptyFetches := &MockFetches{}
				emptyFetches.On("Errors").Return([]*kgo.FetchError{})
				emptyFetches.On("EachRecord", mock.Anything).Return()

				m.On("PollFetches", mock.Anything).Return(mf).Once()
				m.On("PollFetches", mock.Anything).Return(emptyFetches).Maybe()

				// First call to MarkCommitRecords panics
				m.On("MarkCommitRecords", mock.Anything).Run(func(args mock.Arguments) {
					panic("commit panic")
				}).Once()

				mf.On("EachRecord", mock.Anything).Run(func(args mock.Arguments) {
					fn := args.Get(0).(func(*kgo.Record))
					rec := &kgo.Record{Topic: "test", Partition: 0, Offset: 0}
					mh.On("HandleMessage", mock.Anything, rec).Return(Accepted, nil).Once()
					fn(rec) // Should panic during commit and recover
				})
			},
			expectContinue: true,
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			consumer := GetConsumer()
			ctx, cancel := context.WithCancel(context.Background())
			consumer.ctx = ctx
			consumer.cancel = cancel
			consumer.running = true

			mockClient := &MockClient{}
			mf := &MockFetches{}
			mh := &MockHandler{}
			consumer.client = mockClient
			consumer.handler = mh

			if tt.setup != nil {
				tt.setup(mockClient, mf, mh)
			}

			done := make(chan struct{})
			consumer.wg.Add(1)
			go func() {
				consumer.pollLoop()
				close(done)
			}()

			// Let the poll loop process the records and continue
			time.Sleep(150 * time.Millisecond)

			// If expect continue, the loop should still be running
			if tt.expectContinue {
				s.True(consumer.IsRunning(), "consumer should still be running after message panic")
			}

			cancel()

			select {
			case <-done:
				// exited as expected
			case <-time.After(1 * time.Second):
				s.Fail("pollLoop did not exit in time")
			}

			consumer.wg.Wait()

			mockClient.AssertExpectations(s.T())
			mf.AssertExpectations(s.T())
			mh.AssertExpectations(s.T())
		})
	}
}

// TestManageFetchStatePanic tests panic recovery in the fetch state manager
func (s *ConsumerTestSuite) TestManageFetchStatePanic() {
	s.Run("panic in manageFetchState is recovered", func() {
		consumer := GetConsumer()
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		consumer.ctx = ctx
		consumer.cancel = cancel
		consumer.consecutiveFailureThreshold = 5
		consumer.resumeDelaySeconds = 1

		// Force a panic by setting consecutive failures to a very high value
		// that will trigger pause logic
		consumer.consecutiveFailures.Store(1000)

		mockClient := &MockClient{}
		consumer.client = mockClient

		// Mock the pause operation to panic
		mockClient.On("PauseFetchTopics", mock.Anything).Run(func(args mock.Arguments) {
			panic("fetch state manager panic")
		}).Once()

		done := make(chan struct{})
		consumer.wg.Add(1)
		go func() {
			consumer.startManageFetchState()
			close(done)
		}()

		// Wait for context timeout or goroutine exit
		select {
		case <-done:
			// Goroutine exited (recovered from panic or context done)
		case <-time.After(1 * time.Second):
			s.Fail("startManageFetchState did not exit in time")
		}

		cancel()
		consumer.wg.Wait()

		// The panic should have been caught and logged, goroutine should exit cleanly
	})
}

// TestPanicRecoveryDoesNotCommitRecord verifies that panicked records are not committed
func (s *ConsumerTestSuite) TestPanicRecoveryDoesNotCommitRecord() {
	s.Run("panicked record is not committed", func() {
		consumer := GetConsumer()
		ctx, cancel := context.WithCancel(context.Background())
		consumer.ctx = ctx
		consumer.cancel = cancel

		mockClient := &MockClient{}
		mf := &MockFetches{}
		mh := &MockHandler{}
		consumer.client = mockClient
		consumer.handler = mh

		// Track which records were committed
		committedRecords := make([]*kgo.Record, 0)
		mockClient.On("MarkCommitRecords", mock.Anything).Run(func(args mock.Arguments) {
			records := args.Get(0).([]*kgo.Record)
			committedRecords = append(committedRecords, records...)
		})

		mf.On("Errors").Return([]*kgo.FetchError{})

		// Allow continued polling with empty fetches
		emptyFetches := &MockFetches{}
		emptyFetches.On("Errors").Return([]*kgo.FetchError{})
		emptyFetches.On("EachRecord", mock.Anything).Return()

		mockClient.On("PollFetches", mock.Anything).Return(mf).Once()
		mockClient.On("PollFetches", mock.Anything).Return(emptyFetches).Maybe()

		rec1 := &kgo.Record{Topic: "test", Partition: 0, Offset: 100}
		rec2 := &kgo.Record{Topic: "test", Partition: 0, Offset: 101}
		rec3 := &kgo.Record{Topic: "test", Partition: 0, Offset: 102}

		mf.On("EachRecord", mock.Anything).Run(func(args mock.Arguments) {
			fn := args.Get(0).(func(*kgo.Record))

			// Record 1: success
			mh.On("HandleMessage", mock.Anything, rec1).Return(Accepted, nil).Once()
			fn(rec1)

			// Record 2: panic
			mh.On("HandleMessage", mock.Anything, rec2).Run(func(args mock.Arguments) {
				panic("processing error")
			}).Return(Failed, errors.New("not reached")).Once()
			fn(rec2)

			// Record 3: success
			mh.On("HandleMessage", mock.Anything, rec3).Return(Accepted, nil).Once()
			fn(rec3)
		})

		done := make(chan struct{})
		consumer.wg.Add(1)
		go func() {
			consumer.pollLoop()
			close(done)
		}()

		// Let processing happen
		time.Sleep(150 * time.Millisecond)
		cancel()

		select {
		case <-done:
		case <-time.After(1 * time.Second):
			s.Fail("pollLoop did not exit in time")
		}

		consumer.wg.Wait()

		// Verify: rec1 and rec3 should be committed, rec2 should NOT
		s.Len(committedRecords, 2, "should have committed 2 records")
		offsets := make([]int64, 0, len(committedRecords))
		for _, rec := range committedRecords {
			offsets = append(offsets, rec.Offset)
		}
		s.Contains(offsets, int64(100), "should have committed record 1")
		s.Contains(offsets, int64(102), "should have committed record 3")
		s.NotContains(offsets, int64(101), "should NOT have committed panicked record 2")
	})
}
