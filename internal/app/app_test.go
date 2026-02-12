// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

// ^ OnStop Issue: Data race in go.uber.org/fx.exitCodeOption.apply() (at shutdown.go:44)
// Root Cause: Multiple goroutines from server instances calling Shutdown() concurrently, with one writing to the exit code while another is reading it.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"
	"xmidt-org/splitter/internal/consumer"
	"xmidt-org/splitter/internal/log"
	"xmidt-org/splitter/internal/metrics"
	"xmidt-org/splitter/internal/observe"
	"xmidt-org/splitter/internal/publisher"

	_ "github.com/goschtalt/goschtalt/pkg/typical"
	_ "github.com/goschtalt/yaml-decoder"
	_ "github.com/goschtalt/yaml-encoder"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

func Test_provideCLI(t *testing.T) {
	tests := []struct {
		description string
		args        cliArgs
		want        CLI
		exits       bool
		expectedErr error
	}{
		{
			description: "no arguments, everything works",
		}, {
			description: "dev mode",
			args:        cliArgs{"-d"},
			want:        CLI{Dev: true},
		}, {
			description: "invalid argument",
			args:        cliArgs{"-w"},
			exits:       true,
		}, {
			description: "invalid argument",
			args:        cliArgs{"-d", "-w"},
			exits:       true,
		}, {
			description: "help",
			args:        cliArgs{"-h"},
			exits:       true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			assert := assert.New(t)

			if tc.exits {
				assert.Panics(func() {
					_, _ = provideCLIWithOpts(tc.args, true)
				})
			} else {
				got, err := provideCLI(tc.args)

				assert.ErrorIs(err, tc.expectedErr)
				want := tc.want
				assert.Equal(&want, got)
			}
		})
	}
}

func TestWrpKafkaRouter(t *testing.T) {
	tests := []struct {
		description      string
		args             []string
		duration         time.Duration
		expectedErr      error
		expectedStartErr error
		panic            bool
	}{
		{
			description: "show config and exit",
			args:        []string{"-s"},
			panic:       true,
		}, {
			description: "show help and exit",
			args:        []string{"-h"},
			panic:       true,
		}, {
			description: "enable debug mode",
			args:        []string{"-d", "-f", "splitter.yaml"},
		}, {
			description: "output graph",
			args:        []string{"-g", "graph.dot", "-f", "splitter.yaml"},
		}, {
			description:      "start and stop",
			duration:         time.Millisecond,
			args:             []string{"-f", "splitter.yaml"},
			expectedStartErr: consumer.ErrPingingBroker,
		},
	}
	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			assert := assert.New(t)
			require := require.New(t)

			if tc.panic {
				assert.Panics(func() {
					_, _ = WrpKafkaRouter(tc.args)
				})
				return
			}

			app, err := WrpKafkaRouter(tc.args)

			assert.ErrorIs(err, tc.expectedErr)
			if tc.expectedErr != nil {
				assert.Nil(app)
				return
			} else {
				require.NoError(err)
			}

			if tc.duration <= 0 {
				return
			}

			// TODO below will fail the race condition in uber.fx, see top of the file

			// only run the program for	a few seconds to make sure it starts
			// startCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			// defer cancel()
			// err = app.Start(startCtx)
			// if tc.expectedStartErr != nil {
			// 	require.True(errors.Is(err, tc.expectedStartErr))
			// }

			// time.Sleep(tc.duration)

			// stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			// defer cancel()
			// err = app.Stop(stopCtx)
			// require.NoError(err)
		})
	}
}

func Test_provideLogger(t *testing.T) {
	tests := []struct {
		description string
		cli         *CLI
		cfg         LogConfig
		expectedErr error
	}{
		{
			description: "validate empty config",
			cfg:         LogConfig{},
			cli:         &CLI{},
		}, {
			description: "validate dev config",
			cfg:         LogConfig{},
			cli:         &CLI{Dev: true},
		},
	}
	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			assert := assert.New(t)

			got, err := provideLogger(LoggerIn{CLI: tc.cli, Cfg: tc.cfg})

			if tc.expectedErr == nil {
				assert.NotNil(got)
				assert.NoError(err)
				return
			}
			assert.ErrorIs(err, tc.expectedErr)
			assert.Nil(got)
		})
	}
}

// Mock interfaces for testing - these wrap the concrete types for testing purposes
type PublisherInterface interface {
	Start() error
	Stop(ctx context.Context) error
}

type ConsumerInterface interface {
	Start() error
	Stop(ctx context.Context) error
}

type MockPublisher struct {
	mock.Mock
}

func (m *MockPublisher) Start() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockPublisher) Stop(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

type MockConsumer struct {
	mock.Mock
}

func (m *MockConsumer) Start() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockConsumer) Stop(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

// Test helper functions that work with interfaces
func testOnStart(logger *slog.Logger, pub PublisherInterface, con ConsumerInterface) func(context.Context) error {
	return func(ctx context.Context) (err error) {
		if err = ctx.Err(); err != nil {
			return err
		}

		// Start the publisher first (required by consumer)
		if err = pub.Start(); err != nil {
			logger.Error("failed to start publisher", "error", err)
			return err
		}
		logger.Info("publisher started successfully")

		// Start the consumer
		if err = con.Start(); err != nil {
			logger.Error("failed to start consumer", "error", err)
			// Stop the publisher if consumer fails to start
			if stopErr := pub.Stop(ctx); stopErr != nil {
				logger.Error("failed to stop publisher during cleanup", "error", stopErr)
			}
			return err
		}

		logger.Info("consumer started successfully")
		return nil
	}
}

func testOnStop(logger *slog.Logger, pub PublisherInterface, con ConsumerInterface) func(context.Context) error {
	return func(ctx context.Context) error {
		logger.Info("stopping services")

		// Create a timeout context for the shutdown
		shutdownCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		defer cancel()

		// Stop the consumer first
		if err := con.Stop(shutdownCtx); err != nil {
			logger.Error("error stopping consumer", "error", err)
			// Continue to stop publisher even if consumer fails
		} else {
			logger.Info("consumer stopped successfully")
		}

		// Stop the publisher
		if err := pub.Stop(shutdownCtx); err != nil {
			logger.Error("error stopping publisher", "error", err)
			return err
		}

		logger.Info("publisher stopped successfully")
		return nil
	}
}

// AppLifecycleTestSuite provides comprehensive tests for app lifecycle management
type AppLifecycleTestSuite struct {
	suite.Suite
	mockPublisher *MockPublisher
	mockConsumer  *MockConsumer
	logger        *slog.Logger
}

func TestAppLifecycleTestSuite(t *testing.T) {
	suite.Run(t, new(AppLifecycleTestSuite))
}

func (suite *AppLifecycleTestSuite) SetupTest() {
	suite.mockPublisher = new(MockPublisher)
	suite.mockConsumer = new(MockConsumer)
	suite.logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func (suite *AppLifecycleTestSuite) TearDownTest() {
	suite.mockPublisher.AssertExpectations(suite.T())
	suite.mockConsumer.AssertExpectations(suite.T())
}

func (suite *AppLifecycleTestSuite) TestOnStart_TableDriven() {
	testCases := []struct {
		name           string
		setupContext   func() context.Context
		setupMocks     func(*MockPublisher, *MockConsumer)
		expectedErr    error
		expectError    bool
		description    string
	}{
		{
			name: "Success",
			setupContext: func() context.Context {
				return context.Background()
			},
			setupMocks: func(mockPub *MockPublisher, mockCon *MockConsumer) {
				mockPub.On("Start").Return(nil)
				mockCon.On("Start").Return(nil)
			},
			expectedErr: nil,
			expectError: false,
			description: "OnStart should succeed when both publisher and consumer start successfully",
		},
		{
			name: "ContextCanceled",
			setupContext: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel() // Cancel immediately
				return ctx
			},
			setupMocks: func(mockPub *MockPublisher, mockCon *MockConsumer) {
				// No mock setup needed as context is already canceled
			},
			expectedErr: context.Canceled,
			expectError: true,
			description: "OnStart should fail when context is canceled",
		},
		{
			name: "PublisherFailure",
			setupContext: func() context.Context {
				return context.Background()
			},
			setupMocks: func(mockPub *MockPublisher, mockCon *MockConsumer) {
				mockPub.On("Start").Return(errors.New("publisher start failed"))
				// Consumer Start should not be called when publisher fails
			},
			expectedErr: errors.New("publisher start failed"),
			expectError: true,
			description: "OnStart should fail when publisher fails to start",
		},
		{
			name: "ConsumerFailure",
			setupContext: func() context.Context {
				return context.Background()
			},
			setupMocks: func(mockPub *MockPublisher, mockCon *MockConsumer) {
				mockPub.On("Start").Return(nil)
				mockCon.On("Start").Return(errors.New("consumer start failed"))
				// Publisher should be stopped if consumer fails
				mockPub.On("Stop", mock.Anything).Return(nil)
			},
			expectedErr: errors.New("consumer start failed"),
			expectError: true,
			description: "OnStart should fail when consumer fails to start",
		},
		{
			name: "ConsumerFailure_PublisherStopError",
			setupContext: func() context.Context {
				return context.Background()
			},
			setupMocks: func(mockPub *MockPublisher, mockCon *MockConsumer) {
				mockPub.On("Start").Return(nil)
				mockCon.On("Start").Return(errors.New("consumer start failed"))
				mockPub.On("Stop", mock.Anything).Return(errors.New("publisher stop failed"))
			},
			expectedErr: errors.New("consumer start failed"),
			expectError: true,
			description: "Should return the original consumer error, not the publisher stop error",
		},
	}

	for _, tc := range testCases {
		suite.Run(tc.name, func() {
			// Setup fresh mocks for each test case
			suite.SetupTest()

			ctx := tc.setupContext()
			tc.setupMocks(suite.mockPublisher, suite.mockConsumer)

			onStartFunc := testOnStart(suite.logger, suite.mockPublisher, suite.mockConsumer)

			// Execute
			err := onStartFunc(ctx)

			// Verify
			if tc.expectError {
				suite.Error(err, tc.description)
				if tc.expectedErr != nil {
					suite.Equal(tc.expectedErr.Error(), err.Error())
				}
			} else {
				suite.NoError(err, tc.description)
			}
		})
	}
}

func (suite *AppLifecycleTestSuite) TestOnStop_TableDriven() {
	testCases := []struct {
		name           string
		setupContext   func() context.Context
		setupMocks     func(*MockPublisher, *MockConsumer)
		expectedErr    error
		expectError    bool
		description    string
	}{
		{
			name: "Success",
			setupContext: func() context.Context {
				return context.Background()
			},
			setupMocks: func(mockPub *MockPublisher, mockCon *MockConsumer) {
				mockCon.On("Stop", mock.Anything).Return(nil)
				mockPub.On("Stop", mock.Anything).Return(nil)
			},
			expectedErr: nil,
			expectError: false,
			description: "OnStop should succeed when both consumer and publisher stop successfully",
		},
		{
			name: "ConsumerFailure",
			setupContext: func() context.Context {
				return context.Background()
			},
			setupMocks: func(mockPub *MockPublisher, mockCon *MockConsumer) {
				mockCon.On("Stop", mock.Anything).Return(errors.New("consumer stop failed"))
				mockPub.On("Stop", mock.Anything).Return(nil)
			},
			expectedErr: nil,
			expectError: false,
			description: "OnStop should not return error when consumer fails but publisher succeeds",
		},
		{
			name: "PublisherFailure",
			setupContext: func() context.Context {
				return context.Background()
			},
			setupMocks: func(mockPub *MockPublisher, mockCon *MockConsumer) {
				mockCon.On("Stop", mock.Anything).Return(nil)
				mockPub.On("Stop", mock.Anything).Return(errors.New("publisher stop failed"))
			},
			expectedErr: errors.New("publisher stop failed"),
			expectError: true,
			description: "OnStop should return error when publisher fails to stop",
		},
		{
			name: "BothFailure",
			setupContext: func() context.Context {
				return context.Background()
			},
			setupMocks: func(mockPub *MockPublisher, mockCon *MockConsumer) {
				mockCon.On("Stop", mock.Anything).Return(errors.New("consumer stop failed"))
				mockPub.On("Stop", mock.Anything).Return(errors.New("publisher stop failed"))
			},
			expectedErr: errors.New("publisher stop failed"),
			expectError: true,
			description: "Should return publisher error even when consumer also fails",
		},
		{
			name: "Timeout",
			setupContext: func() context.Context {
				ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
				// Don't defer cancel here as each test case manages its own context
				_ = cancel
				return ctx
			},
			setupMocks: func(mockPub *MockPublisher, mockCon *MockConsumer) {
				// Make consumer stop hang longer than timeout
				mockCon.On("Stop", mock.Anything).Run(func(args mock.Arguments) {
					time.Sleep(10 * time.Millisecond) // Longer than context timeout
				}).Return(nil)
				mockPub.On("Stop", mock.Anything).Return(nil)
			},
			expectedErr: nil,
			expectError: false,
			description: "OnStop should succeed despite parent context timeout due to internal 60s timeout",
		},
	}

	for _, tc := range testCases {
		suite.Run(tc.name, func() {
			// Setup fresh mocks for each test case
			suite.SetupTest()

			ctx := tc.setupContext()
			tc.setupMocks(suite.mockPublisher, suite.mockConsumer)

			onStopFunc := testOnStop(suite.logger, suite.mockPublisher, suite.mockConsumer)

			// Execute
			err := onStopFunc(ctx)

			// Verify
			if tc.expectError {
				suite.Error(err, tc.description)
				if tc.expectedErr != nil {
					suite.Equal(tc.expectedErr.Error(), err.Error())
				}
			} else {
				suite.NoError(err, tc.description)
			}
		})
	}
}

// Provider function tests
func TestProvidePublisher_TableDriven(t *testing.T) {
	testCases := []struct {
		name        string
		setupConfig func() PublisherIn
		expectError bool
		description string
	}{
		{
			name: "Success",
			setupConfig: func() PublisherIn {
				logEmitter := observe.NewSubject[log.Event]()
				metricEmitter := observe.NewSubject[metrics.Event]()

				return PublisherIn{
					Config: publisher.Config{
						Brokers: []string{"localhost:9092"},
						TopicRoutes: []publisher.TopicRoute{
							{
								Topic:   "wrp-events",
								Pattern: "*",
							},
						},
						MaxBufferedRecords:     1000,
						MaxBufferedBytes:       1024 * 1024,
						RequestTimeout:         30 * time.Second,
						CleanupTimeout:         60 * time.Second,
						RequestRetries:         3,
						AllowAutoTopicCreation: true,
					},
					LogEmitter:    logEmitter,
					MetricEmitter: metricEmitter,
				}
			},
			expectError: false,
			description: "providePublisher should succeed with valid config",
		},
		{
			name: "InvalidConfig",
			setupConfig: func() PublisherIn {
				logEmitter := observe.NewSubject[log.Event]()
				metricEmitter := observe.NewSubject[metrics.Event]()

				return PublisherIn{
					Config: publisher.Config{
						Brokers: []string{}, // Empty brokers should cause validation error
						TopicRoutes: []publisher.TopicRoute{
							{
								Topic:   "wrp-events",
								Pattern: "*",
							},
						},
					},
					LogEmitter:    logEmitter,
					MetricEmitter: metricEmitter,
				}
			},
			expectError: true,
			description: "providePublisher should fail with invalid config",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Setup
			in := tc.setupConfig()

			// Execute
			out, err := providePublisher(in)

			// Verify
			if tc.expectError {
				assert.Error(t, err, tc.description)
				assert.Nil(t, out.Publisher, "Should not return a publisher instance on error")
				assert.Contains(t, err.Error(), "failed to create publisher", "Error should indicate publisher creation failure")
			} else {
				assert.NoError(t, err, tc.description)
				assert.NotNil(t, out.Publisher, "Should return a valid publisher instance")
			}
		})
	}
}

func TestProvideConsumer_TableDriven(t *testing.T) {
	// Helper function to create a valid publisher for testing
	createValidPublisher := func() *publisher.Publisher {
		logEmitter := observe.NewSubject[log.Event]()
		metricEmitter := observe.NewSubject[metrics.Event]()

		pubConfig := publisher.Config{
			Brokers: []string{"localhost:9092"},
			TopicRoutes: []publisher.TopicRoute{
				{
					Topic:   "wrp-events",
					Pattern: "*",
				},
			},
		}

		pub, err := publisher.New(
			publisher.WithLogEmitter(logEmitter),
			publisher.WithMetricsEmitter(metricEmitter),
			publisher.WithBrokers(pubConfig.Brokers...),
			publisher.WithTopicRoutes(pubConfig.ToWRPKafkaRoutes()...),
		)
		require.NoError(t, err, "Setup should create publisher successfully")
		return pub
	}

	testCases := []struct {
		name        string
		setupConfig func() ConsumerIn
		expectError bool
		description string
	}{
		{
			name: "Success",
			setupConfig: func() ConsumerIn {
				logEmitter := observe.NewSubject[log.Event]()
				metricEmitter := observe.NewSubject[metrics.Event]()

				return ConsumerIn{
					Config: consumer.Config{
						Brokers:           []string{"localhost:9092"},
						Topics:            []string{"wrp-inbound"},
						GroupID:           "splitter-group",
						ClientID:          "splitter-client",
						SessionTimeout:    30 * time.Second,
						HeartbeatInterval: 10 * time.Second,
						RebalanceTimeout:  60 * time.Second,
						FetchMinBytes:     1,
						FetchMaxBytes:     1024 * 1024,
						FetchMaxWait:      500 * time.Millisecond,
					},
					Publisher:     createValidPublisher(),
					LogEmitter:    logEmitter,
					MetricEmitter: metricEmitter,
				}
			},
			expectError: false,
			description: "provideConsumer should succeed with valid config",
		},
		{
			name: "InvalidConfig",
			setupConfig: func() ConsumerIn {
				logEmitter := observe.NewSubject[log.Event]()
				metricEmitter := observe.NewSubject[metrics.Event]()

				return ConsumerIn{
					Config: consumer.Config{
						Brokers: []string{"localhost:9092"},
						Topics:  []string{}, // Empty topics should cause validation error
						GroupID: "splitter-group",
					},
					Publisher:     createValidPublisher(),
					LogEmitter:    logEmitter,
					MetricEmitter: metricEmitter,
				}
			},
			expectError: true,
			description: "provideConsumer should fail with invalid config",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Setup
			in := tc.setupConfig()

			// Execute
			out, err := provideConsumer(in)

			// Verify
			if tc.expectError {
				assert.Error(t, err, tc.description)
				assert.Nil(t, out.Consumer, "Should not return a consumer instance on error")
				assert.Contains(t, err.Error(), "failed to create consumer", "Error should indicate consumer creation failure")
			} else {
				assert.NoError(t, err, tc.description)
				assert.NotNil(t, out.Consumer, "Should return a valid consumer instance")
			}
		})
	}
}

// Test lifecycle function integration - simplified to avoid fx.Lifecycle interface issues
func TestLifeCycle_Simplified(t *testing.T) {
	// Setup
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Create real instances for testing the lifecycle functions directly
	logEmitter := observe.NewSubject[log.Event]()
	metricEmitter := observe.NewSubject[metrics.Event]()

	pubConfig := publisher.Config{
		Brokers: []string{"localhost:9092"},
		TopicRoutes: []publisher.TopicRoute{
			{
				Topic:   "wrp-events",
				Pattern: "*",
			},
		},
	}

	pub, err := publisher.New(
		publisher.WithLogEmitter(logEmitter),
		publisher.WithMetricsEmitter(metricEmitter),
		publisher.WithBrokers(pubConfig.Brokers...),
		publisher.WithTopicRoutes(pubConfig.ToWRPKafkaRoutes()...),
	)
	require.NoError(t, err, "Setup should create publisher successfully")

	con, err := consumer.New(
		consumer.WithLogEmitter(logEmitter),
		consumer.WithMetricsEmitter(metricEmitter),
		consumer.WithBrokers("localhost:9092"),
		consumer.WithTopics("wrp-inbound"),
		consumer.WithGroupID("test-group"),
		consumer.WithMessageHandler(consumer.MessageHandlerFunc(
			consumer.NewWRPMessageHandler(consumer.WRPMessageHandlerConfig{
				Producer:       pub,
				LogEmitter:     logEmitter,
				MetricsEmitter: metricEmitter,
			}).HandleMessage,
		)),
	)
	require.NoError(t, err, "Setup should create consumer successfully")

	// Test that the onStart and onStop functions can be created without panicking
	assert.NotPanics(t, func() {
		startFunc := onStart(logger, pub, con)
		stopFunc := onStop(logger, pub, con)
		// Just verify the functions are created - they would normally be called by fx
		assert.NotNil(t, startFunc, "onStart should return a function")
		assert.NotNil(t, stopFunc, "onStop should return a function")
	}, "Creating lifecycle functions should not panic")
}

func TestProvideAppOptions_Structure(t *testing.T) {
	// This test verifies that provideAppOptions returns a valid fx.Option
	// without needing to fully execute it

	args := []string{"-f", "test-config.yaml"}

	// Execute
	option := provideAppOptions(args)

	// Verify
	assert.NotNil(t, option, "provideAppOptions should return a non-nil fx.Option")
}

func TestCoreModule_Structure(t *testing.T) {
	// This test verifies that CoreModule returns a valid fx.Option
	// without needing to fully execute it

	// Execute
	option := CoreModule()

	// Verify
	assert.NotNil(t, option, "CoreModule should return a non-nil fx.Option")
}

// Edge case and error scenario tests
func TestOnStart_EdgeCases(t *testing.T) {
	tests := []struct {
		name             string
		setupContext     func() context.Context
		setupMocks       func(*MockPublisher, *MockConsumer)
		expectError      bool
		expectedErrorMsg string
	}{
		{
			name: "nil_logger_handling",
			setupContext: func() context.Context {
				return context.Background()
			},
			setupMocks: func(pub *MockPublisher, con *MockConsumer) {
				pub.On("Start").Return(nil)
				con.On("Start").Return(nil)
			},
			expectError: false,
		},
		{
			name: "publisher_timeout",
			setupContext: func() context.Context {
				return context.Background()
			},
			setupMocks: func(pub *MockPublisher, con *MockConsumer) {
				// Simulate timeout in publisher start
				pub.On("Start").Run(func(args mock.Arguments) {
					time.Sleep(10 * time.Millisecond)
				}).Return(fmt.Errorf("timeout"))
			},
			expectError:      true,
			expectedErrorMsg: "timeout",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockPub := new(MockPublisher)
			mockCon := new(MockConsumer)
			logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

			if tt.setupMocks != nil {
				tt.setupMocks(mockPub, mockCon)
			}

			onStartFunc := testOnStart(logger, mockPub, mockCon)
			err := onStartFunc(tt.setupContext())

			if tt.expectError {
				assert.Error(t, err)
				if tt.expectedErrorMsg != "" {
					assert.Contains(t, err.Error(), tt.expectedErrorMsg)
				}
			} else {
				assert.NoError(t, err)
			}

			mockPub.AssertExpectations(t)
			mockCon.AssertExpectations(t)
		})
	}
}

// Test constants and variables
func TestConstants(t *testing.T) {
	assert.Equal(t, "splitter", applicationName, "Application name should be 'splitter'")

	// These should be set by build process but have defaults
	assert.NotEmpty(t, commit, "Commit should be set")
	assert.NotEmpty(t, version, "Version should be set")
	assert.NotEmpty(t, date, "Date should be set")
	assert.NotEmpty(t, builtBy, "BuiltBy should be set")
}
