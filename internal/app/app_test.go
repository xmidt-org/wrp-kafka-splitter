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

	"xmidt-org/splitter/internal/bucket"
	"xmidt-org/splitter/internal/consumer"
	"xmidt-org/splitter/internal/log"
	"xmidt-org/splitter/internal/metrics"
	"xmidt-org/splitter/internal/observe"
	"xmidt-org/splitter/internal/publisher"

	_ "github.com/goschtalt/goschtalt/pkg/typical"
	_ "github.com/goschtalt/yaml-decoder"
	_ "github.com/goschtalt/yaml-encoder"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/xmidt-org/touchstone"
	"github.com/xmidt-org/wrp-go/v5"
	"github.com/xmidt-org/wrpkafka"
)

// Comprehensive tests for CLI provider functions
func TestProvideCLI(t *testing.T) {
	testCases := []struct {
		name        string
		args        cliArgs
		testOpts    bool // Test provideCLIWithOpts instead of provideCLI
		expectError bool
		expectPanic bool // For cases that should panic (help, invalid args)
		validateCLI func(*testing.T, *CLI)
		description string
	}{
		// Basic functionality tests
		{
			name:        "NoArgs_ProvideCLI",
			args:        cliArgs{},
			expectError: false,
			description: "provideCLI should succeed with no arguments",
			validateCLI: func(t *testing.T, cli *CLI) {
				assert.NotNil(t, cli, "CLI should not be nil")
				assert.False(t, cli.Dev, "Dev should be false by default")
				assert.False(t, cli.Show, "Show should be false by default")
				assert.Empty(t, cli.Files, "Files should be empty by default")
				assert.Empty(t, cli.Default, "Default should be empty")
				assert.Empty(t, cli.Graph, "Graph should be empty")
			},
		},
		{
			name:        "DevMode",
			args:        cliArgs{"-d"},
			expectError: false,
			description: "provideCLI should handle dev mode flag",
			validateCLI: func(t *testing.T, cli *CLI) {
				assert.True(t, cli.Dev, "Dev should be true")
			},
		},
		{
			name:        "ShowConfig",
			args:        cliArgs{"-s"},
			expectError: false,
			description: "provideCLI should handle show config flag",
			validateCLI: func(t *testing.T, cli *CLI) {
				assert.True(t, cli.Show, "Show should be true")
			},
		},
		{
			name:        "ConfigFile",
			args:        cliArgs{"-f", "config.yaml"},
			expectError: false,
			description: "provideCLI should handle config file",
			validateCLI: func(t *testing.T, cli *CLI) {
				assert.Contains(t, cli.Files, "config.yaml", "Files should contain config.yaml")
			},
		},
		{
			name:        "MultipleConfigFiles",
			args:        cliArgs{"-f", "config1.yaml", "-f", "config2.yaml"},
			expectError: false,
			description: "provideCLI should handle multiple config files",
			validateCLI: func(t *testing.T, cli *CLI) {
				assert.Contains(t, cli.Files, "config1.yaml")
				assert.Contains(t, cli.Files, "config2.yaml")
				assert.Len(t, cli.Files, 2)
			},
		},
		{
			name:        "GraphFlag",
			args:        cliArgs{"-g", "graph.dot"},
			expectError: false,
			description: "provideCLI should handle graph output flag",
			validateCLI: func(t *testing.T, cli *CLI) {
				assert.Equal(t, "graph.dot", cli.Graph)
			},
		},
		{
			name:        "DefaultFlag",
			args:        cliArgs{"--default", "default.yaml"},
			expectError: false,
			description: "provideCLI should handle default output flag",
			validateCLI: func(t *testing.T, cli *CLI) {
				assert.Equal(t, "default.yaml", cli.Default)
			},
		},
		{
			name:        "CombinedFlags",
			args:        cliArgs{"-d", "-s", "-f", "config.yaml", "-g", "graph.dot"},
			expectError: false,
			description: "provideCLI should handle multiple combined flags",
			validateCLI: func(t *testing.T, cli *CLI) {
				assert.True(t, cli.Dev, "Dev should be true")
				assert.True(t, cli.Show, "Show should be true")
				assert.Contains(t, cli.Files, "config.yaml")
				assert.Equal(t, "graph.dot", cli.Graph)
			},
		},

		// Error and panic cases - removed invalid flag tests since Kong CLI exits rather than panics

		// provideCLIWithOpts specific tests
		{
			name:        "NoArgs_ProvideCLIWithOpts",
			args:        cliArgs{},
			testOpts:    true,
			expectError: false,
			description: "provideCLIWithOpts should succeed with no arguments",
			validateCLI: func(t *testing.T, cli *CLI) {
				assert.NotNil(t, cli, "CLI should not be nil")
				assert.False(t, cli.Dev, "Dev should be false by default")
			},
		},
		{
			name:        "DevFlag_ProvideCLIWithOpts",
			args:        cliArgs{"-d"},
			testOpts:    true,
			expectError: false,
			description: "provideCLIWithOpts should handle dev flag",
			validateCLI: func(t *testing.T, cli *CLI) {
				assert.True(t, cli.Dev, "Dev should be true")
			},
		},
		{
			name:        "ConfigFiles_ProvideCLIWithOpts",
			args:        cliArgs{"-f", "config1.yaml", "-f", "config2.yaml"},
			testOpts:    true,
			expectError: false,
			description: "provideCLIWithOpts should handle multiple config files",
			validateCLI: func(t *testing.T, cli *CLI) {
				assert.Contains(t, cli.Files, "config1.yaml")
				assert.Contains(t, cli.Files, "config2.yaml")
				assert.Len(t, cli.Files, 2)
			},
		},

		// Edge cases
		{
			name:        "EmptyConfigFileName",
			args:        cliArgs{"-f", ""},
			expectError: false,
			description: "provideCLI should handle empty config filename gracefully",
			validateCLI: func(t *testing.T, cli *CLI) {
				// Empty config filename should be filtered out or ignored
				assert.NotContains(t, cli.Files, "", "Files should not contain empty string")
			},
		},
		{
			name:        "SpacesInFileName",
			args:        cliArgs{"-f", "config with spaces.yaml"},
			expectError: false,
			description: "provideCLI should handle filenames with spaces",
			validateCLI: func(t *testing.T, cli *CLI) {
				assert.Contains(t, cli.Files, "config with spaces.yaml")
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.expectPanic {
				assert.Panics(t, func() {
					if tc.testOpts {
						_, _ = provideCLIWithOpts(tc.args, true)
					} else {
						_, _ = provideCLI(tc.args)
					}
				}, tc.description)
				return
			}

			var cli *CLI
			var err error

			if tc.testOpts {
				cli, err = provideCLIWithOpts(tc.args, false) // Don't exit on error for testing
			} else {
				cli, err = provideCLI(tc.args)
			}

			if tc.expectError {
				assert.Error(t, err, tc.description)
				assert.Nil(t, cli, "CLI should be nil on error")
			} else {
				assert.NoError(t, err, tc.description)
				assert.NotNil(t, cli, "CLI should not be nil")
				if tc.validateCLI != nil {
					tc.validateCLI(t, cli)
				}
			}
		})
	}
}

func TestProvideLogger(t *testing.T) {
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

// Mock implementations for testing
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

func (m *MockPublisher) Produce(ctx context.Context, msg *wrp.Message) (wrpkafka.Outcome, error) {
	args := m.Called(ctx, msg)
	return args.Get(0).(wrpkafka.Outcome), args.Error(1)
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

func (m *MockConsumer) IsRunning() bool {
	args := m.Called()
	return args.Bool(0)
}

// Provider function tests
func TestProvidePublisher(t *testing.T) {
	testCases := []struct {
		name        string
		setupConfig func() PublisherIn
		expectError bool
		description string
		verify      func(*testing.T, PublisherOut)
	}{
		{
			name: "Success",
			setupConfig: func() PublisherIn {
				logEmitter := observe.NewSubject[log.Event]()
				metricEmitter := observe.NewSubject[metrics.Event]()

				return PublisherIn{
					Config: publisher.Config{
						Brokers: publisher.Brokers{
							RestartOnConfigChange: false,
							TargetRegion:          "us-east-1",
							Regions: map[string][]string{
								"us-east-1": {"localhost:9092"},
							},
						},
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
					PrometheusConfig: touchstone.Config{
						DefaultNamespace: "xmidt",
						DefaultSubsystem: "splitter",
					},
				}
			},
			expectError: false,
			description: "providePublisher should succeed with valid config",
			verify: func(t *testing.T, out PublisherOut) {
				assert.NotNil(t, out.Publisher, "Should return a valid publisher instance")
			},
		},
		{
			name: "Success_WithPrometheusConfig",
			setupConfig: func() PublisherIn {
				logEmitter := observe.NewSubject[log.Event]()
				metricEmitter := observe.NewSubject[metrics.Event]()

				return PublisherIn{
					Config: publisher.Config{
						Brokers: publisher.Brokers{
							RestartOnConfigChange: false,
							TargetRegion:          "us-east-1",
							Regions: map[string][]string{
								"us-east-1": {"localhost:9092"},
							},
						},
						TopicRoutes: []publisher.TopicRoute{
							{
								Topic:   "wrp-events",
								Pattern: "*",
							},
						},
					},
					LogEmitter:    logEmitter,
					MetricEmitter: metricEmitter,
					PrometheusConfig: touchstone.Config{
						DefaultNamespace: "test_namespace",
						DefaultSubsystem: "test_subsystem",
					},
				}
			},
			expectError: false,
			description: "providePublisher should properly pass Prometheus config to publisher",
			verify: func(t *testing.T, out PublisherOut) {
				assert.NotNil(t, out.Publisher, "Should return a valid publisher instance")
				// Publisher creation succeeds when prometheus config is provided
				// Detailed prometheus config verification is handled by publisher package tests
			},
		},
		{
			name: "Success_WithEmptyPrometheusConfig",
			setupConfig: func() PublisherIn {
				logEmitter := observe.NewSubject[log.Event]()
				metricEmitter := observe.NewSubject[metrics.Event]()

				return PublisherIn{
					Config: publisher.Config{
						Brokers: publisher.Brokers{
							RestartOnConfigChange: false,
							TargetRegion:          "us-east-1",
							Regions: map[string][]string{
								"us-east-1": {"localhost:9092"},
							},
						},
						TopicRoutes: []publisher.TopicRoute{
							{
								Topic:   "wrp-events",
								Pattern: "*",
							},
						},
					},
					LogEmitter:    logEmitter,
					MetricEmitter: metricEmitter,
					PrometheusConfig: touchstone.Config{
						DefaultNamespace: "",
						DefaultSubsystem: "",
					},
				}
			},
			expectError: false,
			description: "providePublisher should succeed with empty Prometheus config",
			verify: func(t *testing.T, out PublisherOut) {
				assert.NotNil(t, out.Publisher, "Should return a valid publisher instance")
				// Publisher creation succeeds when prometheus config is provided, even if empty
			},
		},
		{
			name: "InvalidConfig",
			setupConfig: func() PublisherIn {
				logEmitter := observe.NewSubject[log.Event]()
				metricEmitter := observe.NewSubject[metrics.Event]()

				return PublisherIn{
					Config: publisher.Config{
						Brokers: publisher.Brokers{
							RestartOnConfigChange: false,
							TargetRegion:          "nonexistent-region",
							Regions: map[string][]string{
								"us-east-1": {"localhost:9092"},
							},
						}, // Target region not in regions map should cause validation error
						TopicRoutes: []publisher.TopicRoute{
							{
								Topic:   "wrp-events",
								Pattern: "*",
							},
						},
					},
					LogEmitter:    logEmitter,
					MetricEmitter: metricEmitter,
					PrometheusConfig: touchstone.Config{
						DefaultNamespace: "xmidt",
						DefaultSubsystem: "splitter",
					},
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
				if tc.verify != nil {
					tc.verify(t, out)
				}
			}
		})
	}
}

func TestProvideConsumer(t *testing.T) {
	// Helper function to create a valid publisher for testing
	createValidKafkaPublisher := func() *publisher.KafkaPublisher {
		logEmitter := observe.NewSubject[log.Event]()
		metricEmitter := observe.NewSubject[metrics.Event]()

		pubConfig := publisher.Config{
			Brokers: publisher.Brokers{
				RestartOnConfigChange: false,
				TargetRegion:          "us-east-1",
				Regions: map[string][]string{
					"us-east-1": {"localhost:9092"},
				},
			},
			TopicRoutes: []publisher.TopicRoute{
				{
					Topic:   "wrp-events",
					Pattern: "*",
					HashKey: "source",
				},
			},
		}

		routes, err := pubConfig.ToWRPKafkaRoutes()
		require.NoError(t, err, "Setup should convert routes successfully")

		pub, err := publisher.New(
			publisher.WithLogEmitter(logEmitter),
			publisher.WithMetricsEmitter(metricEmitter),
			publisher.WithBrokers(pubConfig.Brokers),
			publisher.WithTopicRoutes(routes...),
		)
		require.NoError(t, err, "Setup should create publisher successfully")
		return pub
	}

	// Helper function to create a valid bucket configuration for testing
	createValidBucketConfig := func() bucket.Config {
		return bucket.Config{
			TargetBucket: "bucket-0",
			PossibleBuckets: []bucket.BucketSettings{
				{Name: "bucket-0", Threshold: 1.0},
			},
			PartitionKeyType: "source",
		}
	}

	// Helper function to create a valid Bucket instance for testing
	createValidBucket := func() bucket.Bucket {
		cfg := createValidBucketConfig()
		b, err := bucket.NewBuckets(cfg)
		require.NoError(t, err, "Setup should create bucket successfully")
		return b
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
					BucketConfig: createValidBucketConfig(),
					PrometheusConfig: touchstone.Config{
						DefaultNamespace: "xmidt",
						DefaultSubsystem: "test",
					},
					PrometheusRegisterer: prometheus.NewRegistry(),
					Publisher:            createValidKafkaPublisher(),
					LogEmitter:           logEmitter,
					MetricEmitter:        metricEmitter,
					Buckets:              createValidBucket(),
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
					BucketConfig: createValidBucketConfig(),
					PrometheusConfig: touchstone.Config{
						DefaultNamespace: "xmidt",
						DefaultSubsystem: "test",
					},
					PrometheusRegisterer: prometheus.NewRegistry(),
					Publisher:            createValidKafkaPublisher(),
					LogEmitter:           logEmitter,
					MetricEmitter:        metricEmitter,
					Buckets:              createValidBucket(),
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

func TestProvideAppOptions(t *testing.T) {
	// This test verifies that provideAppOptions returns a valid fx.Option
	// without needing to fully execute it

	args := []string{"-f", "test-config.yaml"}

	// Execute
	option := provideAppOptions(args)

	// Verify
	assert.NotNil(t, option, "provideAppOptions should return a non-nil fx.Option")
}

func TestCoreModule(t *testing.T) {
	// This test verifies that CoreModule returns a valid fx.Option
	// without needing to fully execute it

	// Execute
	option := CoreModule()

	// Verify
	assert.NotNil(t, option, "CoreModule should return a non-nil fx.Option")
}

// Comprehensive tests for onStart function
func TestOnStart(t *testing.T) {
	testCases := []struct {
		name           string
		setupContext   func() (context.Context, context.CancelFunc)
		setupMocks     func(*MockPublisher, *MockConsumer)
		expectedErr    string
		expectError    bool
		description    string
		verifyBehavior func(t *testing.T, mockPub *MockPublisher, mockCon *MockConsumer, err error)
		shouldPanic    bool
	}{
		// Basic success cases
		{
			name: "Success",
			setupContext: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			setupMocks: func(mockPub *MockPublisher, mockCon *MockConsumer) {
				mockPub.On("Start").Return(nil)
				mockCon.On("Start").Return(nil)
			},
			expectError: false,
			description: "OnStart should succeed when both publisher and consumer start successfully",
		},
		{
			name: "Success_NormalFlow",
			setupContext: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			setupMocks: func(mockPub *MockPublisher, mockCon *MockConsumer) {
				mockPub.On("Start").Return(nil)
				mockCon.On("Start").Return(nil)
			},
			expectError: false,
			description: "OnStart should succeed with normal publisher and consumer startup",
		},

		// Context-related test cases
		{
			name: "ContextDeadlineExceeded",
			setupContext: func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), 1*time.Nanosecond)
			},
			setupMocks: func(mockPub *MockPublisher, mockCon *MockConsumer) {
				// No mocks needed as context should be expired immediately
			},
			expectedErr: "context deadline exceeded",
			expectError: true,
			description: "OnStart should fail immediately with deadline exceeded context",
			verifyBehavior: func(t *testing.T, mockPub *MockPublisher, mockCon *MockConsumer, err error) {
				assert.Contains(t, err.Error(), "context deadline exceeded")
				// Verify that neither publisher nor consumer Start was called
				mockPub.AssertNotCalled(t, "Start")
				mockCon.AssertNotCalled(t, "Start")
			},
		},
		{
			name: "ContextCanceled",
			setupContext: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel() // Cancel immediately
				return ctx, cancel
			},
			setupMocks: func(mockPub *MockPublisher, mockCon *MockConsumer) {
				// No mocks needed as context is already canceled
			},
			expectedErr: "context canceled",
			expectError: true,
			description: "OnStart should fail when context is canceled",
		},
		{
			name: "ContextWithValue",
			setupContext: func() (context.Context, context.CancelFunc) {
				ctx := context.WithValue(context.Background(), "test-key", "test-value") //nolint
				return ctx, func() {}                                                    // No-op cancel
			},
			setupMocks: func(mockPub *MockPublisher, mockCon *MockConsumer) {
				mockPub.On("Start").Return(nil)
				mockCon.On("Start").Return(nil)
			},
			expectError: false,
			description: "OnStart should work with context containing values",
			verifyBehavior: func(t *testing.T, mockPub *MockPublisher, mockCon *MockConsumer, err error) {
				assert.NoError(t, err)
				mockPub.AssertCalled(t, "Start")
				mockCon.AssertCalled(t, "Start")
			},
		},

		// Publisher failure scenarios
		{
			name: "PublisherStartFailure",
			setupContext: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			setupMocks: func(mockPub *MockPublisher, mockCon *MockConsumer) {
				mockPub.On("Start").Return(errors.New("publisher start failed"))
				// Consumer Start should not be called when publisher fails
			},
			expectedErr: "publisher start failed",
			expectError: true,
			description: "OnStart should fail when publisher fails to start",
		},
		{
			name: "PublisherTimeout",
			setupContext: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			setupMocks: func(mockPub *MockPublisher, mockCon *MockConsumer) {
				// Simulate timeout in publisher start
				mockPub.On("Start").Run(func(args mock.Arguments) {
					time.Sleep(10 * time.Millisecond)
				}).Return(fmt.Errorf("publisher timeout"))
			},
			expectedErr: "publisher timeout",
			expectError: true,
			description: "OnStart should handle publisher timeouts",
		},
		{
			name: "PublisherStartPanic",
			setupContext: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			setupMocks: func(mockPub *MockPublisher, mockCon *MockConsumer) {
				mockPub.On("Start").Panic("publisher panic")
			},
			shouldPanic: true,
			description: "OnStart should panic if publisher.Start panics (no recovery expected)",
		},

		// Consumer failure scenarios with cleanup
		{
			name: "ConsumerStartFailureWithPublisherStopSuccess",
			setupContext: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			setupMocks: func(mockPub *MockPublisher, mockCon *MockConsumer) {
				mockPub.On("Start").Return(nil)
				mockCon.On("Start").Return(errors.New("consumer connection refused"))
				mockPub.On("Stop", mock.AnythingOfType("*context.cancelCtx")).Return(nil)
			},
			expectedErr: "consumer connection refused",
			expectError: true,
			description: "OnStart should properly cleanup publisher when consumer fails",
			verifyBehavior: func(t *testing.T, mockPub *MockPublisher, mockCon *MockConsumer, err error) {
				assert.Equal(t, "consumer connection refused", err.Error())
				// Verify cleanup was attempted
				mockPub.AssertCalled(t, "Stop", mock.AnythingOfType("*context.cancelCtx"))
			},
		},
		{
			name: "ConsumerStartFailureWithPublisherStopFailure",
			setupContext: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			setupMocks: func(mockPub *MockPublisher, mockCon *MockConsumer) {
				mockPub.On("Start").Return(nil)
				mockCon.On("Start").Return(errors.New("consumer startup failed"))
				mockPub.On("Stop", mock.AnythingOfType("*context.cancelCtx")).Return(errors.New("publisher cleanup failed"))
			},
			expectedErr: "consumer startup failed", // Original error should be preserved
			expectError: true,
			description: "OnStart should return original consumer error even if publisher cleanup fails",
			verifyBehavior: func(t *testing.T, mockPub *MockPublisher, mockCon *MockConsumer, err error) {
				assert.Equal(t, "consumer startup failed", err.Error())
				// Verify cleanup was attempted despite failure
				mockPub.AssertCalled(t, "Stop", mock.AnythingOfType("*context.cancelCtx"))
			},
		},
	}

	for _, tc := range testCases {
		// Handle panic test cases separately
		if tc.shouldPanic {
			t.Run(tc.name, func(t *testing.T) {
				mockPub := new(MockPublisher)
				mockCon := new(MockConsumer)
				logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

				ctx, cancel := tc.setupContext()
				defer cancel()

				tc.setupMocks(mockPub, mockCon)
				onStartFunc := onStart(logger, mockPub, mockCon)

				// Expect panic
				assert.Panics(t, func() {
					_ = onStartFunc(ctx)
				}, tc.description)
			})
			continue
		}

		t.Run(tc.name, func(t *testing.T) {
			mockPub := new(MockPublisher)
			mockCon := new(MockConsumer)
			logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

			ctx, cancel := tc.setupContext()
			defer cancel()

			tc.setupMocks(mockPub, mockCon)
			onStartFunc := onStart(logger, mockPub, mockCon)

			// Execute
			err := onStartFunc(ctx)

			// Verify error expectation
			if tc.expectError {
				assert.Error(t, err, tc.description)
				if tc.expectedErr != "" {
					assert.Contains(t, err.Error(), tc.expectedErr)
				}
			} else {
				assert.NoError(t, err, tc.description)
			}

			// Custom verification
			if tc.verifyBehavior != nil {
				tc.verifyBehavior(t, mockPub, mockCon, err)
			}

			mockPub.AssertExpectations(t)
			mockCon.AssertExpectations(t)
		})
	}
}

// Tests for provideConfig function
func TestProvideConfig(t *testing.T) {
	testCases := []struct {
		name           string
		setupCLI       func() *CLI
		setupFiles     []string // Files to create for testing
		fileContents   map[string]string
		expectError    bool
		validateConfig func(*testing.T, interface{})
		description    string
	}{
		{
			name: "EmptyFiles",
			setupCLI: func() *CLI {
				return &CLI{Files: []string{}}
			},
			expectError: false,
			description: "provideConfig should succeed with empty file list",
			validateConfig: func(t *testing.T, config interface{}) {
				assert.NotNil(t, config, "Config should not be nil")
			},
		},
		{
			name: "WithDevMode",
			setupCLI: func() *CLI {
				return &CLI{Dev: true, Files: []string{}}
			},
			expectError: false,
			description: "provideConfig should succeed with dev mode enabled",
			validateConfig: func(t *testing.T, config interface{}) {
				assert.NotNil(t, config, "Config should not be nil")
			},
		},
		{
			name: "WithValidConfigFile",
			setupCLI: func() *CLI {
				return &CLI{Files: []string{"test-valid.yaml"}}
			},
			setupFiles: []string{"test-valid.yaml"},
			fileContents: map[string]string{
				"test-valid.yaml": `
consumer:
  brokers:
    - localhost:9092
  group_id: test-group
  topics: ["test-topic"]
producer:
  brokers:
    - localhost:9092
  topic_routes:
    - pattern: "*"
      topic: output-topic
`,
			},
			expectError: false,
			description: "provideConfig should succeed with valid config file",
			validateConfig: func(t *testing.T, config interface{}) {
				assert.NotNil(t, config, "Config should not be nil")
			},
		},
		{
			name: "WithMultipleFiles",
			setupCLI: func() *CLI {
				return &CLI{Files: []string{"base.yaml", "override.yaml"}}
			},
			setupFiles: []string{"base.yaml", "override.yaml"},
			fileContents: map[string]string{
				"base.yaml": `
consumer:
  brokers:
    - localhost:9092
  group_id: base-group
`,
				"override.yaml": `
consumer:
  topics: ["override-topic"]
`,
			},
			expectError: false,
			description: "provideConfig should handle multiple config files for merging",
			validateConfig: func(t *testing.T, config interface{}) {
				assert.NotNil(t, config, "Config should not be nil")
			},
		},
		{
			name: "WithNonExistentFile",
			setupCLI: func() *CLI {
				return &CLI{Files: []string{"non-existent-file.yaml"}}
			},
			expectError: false, // Should not error, just ignore missing files
			description: "provideConfig should handle non-existent files gracefully",
			validateConfig: func(t *testing.T, config interface{}) {
				assert.NotNil(t, config, "Config should not be nil even with missing files")
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create temporary files if needed
			tempFiles := make([]string, 0, 2*len(tc.setupFiles))
			defer func() {
				for _, file := range tempFiles {
					os.Remove(file)
				}
			}()

			for _, filename := range tc.setupFiles {
				tempFile, err := os.CreateTemp("", filename)
				require.NoError(t, err)
				tempFiles = append(tempFiles, tempFile.Name())

				if content, exists := tc.fileContents[filename]; exists {
					_, err = tempFile.WriteString(content)
					require.NoError(t, err)
				}
				err = tempFile.Close()
				require.NoError(t, err)

				// Update CLI to use actual temp file path
				cli := tc.setupCLI()
				for i, file := range cli.Files {
					if file == filename {
						cli.Files[i] = tempFile.Name()
					}
				}
			}

			cli := tc.setupCLI()
			config, err := provideConfig(cli)

			if tc.expectError {
				assert.Error(t, err, tc.description)
				assert.Nil(t, config, "Config should be nil on error")
			} else {
				assert.NoError(t, err, tc.description)
				if tc.validateConfig != nil {
					tc.validateConfig(t, config)
				}
			}
		})
	}
}

// Test provideCLIWithOpts function
func TestConstants(t *testing.T) {
	assert.Equal(t, "splitter", applicationName, "Application name should be 'splitter'")

	// These should be set by build process but have defaults
	assert.NotEmpty(t, commit, "Commit should be set")
	assert.NotEmpty(t, version, "Version should be set")
	assert.NotEmpty(t, date, "Date should be set")
	assert.NotEmpty(t, builtBy, "BuiltBy should be set")
}

// Comprehensive tests for onStop function
func TestOnStop(t *testing.T) {
	testCases := []struct {
		name           string
		setupContext   func() (context.Context, context.CancelFunc)
		setupMocks     func(*MockPublisher, *MockConsumer)
		expectedErr    string
		expectError    bool
		description    string
		verifyBehavior func(t *testing.T, mockPub *MockPublisher, mockCon *MockConsumer, err error)
		shouldPanic    bool
	}{
		// Basic success cases
		{
			name: "Success",
			setupContext: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			setupMocks: func(mockPub *MockPublisher, mockCon *MockConsumer) {
				mockCon.On("Stop", mock.Anything).Return(nil)
				mockPub.On("Stop", mock.Anything).Return(nil)
			},
			expectError: false,
			description: "OnStop should succeed when both consumer and publisher stop successfully",
		},
		{
			name: "Success_BothStopSuccessfully",
			setupContext: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			setupMocks: func(mockPub *MockPublisher, mockCon *MockConsumer) {
				mockCon.On("Stop", mock.AnythingOfType("*context.timerCtx")).Return(nil)
				mockPub.On("Stop", mock.AnythingOfType("*context.timerCtx")).Return(nil)
			},
			expectError: false,
			description: "OnStop should succeed when both consumer and publisher stop successfully",
		},

		// Consumer failure scenarios
		{
			name: "ConsumerFailure_PublisherSucceeds",
			setupContext: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			setupMocks: func(mockPub *MockPublisher, mockCon *MockConsumer) {
				mockCon.On("Stop", mock.AnythingOfType("*context.timerCtx")).Return(errors.New("consumer stop failed"))
				mockPub.On("Stop", mock.AnythingOfType("*context.timerCtx")).Return(nil)
			},
			expectError: false,
			description: "OnStop should not return error when consumer fails but publisher succeeds",
		},
		{
			name: "ConsumerStopHangsButPublisherSucceeds",
			setupContext: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			setupMocks: func(mockPub *MockPublisher, mockCon *MockConsumer) {
				// Consumer hangs but eventually returns success
				mockCon.On("Stop", mock.AnythingOfType("*context.timerCtx")).Run(func(args mock.Arguments) {
					time.Sleep(50 * time.Millisecond) // Simulate slow shutdown
				}).Return(nil)
				mockPub.On("Stop", mock.AnythingOfType("*context.timerCtx")).Return(nil)
			},
			expectError: false,
			description: "OnStop should succeed even if consumer takes time to stop",
			verifyBehavior: func(t *testing.T, mockPub *MockPublisher, mockCon *MockConsumer, err error) {
				assert.NoError(t, err)
				// Both should be called with timeout context (60 second timeout)
				mockCon.AssertCalled(t, "Stop", mock.AnythingOfType("*context.timerCtx"))
				mockPub.AssertCalled(t, "Stop", mock.AnythingOfType("*context.timerCtx"))
			},
		},
		{
			name: "ConsumerStopPanic",
			setupContext: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			setupMocks: func(mockPub *MockPublisher, mockCon *MockConsumer) {
				mockCon.On("Stop", mock.AnythingOfType("*context.timerCtx")).Panic("consumer stop panic")
			},
			shouldPanic: true,
			description: "OnStop should panic if consumer.Stop panics (no recovery expected)",
		},

		// Publisher failure scenarios
		{
			name: "PublisherFailure_ConsumerSucceeds",
			setupContext: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			setupMocks: func(mockPub *MockPublisher, mockCon *MockConsumer) {
				mockCon.On("Stop", mock.AnythingOfType("*context.timerCtx")).Return(nil)
				mockPub.On("Stop", mock.AnythingOfType("*context.timerCtx")).Return(errors.New("publisher stop failed"))
			},
			expectedErr: "publisher stop failed",
			expectError: true,
			description: "OnStop should return error when publisher fails to stop",
		},
		{
			name: "PublisherStopHangsButSucceeds",
			setupContext: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			setupMocks: func(mockPub *MockPublisher, mockCon *MockConsumer) {
				mockCon.On("Stop", mock.AnythingOfType("*context.timerCtx")).Return(nil)
				// Publisher hangs but eventually succeeds
				mockPub.On("Stop", mock.AnythingOfType("*context.timerCtx")).Run(func(args mock.Arguments) {
					time.Sleep(50 * time.Millisecond) // Simulate slow shutdown
				}).Return(nil)
			},
			expectError: false,
			description: "OnStop should succeed even if publisher takes time to stop",
			verifyBehavior: func(t *testing.T, mockPub *MockPublisher, mockCon *MockConsumer, err error) {
				assert.NoError(t, err)
			},
		},
		{
			name: "PublisherStopPanic",
			setupContext: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			setupMocks: func(mockPub *MockPublisher, mockCon *MockConsumer) {
				mockCon.On("Stop", mock.AnythingOfType("*context.timerCtx")).Return(nil)
				mockPub.On("Stop", mock.AnythingOfType("*context.timerCtx")).Panic("publisher stop panic")
			},
			shouldPanic: true,
			description: "OnStop should panic if publisher.Stop panics (no recovery expected)",
		},

		// Both failure scenarios
		{
			name: "BothFailure_ReturnPublisherError",
			setupContext: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			setupMocks: func(mockPub *MockPublisher, mockCon *MockConsumer) {
				mockCon.On("Stop", mock.AnythingOfType("*context.timerCtx")).Return(errors.New("consumer stop failed"))
				mockPub.On("Stop", mock.AnythingOfType("*context.timerCtx")).Return(errors.New("publisher stop failed"))
			},
			expectedErr: "publisher stop failed",
			expectError: true,
			description: "Should return publisher error even when consumer also fails",
		},
		{
			name: "BothConsumerAndPublisherFailWithDifferentErrors",
			setupContext: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			setupMocks: func(mockPub *MockPublisher, mockCon *MockConsumer) {
				mockCon.On("Stop", mock.AnythingOfType("*context.timerCtx")).Return(errors.New("consumer shutdown error"))
				mockPub.On("Stop", mock.AnythingOfType("*context.timerCtx")).Return(errors.New("publisher shutdown error"))
			},
			expectedErr: "publisher shutdown error", // Publisher error should be returned
			expectError: true,
			description: "OnStop should return publisher error when both fail",
			verifyBehavior: func(t *testing.T, mockPub *MockPublisher, mockCon *MockConsumer, err error) {
				assert.Equal(t, "publisher shutdown error", err.Error())
				// Both should have been called
				mockCon.AssertCalled(t, "Stop", mock.AnythingOfType("*context.timerCtx"))
				mockPub.AssertCalled(t, "Stop", mock.AnythingOfType("*context.timerCtx"))
			},
		},

		// Context and timeout scenarios
		{
			name: "ParentContextTimeout_InternalTimeoutWorks",
			setupContext: func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), 1*time.Millisecond)
			},
			setupMocks: func(mockPub *MockPublisher, mockCon *MockConsumer) {
				// Make consumer stop hang longer than parent timeout but less than internal timeout
				mockCon.On("Stop", mock.Anything).Run(func(args mock.Arguments) {
					time.Sleep(10 * time.Millisecond) // Longer than parent context timeout
				}).Return(nil)
				mockPub.On("Stop", mock.Anything).Return(nil)
			},
			expectError: false,
			description: "OnStop should succeed despite parent context timeout due to internal 60s timeout",
		},
		{
			name: "ParentContextAlreadyCanceledButInternalTimeoutWorks",
			setupContext: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel() // Cancel immediately
				return ctx, cancel
			},
			setupMocks: func(mockPub *MockPublisher, mockCon *MockConsumer) {
				// Even with parent canceled, internal 60s timeout should work
				mockCon.On("Stop", mock.Anything).Return(nil)
				mockPub.On("Stop", mock.Anything).Return(nil)
			},
			expectError: false,
			description: "OnStop should work despite parent context cancellation due to internal timeout",
			verifyBehavior: func(t *testing.T, mockPub *MockPublisher, mockCon *MockConsumer, err error) {
				assert.NoError(t, err)
				// Verify both were called with the internal timeout context
				mockCon.AssertCalled(t, "Stop", mock.Anything)
				mockPub.AssertCalled(t, "Stop", mock.Anything)
			},
		},
	}

	for _, tc := range testCases {
		// Handle panic test cases separately
		if tc.shouldPanic {
			t.Run(tc.name, func(t *testing.T) {
				mockPub := new(MockPublisher)
				mockCon := new(MockConsumer)
				logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

				ctx, cancel := tc.setupContext()
				defer cancel()

				tc.setupMocks(mockPub, mockCon)
				onStopFunc := onStop(logger, mockPub, mockCon)

				// Expect panic
				assert.Panics(t, func() {
					_ = onStopFunc(ctx)
				}, tc.description)
			})
			continue
		}

		t.Run(tc.name, func(t *testing.T) {
			mockPub := new(MockPublisher)
			mockCon := new(MockConsumer)
			logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

			ctx, cancel := tc.setupContext()
			defer cancel()

			tc.setupMocks(mockPub, mockCon)
			onStopFunc := onStop(logger, mockPub, mockCon)

			// Execute
			err := onStopFunc(ctx)

			// Verify error expectation
			if tc.expectError {
				assert.Error(t, err, tc.description)
				if tc.expectedErr != "" {
					assert.Contains(t, err.Error(), tc.expectedErr)
				}
			} else {
				assert.NoError(t, err, tc.description)
			}

			// Custom verification
			if tc.verifyBehavior != nil {
				tc.verifyBehavior(t, mockPub, mockCon, err)
			}

			mockPub.AssertExpectations(t)
			mockCon.AssertExpectations(t)
		})
	}
}
