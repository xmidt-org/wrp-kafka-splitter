// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package integrationtests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/xmidt-org/wrp-go/v3"
	"go.uber.org/fx"
)

const (
	rawEventsTopic = "raw-events"
)

// SplitterTestSuite is a test suite for integration testing of wrp-kafka-splitter.
// It sets up a fresh Kafka broker and splitter service instance for each test.
type SplitterTestSuite struct {
	suite.Suite

	ctx    context.Context
	cancel context.CancelFunc

	// Kafka broker instance
	broker *KafkaBroker

	// Splitter service instance
	app *fx.App
}

// SetupTest creates a new Kafka broker and starts the splitter service before each test.
func (s *SplitterTestSuite) SetupTest() {
	var err error

	// Create a context for this test
	s.ctx, s.cancel = context.WithCancel(context.Background())

	// Setup Kafka broker
	s.broker, err = setupKafka(s.ctx, s.T())
	s.Require().NoError(err, "Failed to setup Kafka broker")

	// Start the splitter service
	s.app, err = startService(s.ctx, s.T(), s.broker.Address)
	s.Require().NoError(err, "Failed to start splitter service")

	// create raw events topic
	s.broker.container.Exec(s.ctx, []string{"kafka-topics", "--create", "--topic", rawEventsTopic, "--partitions", "1", "--replication-factor", "1", "--if-not-exists", "--bootstrap-server", "localhost:9092"})

	// Ensure cleanup happens

	// s.T().Cleanup(func() {
	// 	if s.cancel != nil {
	// 		s.cancel()
	// 	}
	// })
}

func (s *SplitterTestSuite) TearDownTest() {
	// testify should have already stopped kafka and the service. Cancel the test context
	if s.cancel != nil {
		s.cancel()
	}
}

// Runs all integration tests in the SplitterTestSuite.
func TestSplitterIntegration(t *testing.T) {
	suite.Run(t, new(SplitterTestSuite))
}

// TestSplitterStartup is a temporary test that verifies the splitter can start with Kafka.
func (s *SplitterTestSuite) TestSplitterStartup() {
	// If we get here, both Kafka and the splitter started successfully
	require.NotNil(s.T(), s.app, "Splitter app should be running")
	require.NotNil(s.T(), s.broker, "Kafka broker should be running")
	require.NotEmpty(s.T(), s.broker.Address, "Broker address should be set")

	msg := &wrp.Message{
		Type:        wrp.SimpleEventMessageType,
		Source:      "test-device",
		Destination: "mac:aabbccddee00",
		Payload:     []byte(`{"event": "data"}`),
	}

	err := produceMessage(s.ctx, s.T(), s.broker.Address, rawEventsTopic, msg)
	require.NoError(s.T(), err, "Failed to produce message to Kafka")
}
