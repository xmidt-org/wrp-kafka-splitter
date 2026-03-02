// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package integrationtests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"github.com/xmidt-org/wrp-go/v5"
	"go.uber.org/fx"
)

const (
	inputTopic              = "wrp-events"           // Topic the service consumes from
	defaultOutputTopic      = "default-events"       // Default topic for general events
	deviceStatusOutputTopic = "device-status-events" // Topic for device status events
	timeoutShort            = 5 * time.Second        // Short timeout for basic operations
	timeoutMedium           = 10 * time.Second       // Medium timeout for processing
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

	// Create necessary topics for testing
	s.createTopics(inputTopic, defaultOutputTopic, deviceStatusOutputTopic)

	// Give the service time to start consuming
	time.Sleep(2 * time.Second)
}

func (s *SplitterTestSuite) TearDownTest() {
	// cancel test context. testify should have already stopped kafka and the service.
	if s.cancel != nil {
		s.cancel()
	}
}

// createTopics creates the necessary Kafka topics for testing
func (s *SplitterTestSuite) createTopics(topics ...string) {
	for _, topic := range topics {
		s.broker.container.Exec(s.ctx, []string{
			"kafka-topics", "--create",
			"--topic", topic,
			"--partitions", "3", // Multiple partitions for better testing
			"--replication-factor", "1",
			"--if-not-exists",
			"--bootstrap-server", "localhost:9092",
		})
		s.T().Logf("Created topic: %s", topic)
	}
}

// Helper method to create test WRP messages with proper URI schemes
func (s *SplitterTestSuite) createWRPMessage(msgType wrp.MessageType, source, destination, payload string) *wrp.Message {
	return &wrp.Message{
		Type:            msgType,
		Source:          source,
		Destination:     destination,
		TransactionUUID: generateUUID(),
		Payload:         []byte(payload),
	}
}

// Runs all integration tests in the SplitterTestSuite.
func TestSplitterIntegration(t *testing.T) {
	suite.Run(t, new(SplitterTestSuite))
}

// TestSplitterStartup verifies the basic startup functionality
func (s *SplitterTestSuite) TestMultipleOutputTopics() {
	// If we get here, both Kafka and the splitter started successfully
	s.Require().NotNil(s.app, "Splitter app should be running")
	s.Require().NotNil(s.broker, "Kafka broker should be running")
	s.Require().NotEmpty(s.broker.Address, "Broker address should be set")

	// Verify service can process a basic message
	msg := s.createWRPMessage(
		wrp.SimpleEventMessageType,
		"mac:aa11bb22cc33/service",
		"event:device-status/online",
		`{"event": "device-status"}`,
	)

	// Produce message to topic
	err := produceWRPMessage(s.ctx, s.T(), s.broker.Address, inputTopic, msg)
	s.Require().NoError(err, "Failed to produce message to input topic")

	// Consume from topic(s) to verify routing
	records := consumeMessages(s.T(), s.broker.Address, defaultOutputTopic, timeoutShort)
	s.Require().Len(records, 1, "Should have routed exactly one message")

	records = consumeMessages(s.T(), s.broker.Address, deviceStatusOutputTopic, timeoutShort)
	s.Require().Len(records, 1, "Should have routed exactly one message")

	// Verify the routed message content
	var routedMsg wrp.Message
	err = wrp.NewDecoderBytes(records[0].Value, wrp.Msgpack).Decode(&routedMsg)
	s.Require().NoError(err, "Should be able to decode routed WRP message")
	s.Equal(msg.Source, routedMsg.Source, "Source should be preserved")
	s.Equal(msg.Destination, routedMsg.Destination, "Destination should be preserved")
	s.Equal(msg.TransactionUUID, routedMsg.TransactionUUID, "Transaction UUID should be preserved")
}

// TestHighVolumeProcessing verifies the service can handle multiple messages efficiently
func (s *SplitterTestSuite) TestHighVolumeProcessing() {
	const messageCount = 50

	messages := make([]*wrp.Message, messageCount)
	expectedUUIDs := make(map[string]bool)

	// Generate test messages
	for i := 0; i < messageCount; i++ {
		msg := s.createWRPMessage(
			wrp.SimpleEventMessageType,
			fmt.Sprintf("mac:%02x%02x%02x%02x%02x%02x/service",
				0xaa, 0xbb, 0xcc, 0xdd, 0xee, i%256), // Generate unique MAC addresses
			"event:high-volume-test/batch",
			fmt.Sprintf(`{"batch": "high_volume", "sequence": %d}`, i),
		)
		messages[i] = msg
		expectedUUIDs[msg.TransactionUUID] = false
	}

	// Produce all messages
	for i, msg := range messages {
		err := produceWRPMessage(s.ctx, s.T(), s.broker.Address, inputTopic, msg)
		s.Require().NoError(err, "Failed to produce message %d", i)
	}

	// Consume all routed messages
	records := consumeMessages(s.T(), s.broker.Address, defaultOutputTopic, timeoutMedium)
	s.Require().Len(records, messageCount, "Should route all %d messages", messageCount)

	// Verify all messages were processed correctly
	processedUUIDs := make(map[string]bool)
	for _, record := range records {
		var routedMsg wrp.Message
		err := wrp.NewDecoderBytes(record.Value, wrp.Msgpack).Decode(&routedMsg)
		s.Require().NoError(err, "Should decode routed message")

		_, exists := expectedUUIDs[routedMsg.TransactionUUID]
		s.True(exists, "Routed message should have expected UUID: %s", routedMsg.TransactionUUID)
		processedUUIDs[routedMsg.TransactionUUID] = true
	}

	s.Equal(messageCount, len(processedUUIDs), "Should process unique messages")
	s.Equal(len(expectedUUIDs), len(processedUUIDs), "Should process all expected messages")
}

// TestConcurrentProcessing verifies the service handles concurrent message processing
func (s *SplitterTestSuite) TestConcurrentProcessing() {
	const goroutines = 5
	const messagesPerGoroutine = 10
	const totalMessages = goroutines * messagesPerGoroutine

	errChan := make(chan error, totalMessages)

	// Launch concurrent producers
	for g := 0; g < goroutines; g++ {
		go func(routineID int) {
			for m := 0; m < messagesPerGoroutine; m++ {
				msg := s.createWRPMessage(
					wrp.SimpleEventMessageType,
					fmt.Sprintf("mac:%02x%02x%02x%02x%02x%02x/service",
						0x11, 0x22, 0x33, 0x44, 0x55, routineID), // Generate unique MAC addresses per routine
					"event:concurrent-test/status",
					fmt.Sprintf(`{"routine": %d, "message": %d}`, routineID, m),
				)

				err := produceWRPMessage(s.ctx, s.T(), s.broker.Address, inputTopic, msg)
				errChan <- err
			}
		}(g)
	}

	// Check all produces succeeded
	for i := 0; i < totalMessages; i++ {
		err := <-errChan
		s.Require().NoError(err, "Concurrent produce %d should succeed", i)
	}

	// Verify all messages were routed
	records := consumeMessages(s.T(), s.broker.Address, defaultOutputTopic, timeoutMedium)
	s.Require().Len(records, totalMessages, "Should route all concurrent messages")

	// Verify message integrity
	deviceCounts := make(map[string]int)
	for _, record := range records {
		var routedMsg wrp.Message
		err := wrp.NewDecoderBytes(record.Value, wrp.Msgpack).Decode(&routedMsg)
		s.Require().NoError(err, "Should decode concurrent message")
		deviceCounts[routedMsg.Source]++
	}

	s.Require().Len(deviceCounts, goroutines, "Should have messages from all %d devices", goroutines)
	for device, count := range deviceCounts {
		s.Equal(messagesPerGoroutine, count, "Device %s should have exactly %d messages", device, messagesPerGoroutine)
	}
}
