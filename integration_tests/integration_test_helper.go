// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package integrationtests

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"xmidt-org/wrp-kafka-splitter/internal/app"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/xmidt-org/wrp-go/v3"
	"go.uber.org/fx"
)

// KafkaBroker represents a Kafka broker container with connection details and cleanup function.
type KafkaBroker struct {
	// Address is the broker address in the format "host:port"
	Address string
	// Terminate is a function that shuts down the Kafka broker container
	Terminate func() error
	// container holds a reference to the underlying testcontainers container
	container testcontainers.Container
	// ctx is the context for the container
	ctx context.Context
}

// setupKafka starts a Kafka broker in a Docker container using the official
// testcontainers Kafka module. It returns a KafkaBroker with the broker address
// and a cleanup function. The caller should defer the Terminate function to ensure cleanup.
//
// Example:
//
//	broker, err := setupKafka(ctx)
//	if err != nil {
//		t.Fatal(err)
//	}
//	defer broker.Terminate()
//	// Use broker.Address to connect to Kafka
func setupKafka(ctx context.Context, t *testing.T) (*KafkaBroker, error) {
	t.Helper()

	// Use the official Kafka testcontainer module with RunContainer which handles
	// configuration automatically
	kafkaContainer, err := kafka.Run(ctx,
		"confluentinc/confluent-local:7.8.0",
		kafka.WithClusterID("test-cluster"),
	)
	require.NoError(t, err, "Failed to start Kafka container")

	t.Cleanup(func() {
		t.Log("Stopping Kafka container...")
		if err := kafkaContainer.Terminate(ctx); err != nil {
			t.Logf("Failed to terminate Kafka container: %v", err)
		}
	})

	// Get the brokers
	brokers, err := kafkaContainer.Brokers(ctx)
	if err != nil {
		_ = kafkaContainer.Terminate(ctx)
		return nil, fmt.Errorf("failed to get kafka brokers: %w", err)
	}

	if len(brokers) == 0 {
		_ = kafkaContainer.Terminate(ctx)
		return nil, fmt.Errorf("no brokers returned from kafka container")
	}

	return &KafkaBroker{
		Address: brokers[0],
		Terminate: func() error {
			return kafkaContainer.Terminate(ctx)
		},
		container: kafkaContainer,
		ctx:       ctx,
	}, nil
}

// startService starts an instance of the wrp-kafka-splitter service.
// It sets the KAFKA_BROKERS environment variable to the broker address from setupKafka,
// and uses the wrp-kafka-splitter.yaml configuration file.
// The caller is responsible for stopping the service by calling Stop on the returned app.
//
// Example:
//
//	broker, err := setupKafka(ctx)
//	if err != nil {
//		t.Fatal(err)
//	}
//	defer broker.Terminate()
//
//	app, err := startService(ctx, broker.Address)
//	if err != nil {
//		t.Fatal(err)
//	}
//	defer app.Stop(ctx)
func startService(ctx context.Context, t *testing.T, brokerAddress string) (*fx.App, error) {
	t.Helper()

	// Set environment variable for Kafka broker override
	if err := os.Setenv("KAFKA_BROKERS", brokerAddress); err != nil {
		return nil, fmt.Errorf("failed to set KAFKA_BROKERS environment variable: %w", err)
	}

	// Create the app with configuration file
	app, err := app.WrpKafkaRouter([]string{"-f", "wrp-kafka-splitter.yaml"})
	if err != nil {
		return nil, fmt.Errorf("failed to create splitter app: %w", err)
	}

	// Start the app
	if err := app.Start(ctx); err != nil {
		return nil, fmt.Errorf("failed to start splitter app: %w", err)
	}

	t.Cleanup(func() {
		if app != nil {
			err := app.Stop(ctx)
			if err != nil {
				t.Logf("Error stopping splitter app: %v", err)
			}
		}
	})

	return app, nil
}

// produceMessage writes a WRP message to the Kafka broker on the specified topic.
// The WRP message is JSON-encoded before sending.
// Note: Kafka must be configured with auto.create.topics.enable=true for automatic topic creation,
// otherwise the topic must already exist.
//
// Example:
//
//	msg := &wrp.Message{
//		Type:        wrp.MessageTypeEvent,
//		Source:      "test",
//		Destination: "mac:aabbccddee00",
//		Payload:     []byte(`{"test": "data"}`),
//	}
//	err := produceMessage(ctx, t, broker.Address, "raw-events", msg)
//	require.NoError(t, err)
func produceMessage(ctx context.Context, t *testing.T, brokerAddress, topic string, msg *wrp.Message) error {
	t.Helper()

	// Create producer client
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokerAddress),
	)
	if err != nil {
		return fmt.Errorf("failed to create kafka producer: %w", err)
	}
	defer client.Close()

	// Marshal the WRP message to JSON
	msgBytes, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal WRP message: %w", err)
	}

	// Produce the message
	record := &kgo.Record{
		Topic: topic,
		Value: msgBytes,
	}

	// Add key if message has a source
	if msg.Source != "" {
		record.Key = []byte(msg.Source)
	}

	results := client.ProduceSync(ctx, record)
	if err := results[0].Err; err != nil {
		return fmt.Errorf("failed to produce message to kafka: %w", err)
	}

	t.Logf("Produced message to topic %s", topic)
	return nil
}

func consumeMessages(t *testing.T, broker string, topic string, timeout time.Duration) []*kgo.Record {
	t.Helper()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	require.NoError(t, err, "Failed to create Kafka consumer")
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var records []*kgo.Record
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		fetches := client.PollFetches(ctx)
		if fetches.IsClientClosed() {
			break
		}

		fetches.EachError(func(topic string, partition int32, err error) {
			t.Logf("Fetch error on %s[%d]: %v", topic, partition, err)
		})

		fetches.EachRecord(func(r *kgo.Record) {
			records = append(records, r)
		})

		// If we got records, give a bit more time for any additional ones
		if len(records) > 0 {
			time.Sleep(500 * time.Millisecond)
			// Try one more fetch
			fetches = client.PollFetches(ctx)
			fetches.EachRecord(func(r *kgo.Record) {
				records = append(records, r)
			})
			break
		}

		time.Sleep(100 * time.Millisecond)
	}

	return records
}
