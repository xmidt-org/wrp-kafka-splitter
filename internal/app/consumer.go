// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package app

import (
	"context"
	"fmt"

	"xmidt-org/splitter/internal/consumer"
	"xmidt-org/splitter/internal/log"
	"xmidt-org/splitter/internal/metrics"
	"xmidt-org/splitter/internal/observe"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/xmidt-org/wrpkafka"
	"go.uber.org/fx"
)

type ConsumerIn struct {
	fx.In
	Config        consumer.Config
	LogEmitter    *observe.Subject[log.Event]
	MetricEmitter *observe.Subject[metrics.Event]
}

type ConsumerOut struct {
	fx.Out
	Consumer *consumer.Consumer
}

// provideConsumer creates a new Kafka consumer instance with options from the config file
// and integrates it with the metrics and logging emitters.
func provideConsumer(in ConsumerIn) (ConsumerOut, error) {
	cfg := in.Config

	// Build options from configuration - validation is handled by the option functions
	opts := []consumer.Option{
		// Observability
		consumer.WithLogEmitter(in.LogEmitter),
		consumer.WithMetricsEmitter(in.MetricEmitter),

		// Required options
		consumer.WithBrokers(cfg.Brokers...),
		consumer.WithTopics(cfg.Topics...),
		consumer.WithGroupID(cfg.GroupID),

		// Message handler (placeholder - replace with actual implementation)
		consumer.WithMessageHandler(
			consumer.MessageHandlerFunc(func(ctx context.Context, record *kgo.Record) (wrpkafka.Outcome, error) {
				// TODO: Replace with actual WRP message processing
				fmt.Printf("Received message: topic=%s partition=%d offset=%d key=%s value=%s\n",
					record.Topic, record.Partition, record.Offset, string(record.Key), string(record.Value))
				return wrpkafka.Attempted, nil
			}),
		),

		// Client identification
		consumer.WithClientID(cfg.ClientID),
		consumer.WithRack(cfg.Rack),
		consumer.WithInstanceID(cfg.InstanceID),

		// Session and heartbeat
		consumer.WithSessionTimeout(cfg.SessionTimeout),
		consumer.WithHeartbeatInterval(cfg.HeartbeatInterval),
		consumer.WithRebalanceTimeout(cfg.RebalanceTimeout),

		// Fetch configuration
		consumer.WithFetchMinBytes(cfg.FetchMinBytes),
		consumer.WithFetchMaxBytes(cfg.FetchMaxBytes),
		consumer.WithFetchMaxWait(cfg.FetchMaxWait),
		consumer.WithFetchMaxPartitionBytes(cfg.FetchMaxPartitionBytes),
		consumer.WithMaxConcurrentFetches(cfg.MaxConcurrentFetches),
		// Auto-commit
		consumer.WithAutoCommitInterval(cfg.AutoCommitInterval),

		// SASL authentication (uses config-aware wrapper)
		consumer.WithSASLConfig(cfg.SASL),

		// TLS configuration (uses config-aware wrapper)
		consumer.WithTLSConfig(cfg.TLS),

		// Retry and backoff
		consumer.WithRequestRetries(cfg.RequestRetries),

		// Connection (Duration type, not pointer)
		consumer.WithConnIdleTimeout(cfg.ConnIdleTimeout),
		consumer.WithRequestTimeoutOverhead(cfg.RequestTimeoutOverhead),
		consumer.WithConsumeFromTheBeginning(cfg.ConsumeFromBeginning),
	}

	// Create the consumer
	c, err := consumer.New(opts...)
	if err != nil {
		return ConsumerOut{}, fmt.Errorf("failed to create consumer: %w", err)
	}

	return ConsumerOut{Consumer: c}, nil
}
