// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package app

import (
	"context"
	"fmt"
	"time"

	"xmidt-org/wrp-kafka-splitter/internal/consumer"
	"xmidt-org/wrp-kafka-splitter/internal/log"
	"xmidt-org/wrp-kafka-splitter/internal/metrics"
	"xmidt-org/wrp-kafka-splitter/internal/observe"

	"github.com/twmb/franz-go/pkg/kgo"
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
			consumer.MessageHandlerFunc(func(ctx context.Context, record *kgo.Record) error {
				// TODO: Replace with actual WRP message processing
				fmt.Printf("Received message: topic=%s partition=%d offset=%d key=%s value=%s\n",
					record.Topic, record.Partition, record.Offset, string(record.Key), string(record.Value))
				return nil
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
		consumer.WithConnIdleTimeout(time.Duration(cfg.ConnIdleTimeout)),
		consumer.WithRequestTimeoutOverhead(time.Duration(cfg.RequestTimeoutOverhead)),
		consumer.WithDisableAutoCommit(cfg.DisableAutoCommit),
		consumer.WithConsumeFromTheBeginning(cfg.ConsumeFromBeginning),
	}

	// Create the consumer
	c, err := consumer.New(opts...)
	if err != nil {
		return ConsumerOut{}, fmt.Errorf("failed to create consumer: %w", err)
	}

	return ConsumerOut{Consumer: c}, nil
}

// Helper functions to safely dereference pointers

// func ptrDuration(d *consumer.Duration) time.Duration {
// 	if d == nil {
// 		return 0
// 	}
// 	return time.Duration(*d)
// }

// func ptrInt32(i *int32) int32 {
// 	if i == nil {
// 		return 0
// 	}
// 	return *i
// }

// func ptrInt(i *int) int {
// 	if i == nil {
// 		return 0
// 	}
// 	return *i
// }

// configToOptions converts a consumer.Config to a slice of consumer.Option.
// func configToOptions(cfg consumer.Config) []consumer.Option {
// 	opts := make([]consumer.Option, 0)

// 	// Required options
// 	if len(cfg.Brokers) > 0 {
// 		opts = append(opts, consumer.WithBrokers(cfg.Brokers...))
// 	}
// 	if len(cfg.Topics) > 0 {
// 		opts = append(opts, consumer.WithTopics(cfg.Topics...))
// 	}
// 	if cfg.GroupID != "" {
// 		opts = append(opts, consumer.WithGroupID(cfg.GroupID))
// 	}

// 	// Session and heartbeat options
// 	if cfg.SessionTimeout != nil {
// 		opts = append(opts, consumer.WithSessionTimeout(time.Duration(*cfg.SessionTimeout)))
// 	}
// 	if cfg.HeartbeatInterval != nil {
// 		opts = append(opts, consumer.WithHeartbeatInterval(time.Duration(*cfg.HeartbeatInterval)))
// 	}
// 	if cfg.RebalanceTimeout != nil {
// 		opts = append(opts, consumer.WithRebalanceTimeout(time.Duration(*cfg.RebalanceTimeout)))
// 	}

// 	// Fetch options
// 	if cfg.FetchMinBytes != nil {
// 		opts = append(opts, consumer.WithFetchMinBytes(*cfg.FetchMinBytes))
// 	}
// 	if cfg.FetchMaxBytes != nil {
// 		opts = append(opts, consumer.WithFetchMaxBytes(*cfg.FetchMaxBytes))
// 	}
// 	if cfg.FetchMaxWait != nil {
// 		opts = append(opts, consumer.WithFetchMaxWait(time.Duration(*cfg.FetchMaxWait)))
// 	}
// 	if cfg.FetchMaxPartitionBytes != nil {
// 		opts = append(opts, consumer.WithFetchMaxPartitionBytes(*cfg.FetchMaxPartitionBytes))
// 	}
// 	if cfg.MaxConcurrentFetches != nil {
// 		opts = append(opts, consumer.WithMaxConcurrentFetches(*cfg.MaxConcurrentFetches))
// 	}

// 	// Auto-commit options
// 	if cfg.AutoCommitInterval != nil {
// 		opts = append(opts, consumer.WithAutoCommitInterval(time.Duration(*cfg.AutoCommitInterval)))
// 	}
// 	if cfg.DisableAutoCommit != nil && *cfg.DisableAutoCommit {
// 		opts = append(opts, consumer.WithDisableAutoCommit())
// 	}

// 	// SASL authentication
// 	if cfg.SASL != nil {
// 		switch cfg.SASL.Mechanism {
// 		case "PLAIN":
// 			opts = append(opts, consumer.WithSASLPlain(cfg.SASL.Username, cfg.SASL.Password))
// 		case "SCRAM-SHA-256":
// 			opts = append(opts, consumer.WithSASLScram256(cfg.SASL.Username, cfg.SASL.Password))
// 		case "SCRAM-SHA-512":
// 			opts = append(opts, consumer.WithSASLScram512(cfg.SASL.Username, cfg.SASL.Password))
// 		default:
// 			fmt.Printf("Warning: unsupported SASL mechanism: %s\n", cfg.SASL.Mechanism)
// 		}
// 	}

// 	// TLS configuration
// 	if cfg.TLS != nil && cfg.TLS.Enabled {
// 		tlsConfig, err := cfg.TLS.ToTLSConfig()
// 		if err != nil {
// 			// Note: In a real implementation, you might want to handle this error differently
// 			// For now, we'll just skip TLS if there's an error
// 			fmt.Printf("Warning: failed to load TLS config: %v\n", err)
// 		} else if tlsConfig != nil {
// 			opts = append(opts, consumer.WithTLS(tlsConfig))
// 		}
// 	}

// 	// Connection options
// 	if cfg.ConnIdleTimeout != 0 {
// 		opts = append(opts, consumer.WithConnIdleTimeout(time.Duration(cfg.ConnIdleTimeout)))
// 	}
// 	if cfg.RequestTimeoutOverhead != 0 {
// 		opts = append(opts, consumer.WithRequestTimeoutOverhead(time.Duration(cfg.RequestTimeoutOverhead)))
// 	}

// 	// Client identification
// 	if cfg.ClientID != "" {
// 		opts = append(opts, consumer.WithClientID(cfg.ClientID))
// 	}
// 	if cfg.Rack != "" {
// 		opts = append(opts, consumer.WithRack(cfg.Rack))
// 	}
// 	if cfg.InstanceID != "" {
// 		opts = append(opts, consumer.WithInstanceID(cfg.InstanceID))
// 	}

// 	// Offset management
// 	if cfg.ConsumeFromBeginning {
// 		opts = append(opts, consumer.WithConsumeResetOffset(kgo.NewOffset().AtStart()))
// 	}

// 	// Retry configuration
// 	if cfg.RequestRetries > 0 {
// 		opts = append(opts, consumer.WithRequestRetries(cfg.RequestRetries))
// 	}

// 	return opts
// }
