// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package app

import (
	"fmt"

	"xmidt-org/splitter/internal/consumer"
	"xmidt-org/splitter/internal/log"
	"xmidt-org/splitter/internal/metrics"
	"xmidt-org/splitter/internal/observe"
	"xmidt-org/splitter/internal/publisher"

	"go.uber.org/fx"
)

// splitter  module as a potential library component - rework this.
type ConsumerIn struct {
	fx.In
	Config        consumer.Config
	Publisher     *publisher.Publisher
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

	// Create the WRP message handler with the provided publisher
	handler := consumer.NewWRPMessageHandler(consumer.WRPMessageHandlerConfig{
		Producer:       in.Publisher,
		LogEmitter:     in.LogEmitter,
		MetricsEmitter: in.MetricEmitter,
	})

	// Build options from configuration - validation is handled by the option functions
	opts := []consumer.Option{
		// Observability
		consumer.WithLogEmitter(in.LogEmitter),
		consumer.WithMetricsEmitter(in.MetricEmitter),

		// Required options
		consumer.WithBrokers(cfg.Brokers...),
		consumer.WithTopics(cfg.Topics...),
		consumer.WithGroupID(cfg.GroupID),

		// Message handler
		consumer.WithMessageHandler(consumer.MessageHandlerFunc(handler.HandleMessage)),

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
