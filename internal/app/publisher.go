// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package app

import (
	"fmt"

	"xmidt-org/splitter/internal/log"
	"xmidt-org/splitter/internal/metrics"
	"xmidt-org/splitter/internal/observe"
	"xmidt-org/splitter/internal/publisher"

	"go.uber.org/fx"
)

// PublisherIn contains the dependencies needed to create a publisher instance.
type PublisherIn struct {
	fx.In
	Config        publisher.Config
	LogEmitter    *observe.Subject[log.Event]
	MetricEmitter *observe.Subject[metrics.Event]
}

// PublisherOut contains the created publisher instance.
type PublisherOut struct {
	fx.Out
	Publisher *publisher.Publisher
}

// providePublisher creates a new Kafka publisher instance with options from the config file
func providePublisher(in PublisherIn) (PublisherOut, error) {
	cfg := in.Config

	// Convert config routes to wrpkafka routes
	wrpRoutes := cfg.ToWRPKafkaRoutes()

	// Build options from configuration - validation is handled by the option functions
	opts := []publisher.Option{
		// Observability
		publisher.WithLogEmitter(in.LogEmitter),
		publisher.WithMetricsEmitter(in.MetricEmitter),

		// Required options
		publisher.WithBrokers(cfg.Brokers...),
		publisher.WithTopicRoutes(wrpRoutes...),

		// Optional configurations
		publisher.WithMaxBufferedRecords(cfg.MaxBufferedRecords),
		publisher.WithMaxBufferedBytes(cfg.MaxBufferedBytes),
		publisher.WithRequestTimeout(cfg.RequestTimeout),
		publisher.WithCleanupTimeout(cfg.CleanupTimeout),
		publisher.WithMaxRetries(cfg.RequestRetries),
		publisher.WithAllowAutoTopicCreation(cfg.AllowAutoTopicCreation),

		// SASL authentication (uses config-aware wrapper)
		publisher.WithSASLConfig(cfg.SASL),

		// TLS configuration (uses config-aware wrapper)
		publisher.WithTLSConfig(cfg.TLS),
	}

	// Create the publisher
	pub, err := publisher.New(opts...)
	if err != nil {
		return PublisherOut{}, fmt.Errorf("failed to create publisher: %w", err)
	}

	return PublisherOut{Publisher: pub}, nil
}
