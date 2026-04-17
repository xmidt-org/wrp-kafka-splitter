// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package app

import (
	"fmt"

	"xmidt-org/splitter/internal/log"
	"xmidt-org/splitter/internal/metrics"
	"xmidt-org/splitter/internal/observe"
	"xmidt-org/splitter/internal/publisher"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/xmidt-org/touchstone"
	"go.uber.org/fx"
)

// PublisherIn contains the dependencies needed to create a publisher instance.
type PublisherIn struct {
	fx.In
	Config               publisher.Config
	LogEmitter           *observe.Subject[log.Event]
	MetricEmitter        *observe.Subject[metrics.Event]
	PrometheusConfig     touchstone.Config
	PrometheusRegisterer prometheus.Registerer
}

// PublisherOut contains the created publisher instance.
type PublisherOut struct {
	fx.Out
	Publisher publisher.Publisher
}

// providePublisher creates a new Kafka publisher instance with options from the config file
func providePublisher(in PublisherIn) (PublisherOut, error) {
	cfg := in.Config
	prometheusCfg := in.PrometheusConfig

	// Convert config routes to wrpkafka routes
	wrpRoutes, err := cfg.ToWRPKafkaRoutes()
	if err != nil {
		return PublisherOut{}, fmt.Errorf("failed to convert topic routes: %w", err)
	}

	// Build options from configuration - validation is handled by the option functions
	opts := []publisher.Option{
		// Observability
		publisher.WithLogEmitter(in.LogEmitter),
		publisher.WithMetricsEmitter(in.MetricEmitter),

		// Required options
		publisher.WithBrokers(cfg.Brokers),
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

		// Configure Prometheus metrics
		// Use config from YAML if provided, otherwise build from touchstone
		publisher.WithPrometheusConfig(buildPrometheusConfig(cfg.Prometheus, prometheusCfg, in.PrometheusRegisterer)),
	}

	// Create the publisher
	pub, err := publisher.New(opts...)
	if err != nil {
		return PublisherOut{}, fmt.Errorf("failed to create publisher: %w", err)
	}

	return PublisherOut{
		Publisher: pub,
	}, nil
}

// buildPrometheusConfig creates a PrometheusConfig by merging YAML config with touchstone config.
// If YAML config is provided, it takes precedence for optional metrics while namespace/subsystem
// come from touchstone (consistent across all services).
func buildPrometheusConfig(yamlCfg *publisher.PrometheusConfig, touchstoneCfg touchstone.Config, registerer prometheus.Registerer) *publisher.PrometheusConfig {
	// Always use touchstone for namespace/subsystem and registerer (consistent across services)
	cfg := &publisher.PrometheusConfig{
		Namespace:  touchstoneCfg.DefaultNamespace,
		Subsystem:  touchstoneCfg.DefaultSubsystem + "_publisher",
		Registerer: registerer,
	}

	// If YAML config is provided, use it for optional franz-go metrics
	if yamlCfg != nil {
		cfg.EnableRecordMetrics = yamlCfg.EnableRecordMetrics
		cfg.EnableBatchMetrics = yamlCfg.EnableBatchMetrics
		cfg.EnableCompressedBytes = yamlCfg.EnableCompressedBytes
		cfg.EnableGoCollectors = yamlCfg.EnableGoCollectors
		cfg.WithClientLabel = yamlCfg.WithClientLabel
	}

	return cfg
}
