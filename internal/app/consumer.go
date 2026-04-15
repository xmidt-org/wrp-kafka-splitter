// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package app

import (
	"fmt"

	"xmidt-org/splitter/internal/bucket"
	"xmidt-org/splitter/internal/consumer"
	"xmidt-org/splitter/internal/log"
	"xmidt-org/splitter/internal/metrics"
	"xmidt-org/splitter/internal/observe"
	"xmidt-org/splitter/internal/publisher"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/xmidt-org/touchstone"
	"go.uber.org/fx"
)

type ConsumerIn struct {
	fx.In
	Config               consumer.Config
	BucketConfig         bucket.Config
	PrometheusConfig     touchstone.Config
	PrometheusRegisterer prometheus.Registerer
	Publisher            publisher.Publisher
	LogEmitter           *observe.Subject[log.Event]
	MetricEmitter        *observe.Subject[metrics.Event]
	Buckets              bucket.Bucket
}

type ConsumerOut struct {
	fx.Out
	Consumer consumer.Consumer
}

// provideConsumer creates a new Kafka consumer instance with options from the config file
// and integrates it with the metrics and logging emitters.
func provideConsumer(in ConsumerIn) (ConsumerOut, error) {
	cfg := in.Config

	handlerOpts := []consumer.HandlerOption{
		// Observability
		consumer.WithHandlerLogEmitter(in.LogEmitter),
		consumer.WithHandlerMetricsEmitter(in.MetricEmitter),
		consumer.WithBuckets(in.Buckets),
		consumer.WithHandlerProducer(in.Publisher),
	}
	// Create the WRP message handler with the provided publisher
	handler, err := consumer.NewWRPMessageHandler(handlerOpts...)
	if err != nil {
		return ConsumerOut{}, fmt.Errorf("failed to create WRP message handler: %w", err)
	}

	// Build options from configuration - validation is handled by the option functions
	opts := []consumer.Option{
		// Observability
		consumer.WithLogEmitter(in.LogEmitter),
		consumer.WithPrometheusMetrics(in.PrometheusConfig.DefaultNamespace, in.PrometheusConfig.DefaultSubsystem+"_consumer", in.PrometheusRegisterer),
		consumer.WithMetricsEmitter(in.MetricEmitter),

		// Required options
		consumer.WithBrokers(cfg.Brokers...),
		consumer.WithTopics(cfg.Topics...),
		consumer.WithGroupID(fmt.Sprintf("%s.%s", cfg.GroupID, in.BucketConfig.TargetBucket)),

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
		consumer.WithConsumeFromTheBeginning(cfg.ConsumeFromBeginning),

		// Fetch State Management
		consumer.WithResumeDelaySeconds(cfg.ResumeDelaySeconds),
		consumer.WithConsecutiveFailuresThreshold(cfg.ConsecutiveFailureThreshold),
	}

	// Create the consumer
	c, err := consumer.New(opts...)
	if err != nil {
		return ConsumerOut{}, fmt.Errorf("failed to create consumer: %w", err)
	}

	return ConsumerOut{Consumer: c}, nil
}
