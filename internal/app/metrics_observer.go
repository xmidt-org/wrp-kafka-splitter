// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package app

import (
	"xmidt-org/splitter/internal/metrics"
	"xmidt-org/splitter/internal/observe"

	kit "github.com/go-kit/kit/metrics"

	"go.uber.org/fx"
)

type MetricsIn struct {
	fx.In
	ConsumerFetchErrors    kit.Counter   `name:"fetch_errors"`
	ConsumerCommitErrors   kit.Counter   `name:"commit_errors"`
	ConsumerPauses         kit.Gauge     `name:"fetch_pauses"`
	PublisherOutcomes      kit.Counter   `name:"publish_outcomes"`
	PublisherErrorsCounter kit.Counter   `name:"publish_errors_total"`
	BucketKeyErrorCount    kit.Counter   `name:"bucket_key_error_count"`
	KafkaPublished         kit.Counter   `name:"kafka_messages_published_total"`
	KafkaPublishLatency    kit.Histogram `name:"kafka_publish_latency_seconds"`
	Panics                 kit.Counter   `name:"panics_total"`
	UnknownMetrics         kit.Counter   `name:"unknown_metrics_total"`
}

type metricsObserverIn struct {
	fx.In
	Metrics metrics.Metrics
}

type metricsObserverOut struct {
	fx.Out
	Subject *observe.Subject[metrics.Event]
}

var MetricObserversModule = fx.Module("metrics_observers",
	fx.Provide(
		func(in MetricsIn) metrics.Metrics {
			return metrics.Metrics{
				ConsumerFetchErrors:    in.ConsumerFetchErrors,
				ConsumerCommitErrors:   in.ConsumerCommitErrors,
				ConsumerPauses:         in.ConsumerPauses,
				BucketKeyErrorCount:    in.BucketKeyErrorCount,
				PublisherOutcomes:      in.PublisherOutcomes,
				PublisherErrorsCounter: in.PublisherErrorsCounter,
				Panics:                 in.Panics,
				KafkaPublished:         in.KafkaPublished,
				KafkaPublishLatency:    in.KafkaPublishLatency,
				UnknownMetrics:         in.UnknownMetrics,
			}
		}),
	fx.Provide(
		func(in metricsObserverIn) (metricsObserverOut, error) {
			return metricsObserverOut{
				Subject: metrics.New(in.Metrics),
			}, nil
		},
	),
)
