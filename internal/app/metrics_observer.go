// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package app

import (
	"xmidt-org/wrp-kafka-splitter/internal/metrics"
	"xmidt-org/wrp-kafka-splitter/internal/observe"

	kit "github.com/go-kit/kit/metrics"

	"go.uber.org/fx"
)

type MetricsIn struct {
	fx.In
	ConsumerErrors kit.Counter `name:"consumer_errors"`
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
				ConsumerErrors: in.ConsumerErrors,
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
