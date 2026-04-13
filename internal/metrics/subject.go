// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"xmidt-org/splitter/internal/observe"

	kit "github.com/go-kit/kit/metrics"
)

type Metrics struct {
	ConsumerFetchErrors  kit.Counter
	ConsumerCommitErrors kit.Counter
	ConsumerPauses       kit.Gauge
	BucketKeyErrorCount  kit.Counter

	PublisherOutcomes      kit.Counter
	PublisherErrorsCounter kit.Counter

	// Kafka publisher metrics (wrpkafka event listeners)
	KafkaPublished      kit.Counter
	KafkaPublishLatency kit.Histogram
	Panics              kit.Counter
	UnknownMetrics      kit.Counter
}

type Metric struct {
	counter   kit.Counter
	gauge     kit.Gauge
	histogram kit.Histogram
}

// New creates a new Subject for metric events with unknown metrics tracking
func New(m Metrics) *observe.Subject[Event] {
	subject := observe.NewSubject[Event]()

	// Create observers
	counterMetrics := map[string]kit.Counter{
		"fetch_errors":                   m.ConsumerFetchErrors,
		"commit_errors":                  m.ConsumerCommitErrors,
		"bucket_key_error_count":         m.BucketKeyErrorCount,
		"publish_outcomes":               m.PublisherOutcomes,
		"publish_errors_total":           m.PublisherErrorsCounter,
		"kafka_messages_published_total": m.KafkaPublished,
		"panics_total":                   m.Panics,
		"unknown_metrics_total":          m.UnknownMetrics,
	}

	gaugeMetrics := map[string]kit.Gauge{
		"fetch_pauses": m.ConsumerPauses,
	}

	histogramMetrics := map[string]kit.Histogram{
		"kafka_publish_latency_seconds": m.KafkaPublishLatency,
	}

	counterObserver := NewCounterObserver(counterMetrics)
	gaugeObserver := NewGaugeObserver(gaugeMetrics)
	histogramObserver := NewHistogramObserver(histogramMetrics)

	// Create a handler that calls all observers and tracks unknown metrics
	handleEvent := func(event Event) {
		// Call all observers and collect their results
		counterHandled := counterObserver.HandleEvent(event)
		gaugeHandled := gaugeObserver.HandleEvent(event)
		histogramHandled := histogramObserver.HandleEvent(event)

		// If no observer handled the event, it's unknown
		if !counterHandled && !gaugeHandled && !histogramHandled && m.UnknownMetrics != nil {
			m.UnknownMetrics.With("metric_name", event.Name, "metric_type", "unknown").Add(1)
		}
	}

	subject.Attach(handleEvent)

	return subject
}

func NewNoop() *observe.Subject[Event] {
	s := observe.NewSubject[Event]()
	// Don't attach any observers - events will be discarded
	return s
}
