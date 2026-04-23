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
	KafkaPublished        kit.Counter
	KafkaPublishLatency   kit.Histogram
	Panics                kit.Counter
	UnknownMetrics        kit.Counter
	MetricPanics          kit.Counter
	MalformedMessageCount kit.Counter
}

// New creates a new Subject for metric events with unknown metrics tracking
func New(m Metrics) *observe.Subject[Event] {
	subject := observe.NewSubject[Event]()

	// Create observers
	counterMetrics := map[string]kit.Counter{
		ConsumerFetchErrors:    m.ConsumerFetchErrors,
		ConsumerCommitErrors:   m.ConsumerCommitErrors,
		BucketKeyErrorCount:    m.BucketKeyErrorCount,
		PublisherOutcomes:      m.PublisherOutcomes,
		PublisherErrorsCounter: m.PublisherErrorsCounter,
		KafkaPublished:         m.KafkaPublished,
		Panics:                 m.Panics,
		MalformedMessageCount:  m.MalformedMessageCount,
	}

	gaugeMetrics := map[string]kit.Gauge{
		ConsumerPauses: m.ConsumerPauses,
	}

	histogramMetrics := map[string]kit.Histogram{
		KafkaPublishLatency: m.KafkaPublishLatency,
	}

	counterObserver := NewCounterObserver(counterMetrics, m.MetricPanics)
	gaugeObserver := NewGaugeObserver(gaugeMetrics, m.MetricPanics)
	histogramObserver := NewHistogramObserver(histogramMetrics, m.MetricPanics)

	// Create a handler that calls all observers and tracks unknown metrics
	handleEvent := func(event Event) {
		// Call all observers and collect their results
		counterHandled := counterObserver.HandleEvent(event)
		gaugeHandled := gaugeObserver.HandleEvent(event)
		histogramHandled := histogramObserver.HandleEvent(event)

		// If no observer handled the event, it's unknown
		if !counterHandled && !gaugeHandled && !histogramHandled && m.UnknownMetrics != nil {
			m.UnknownMetrics.With(MetricNameLabel, event.Name, MetricTypeLabel, "unknown").Add(1)
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
