// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"xmidt-org/splitter/internal/observe"

	kit "github.com/go-kit/kit/metrics"
)

type Metrics struct {
	ConsumerFetchErrors    kit.Counter
	ConsumerCommitErrors   kit.Counter
	ConsumerPauses         kit.Gauge
	PublisherOutcomes      kit.Counter
	PublisherErrorsCounter kit.Counter

	// Kafka publisher metrics (wrpkafka event listeners)
	KafkaPublished         kit.Counter
	KafkaPublishLatency    kit.Histogram
	KafkaBufferUtilization kit.Gauge
}

type Metric struct {
	counter   kit.Counter
	gauge     kit.Gauge
	histogram kit.Histogram
}

// New creates a new Subject for metric events
func New(m Metrics) *observe.Subject[Event] {
	subject := observe.NewSubject[Event]()

	observers := createObservers(m)

	for _, observer := range observers {
		subject.Attach(observer.HandleEvent)
	}

	return subject
}

func createObservers(m Metrics) []*Observer {
	observers := []*Observer{
		NewObserver(ConsumerFetchErrors, COUNTER, Metric{counter: m.ConsumerFetchErrors}),
		NewObserver(ConsumerCommitErrors, COUNTER, Metric{counter: m.ConsumerCommitErrors}),
		NewObserver(ConsumerPauses, GAUGE, Metric{gauge: m.ConsumerPauses}),
		NewObserver(PublisherOutcomes, COUNTER, Metric{counter: m.PublisherOutcomes}),
		NewObserver(PublisherErrorsCounter, COUNTER, Metric{counter: m.PublisherErrorsCounter}),
		NewObserver(KafkaPublished, COUNTER, Metric{counter: m.KafkaPublished}),
		NewObserver(KafkaPublishLatency, HISTOGRAM, Metric{histogram: m.KafkaPublishLatency}),
		NewObserver(KafkaBufferUtilization, GAUGE, Metric{gauge: m.KafkaBufferUtilization}),
	}
	return observers
}

func NewNoop() *observe.Subject[Event] {
	s := observe.NewSubject[Event]()
	// Don't attach any observers - events will be discarded
	return s
}
