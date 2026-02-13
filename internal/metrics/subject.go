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
	}
	return observers
}

func NewNoop() *observe.Subject[Event] {
	s := observe.NewSubject[Event]()
	// Don't attach any observers - events will be discarded
	return s
}
