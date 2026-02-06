// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"xmidt-org/splitter/internal/observe"

	kit "github.com/go-kit/kit/metrics"
)

type Metrics struct {
	ConsumerErrors kit.Counter
	ConsumerPauses kit.Counter
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

// using touchkit gives us less control over creating the metrics.  Ideally we
// should create the listeners at the same time we register the metrics
func createObservers(m Metrics) []*Observer {
	observers := []*Observer{
		NewObserver(ConsumerErrors, COUNTER, Metric{counter: m.ConsumerErrors}),
	}
	return observers
}

func NewNoop() *observe.Subject[Event] {
	s := observe.NewSubject[Event]()
	// Don't attach any observers - events will be discarded
	return s
}
