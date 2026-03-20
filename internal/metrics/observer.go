// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"fmt"
)

type Observer struct {
	metric     Metric
	name       string
	metricType metricType
}

func NewObserver(name string, metricType metricType, metric Metric) *Observer {
	return &Observer{
		name:       name,
		metricType: metricType,
		metric:     metric,
	}
}

func (l *Observer) HandleEvent(event Event) {
	// Catch any panics from Prometheus/go-kit metrics
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("ERROR: Prometheus panic for metric '%s': %v (labels: %v)\n", l.name, r, event.Labels)
		}
	}()

	if l.name != event.Name {
		return
	}

	switch l.metricType {
	case COUNTER:
		if l.metric.counter == nil {
			fmt.Printf("ERROR: counter for metric '%s' is nil\n", l.name)
			return
		}
		l.metric.counter.With(event.Labels...).Add(event.Value)
	case GAUGE:
		if l.metric.gauge == nil {
			fmt.Printf("ERROR: gauge for metric '%s' is nil\n", l.name)
			return
		}
		l.metric.gauge.With(event.Labels...).Set(event.Value)
	case HISTOGRAM:
		if l.metric.histogram == nil {
			fmt.Printf("ERROR: histogram for metric '%s' is nil\n", l.name)
			return
		}
		l.metric.histogram.With(event.Labels...).Observe(event.Value)
	}
}
