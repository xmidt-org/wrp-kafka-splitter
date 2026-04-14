// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"fmt"

	kit "github.com/go-kit/kit/metrics"
)

// Generic observers that can handle multiple metrics of the same type
type CounterObserver struct {
	counters     map[string]kit.Counter
	panicCounter kit.Counter
}

type GaugeObserver struct {
	gauges       map[string]kit.Gauge
	panicCounter kit.Counter
}

type HistogramObserver struct {
	histograms   map[string]kit.Histogram
	panicCounter kit.Counter
}

// NewCounterObserver creates a new observer for counter metrics
func NewCounterObserver(counters map[string]kit.Counter, panicCounter kit.Counter) *CounterObserver {
	return &CounterObserver{counters: counters, panicCounter: panicCounter}
}

// NewGaugeObserver creates a new observer for gauge metrics
func NewGaugeObserver(gauges map[string]kit.Gauge, panicCounter kit.Counter) *GaugeObserver {
	return &GaugeObserver{gauges: gauges, panicCounter: panicCounter}
}

// NewHistogramObserver creates a new observer for histogram metrics
func NewHistogramObserver(histograms map[string]kit.Histogram, panicCounter kit.Counter) *HistogramObserver {
	return &HistogramObserver{histograms: histograms, panicCounter: panicCounter}
}

// HandleEvent processes counter events
func (c *CounterObserver) HandleEvent(event Event) bool {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("ERROR: Prometheus panic for counter metric '%s': %v (labels: %v)\n", event.Name, r, event.Labels)
			if c.panicCounter != nil {
				c.panicCounter.With(MetricNameLabel, event.Name, MetricTypeLabel, "counter").Add(1)
			}
		}
	}()

	counter, ok := c.counters[event.Name]
	if !ok {
		return false
	}
	if counter == nil {
		fmt.Printf("ERROR: counter for metric '%s' is nil\n", event.Name)
		return false
	}
	counter.With(event.Labels...).Add(event.Value)
	return true
}

// HandleEvent processes gauge events
func (g *GaugeObserver) HandleEvent(event Event) bool {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("ERROR: Prometheus panic for gauge metric '%s': %v (labels: %v)\n", event.Name, r, event.Labels)
			if g.panicCounter != nil {
				g.panicCounter.With(MetricNameLabel, event.Name, MetricTypeLabel, "gauge").Add(1)
			}
		}
	}()

	gauge, ok := g.gauges[event.Name]
	if !ok {
		return false
	}
	if gauge == nil {
		fmt.Printf("ERROR: gauge for metric '%s' is nil\n", event.Name)
		return false
	}
	gauge.With(event.Labels...).Set(event.Value)
	return true
}

// HandleEvent processes histogram events
func (h *HistogramObserver) HandleEvent(event Event) bool {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("ERROR: Prometheus panic for histogram metric '%s': %v (labels: %v)\n", event.Name, r, event.Labels)
			if h.panicCounter != nil {
				h.panicCounter.With(MetricNameLabel, event.Name, MetricTypeLabel, "histogram").Add(1)
			}
		}
	}()

	histogram, ok := h.histograms[event.Name]
	if !ok {
		return false
	}
	if histogram == nil {
		fmt.Printf("ERROR: histogram for metric '%s' is nil\n", event.Name)
		return false
	}
	histogram.With(event.Labels...).Observe(event.Value)
	return true
}
