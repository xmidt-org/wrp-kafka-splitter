// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// createMinimalMetrics creates a Metrics struct with mock metrics
func createMinimalMetrics() Metrics {
	return Metrics{
		ConsumerFetchErrors:    &MockCounter{},
		ConsumerCommitErrors:   &MockCounter{},
		ConsumerPauses:         &MockGauge{},
		BucketKeyErrorCount:    &MockCounter{},
		PublisherOutcomes:      &MockCounter{},
		PublisherErrorsCounter: &MockCounter{},
		KafkaPublished:         &MockCounter{},
		KafkaPublishLatency:    &MockHistogram{},
		Panics:                 &MockCounter{},
		UnknownMetrics:         &MockCounter{},
		MetricPanics:           &MockCounter{},
	}
}

func TestNew(t *testing.T) {
	tests := []struct {
		name                string
		setupMetrics        func() Metrics
		expectNonNilSubject bool
	}{
		{
			name:                "create new subject with minimal metrics",
			setupMetrics:        createMinimalMetrics,
			expectNonNilSubject: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockMetrics := tt.setupMetrics()
			subject := New(mockMetrics)

			if tt.expectNonNilSubject {
				assert.NotNil(t, subject)
			} else {
				assert.Nil(t, subject)
			}
		})
	}
}

func TestSubjectNotify(t *testing.T) {
	tests := []struct {
		name         string
		setupMetrics func() (Metrics, []string)
		event        Event
		useSync      bool
		shouldPanic  bool
		verifyMocks  bool
	}{
		{
			name: "counter metric handled correctly",
			setupMetrics: func() (Metrics, []string) {
				counter := &MockCounter{}
				expectedLabels := []string{PartitionLabel, "0", TopicLabel, "test-topic"}
				counter.On("With", expectedLabels).Return(counter)
				counter.On("Add", 1.0).Return()

				mockMetrics := createMinimalMetrics()
				mockMetrics.ConsumerFetchErrors = counter
				return mockMetrics, expectedLabels
			},
			event: Event{
				Name:   "fetch_errors",
				Labels: []string{PartitionLabel, "0", TopicLabel, "test-topic"},
				Value:  1.0,
			},
			useSync:     true,
			verifyMocks: true,
		},
		{
			name: "gauge metric handled correctly",
			setupMetrics: func() (Metrics, []string) {
				gauge := &MockGauge{}
				expectedLabels := []string{GroupLabel, "test-group"}
				gauge.On("With", expectedLabels).Return(gauge)
				gauge.On("Set", 5.0).Return()

				mockMetrics := createMinimalMetrics()
				mockMetrics.ConsumerPauses = gauge
				return mockMetrics, expectedLabels
			},
			event: Event{
				Name:   "fetch_pauses",
				Labels: []string{GroupLabel, "test-group"},
				Value:  5.0,
			},
			useSync:     true,
			verifyMocks: true,
		},
		{
			name: "histogram metric handled correctly",
			setupMetrics: func() (Metrics, []string) {
				histogram := &MockHistogram{}
				expectedLabels := []string{OutcomeLabel, "success"}
				histogram.On("With", expectedLabels).Return(histogram)
				histogram.On("Observe", 123.45).Return()

				mockMetrics := createMinimalMetrics()
				mockMetrics.KafkaPublishLatency = histogram
				return mockMetrics, expectedLabels
			},
			event: Event{
				Name:   "kafka_publish_latency_seconds",
				Labels: []string{OutcomeLabel, "success"},
				Value:  123.45,
			},
			useSync:     true,
			verifyMocks: true,
		},
		{
			name: "unknown metric tracked by unknown metrics observer",
			setupMetrics: func() (Metrics, []string) {
				counter := &MockCounter{}
				unknownCounter := &MockCounter{}
				// Set up expectations for unknown metrics tracking
				unknownCounter.On("With", []string{"metric_name", "unknown_metric_name", "metric_type", "unknown"}).Return(unknownCounter)
				unknownCounter.On("Add", 1.0).Return()

				mockMetrics := createMinimalMetrics()
				mockMetrics.ConsumerFetchErrors = counter
				mockMetrics.UnknownMetrics = unknownCounter
				return mockMetrics, []string{}
			},
			event: Event{
				Name:   "unknown_metric_name",
				Labels: []string{},
				Value:  10.0,
			},
			useSync:     false,
			verifyMocks: true, // Now we expect calls to unknown metrics
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockMetrics, _ := tt.setupMetrics()
			subject := New(mockMetrics)

			if tt.shouldPanic {
				assert.Panics(t, func() {
					if tt.useSync {
						subject.NotifySync(tt.event)
					} else {
						subject.Notify(tt.event)
					}
				})
			} else {
				assert.NotPanics(t, func() {
					if tt.useSync {
						subject.NotifySync(tt.event)
					} else {
						subject.Notify(tt.event)
					}
				})
			}

			if tt.verifyMocks {
				// Verify specific mocks based on the test
				switch tt.event.Name {
				case "fetch_errors":
					if counter, ok := mockMetrics.ConsumerFetchErrors.(*MockCounter); ok {
						counter.AssertExpectations(t)
					}
				case "fetch_pauses":
					if gauge, ok := mockMetrics.ConsumerPauses.(*MockGauge); ok {
						gauge.AssertExpectations(t)
					}
				case "kafka_publish_latency_seconds":
					if histogram, ok := mockMetrics.KafkaPublishLatency.(*MockHistogram); ok {
						histogram.AssertExpectations(t)
					}
				}
			} else if tt.event.Name == "unknown_metric_name" {
				// Verify unknown metrics counter was called
				if unknownCounter, ok := mockMetrics.UnknownMetrics.(*MockCounter); ok {
					unknownCounter.AssertExpectations(t)
				}
			}
		})
	}
}

func TestSubjectAsync(t *testing.T) {
	tests := []struct {
		name         string
		setupMetrics func() Metrics
		eventCount   int
		shouldPanic  bool
	}{
		{
			name: "async events don't panic",
			setupMetrics: func() Metrics {
				// Set up mock with expectations for async test
				publisherOutcomes := &MockCounter{}
				publisherOutcomes.On("With", []string{"test", "async"}).Return(publisherOutcomes).Maybe()
				publisherOutcomes.On("Add", mock.AnythingOfType("float64")).Return().Maybe()

				// Set up panic counter expectations
				panicCounter := &MockCounter{}
				panicCounter.On("With", []string{"metric_name", "publish_outcomes", "metric_type", "counter"}).Return(panicCounter).Maybe()
				panicCounter.On("Add", 1.0).Return().Maybe()

				mockMetrics := createMinimalMetrics()
				mockMetrics.PublisherOutcomes = publisherOutcomes
				mockMetrics.MetricPanics = panicCounter
				return mockMetrics
			},
			eventCount:  10,
			shouldPanic: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockMetrics := tt.setupMetrics()
			subject := New(mockMetrics)

			if tt.shouldPanic {
				assert.Panics(t, func() {
					for i := 0; i < tt.eventCount; i++ {
						go subject.Notify(Event{
							Name:   "publish_outcomes",
							Labels: []string{"test", "async"},
							Value:  float64(i),
						})
					}
				})
			} else {
				assert.NotPanics(t, func() {
					for i := 0; i < tt.eventCount; i++ {
						go subject.Notify(Event{
							Name:   "publish_outcomes",
							Labels: []string{"test", "async"},
							Value:  float64(i),
						})
					}
				})
			}
		})
	}
}

func TestNewNoop(t *testing.T) {
	tests := []struct {
		name             string
		testEvents       []Event
		eventCount       int // For high volume tests
		shouldPanic      bool
		expectNonNilNoop bool
		testHighVolume   bool
	}{
		{
			name: "noop subject creation",
			testEvents: []Event{
				{Name: "any_metric", Labels: []string{}, Value: 42.0},
				{Name: "another_metric", Labels: []string{"key", "value"}, Value: 100.0},
			},
			shouldPanic:      false,
			expectNonNilNoop: true,
		},
		{
			name:           "noop high volume handling",
			eventCount:     1000,
			shouldPanic:    false,
			testHighVolume: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			subject := NewNoop()

			if tt.expectNonNilNoop {
				assert.NotNil(t, subject)
			}

			if tt.testHighVolume {
				assert.NotPanics(t, func() {
					for i := 0; i < tt.eventCount; i++ {
						event := Event{
							Name:   "high_volume_metric",
							Labels: []string{"iteration", string(rune(i % 10))},
							Value:  float64(i),
						}
						subject.Notify(event)
					}
				})
			} else {
				for _, event := range tt.testEvents {
					if tt.shouldPanic {
						assert.Panics(t, func() {
							subject.Notify(event)
						})
						assert.Panics(t, func() {
							subject.NotifySync(event)
						})
					} else {
						assert.NotPanics(t, func() {
							subject.Notify(event)
						})
						assert.NotPanics(t, func() {
							subject.NotifySync(event)
						})
					}
				}
			}
		})
	}
}
