// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type SubjectTestSuite struct {
	suite.Suite
}

func TestSubjectTestSuite(t *testing.T) {
	suite.Run(t, new(SubjectTestSuite))
}

// TestNew tests creating a new metrics subject with observers
func (s *SubjectTestSuite) TestNew() {
	counter := &MockCounter{}
	metrics := Metrics{
		ConsumerErrors: counter,
	}

	subject := New(metrics)

	s.NotNil(subject)
}

// TestNew_NotifyObservers tests that observers are notified when events are published
func (s *SubjectTestSuite) TestNew_NotifyObservers() {
	counter := &MockCounter{}
	expectedLabels := []string{PartitionLabel, "0", TopicLabel, "test-topic", ErrorTypeLabel, "decode_error"}
	counter.On("With", expectedLabels).Return(counter)
	counter.On("Add", 1.0).Return()

	metrics := Metrics{
		ConsumerErrors: counter,
	}

	subject := New(metrics)

	// Publish an event
	event := Event{
		Name:   ConsumerErrors,
		Labels: expectedLabels,
		Value:  1.0,
	}

	subject.NotifySync(event)

	// Verify the counter was called
	counter.AssertExpectations(s.T())
}

// TestNew_MultipleEvents tests publishing multiple events
func (s *SubjectTestSuite) TestNew_MultipleEvents() {
	counter := &MockCounter{}
	expectedLabels := []string{ErrorTypeLabel, "test_error"}
	counter.On("With", expectedLabels).Return(counter).Times(5)
	counter.On("Add", 1.0).Return().Times(5)

	metrics := Metrics{
		ConsumerErrors: counter,
	}

	subject := New(metrics)

	// Publish multiple events
	for i := 0; i < 5; i++ {
		event := Event{
			Name:   ConsumerErrors,
			Labels: expectedLabels,
			Value:  1.0,
		}
		subject.NotifySync(event)
	}

	// Verify the counter was called 5 times
	counter.AssertExpectations(s.T())
}

// TestNew_EventWithDifferentName tests that events with non-matching names are ignored
func (s *SubjectTestSuite) TestNew_EventWithDifferentName() {
	counter := &MockCounter{}
	// No expectations set - methods should not be called

	metrics := Metrics{
		ConsumerErrors: counter,
	}

	subject := New(metrics)

	// Publish event with different name
	event := Event{
		Name:   "unknown_metric",
		Labels: []string{},
		Value:  10.0,
	}

	subject.Notify(event)

	// Counter should not be called because name doesn't match
	counter.AssertNotCalled(s.T(), "With")
	counter.AssertNotCalled(s.T(), "Add")
}

// TestNewNoop tests creating a no-op metrics subject
func (s *SubjectTestSuite) TestNewNoop() {
	subject := NewNoop()

	s.NotNil(subject)

	// Should not panic when notifying
	event := Event{
		Name:   "any_metric",
		Labels: []string{},
		Value:  42.0,
	}

	s.NotPanics(func() {
		subject.Notify(event)
	})
}

// TestNewNoop_MultipleEvents tests that noop subject handles multiple events
func (s *SubjectTestSuite) TestNewNoop_MultipleEvents() {
	subject := NewNoop()

	s.NotPanics(func() {
		for i := 0; i < 100; i++ {
			event := Event{
				Name:   "test_metric",
				Labels: []string{"iteration", string(rune(i))},
				Value:  float64(i),
			}
			subject.Notify(event)
		}
	})
}

// TestCreateObservers tests the createObservers function
func (s *SubjectTestSuite) TestCreateObservers() {
	counter := &MockCounter{}
	metrics := Metrics{
		ConsumerErrors: counter,
	}

	observers := createObservers(metrics)

	s.NotNil(observers)
	s.Len(observers, 1) // Currently only one metric defined

	// Verify the observer is configured for ConsumerErrors
	s.Equal(ConsumerErrors, observers[0].name)
	s.Equal(COUNTER, observers[0].metricType)
	s.NotNil(observers[0].metric.counter)
}

// TestMetrics_Struct tests the Metrics struct
func (s *SubjectTestSuite) TestMetrics_Struct() {
	counter := &MockCounter{}
	metrics := Metrics{
		ConsumerErrors: counter,
	}

	s.NotNil(metrics.ConsumerErrors)
	s.Equal(counter, metrics.ConsumerErrors)
}

// TestMetric_Struct tests the Metric struct with different types
func (s *SubjectTestSuite) TestMetric_Struct() {
	tests := []struct {
		name         string
		metric       Metric
		hasCounter   bool
		hasGauge     bool
		hasHistogram bool
	}{
		{
			name: "counter only",
			metric: Metric{
				counter: &MockCounter{},
			},
			hasCounter: true,
		},
		{
			name: "gauge only",
			metric: Metric{
				gauge: &MockGauge{},
			},
			hasGauge: true,
		},
		{
			name: "histogram only",
			metric: Metric{
				histogram: &MockHistogram{},
			},
			hasHistogram: true,
		},
		{
			name:   "empty metric",
			metric: Metric{},
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			if tt.hasCounter {
				s.NotNil(tt.metric.counter)
			} else {
				s.Nil(tt.metric.counter)
			}
			if tt.hasGauge {
				s.NotNil(tt.metric.gauge)
			} else {
				s.Nil(tt.metric.gauge)
			}
			if tt.hasHistogram {
				s.NotNil(tt.metric.histogram)
			} else {
				s.Nil(tt.metric.histogram)
			}
		})
	}
}
