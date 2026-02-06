// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type ObserverTestSuite struct {
	suite.Suite
}

func TestObserverTestSuite(t *testing.T) {
	suite.Run(t, new(ObserverTestSuite))
}

// TestNewObserver tests creating new observers
func (s *ObserverTestSuite) TestNewObserver() {
	tests := []struct {
		name         string
		metricName   string
		metricType   metricType
		expectedName string
		expectedType metricType
		hasCounter   bool
		hasGauge     bool
		hasHistogram bool
	}{
		{
			name:         "counter observer",
			metricName:   "test_counter",
			metricType:   COUNTER,
			expectedName: "test_counter",
			expectedType: COUNTER,
			hasCounter:   true,
		},
		{
			name:         "gauge observer",
			metricName:   "test_gauge",
			metricType:   GAUGE,
			expectedName: "test_gauge",
			expectedType: GAUGE,
			hasGauge:     true,
		},
		{
			name:         "histogram observer",
			metricName:   "test_histogram",
			metricType:   HISTOGRAM,
			expectedName: "test_histogram",
			expectedType: HISTOGRAM,
			hasHistogram: true,
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			var metric Metric
			if tt.hasCounter {
				metric.counter = &MockCounter{}
			}
			if tt.hasGauge {
				metric.gauge = &MockGauge{}
			}
			if tt.hasHistogram {
				metric.histogram = &MockHistogram{}
			}

			observer := NewObserver(tt.metricName, tt.metricType, metric)

			s.NotNil(observer)
			s.Equal(tt.expectedName, observer.name)
			s.Equal(tt.expectedType, observer.metricType)
		})
	}
}

// TestObserver_HandleEvent_Counter tests handling counter events
func (s *ObserverTestSuite) TestObserver_HandleEvent_Counter() {
	counter := &MockCounter{}
	counter.On("With", []string{"label1", "value1"}).Return(counter)
	counter.On("Add", 5.0).Return()

	observer := NewObserver("test_counter", COUNTER, Metric{counter: counter})

	event := Event{
		Name:   "test_counter",
		Labels: []string{"label1", "value1"},
		Value:  5.0,
	}

	observer.HandleEvent(event)

	counter.AssertExpectations(s.T())
}

// TestObserver_HandleEvent_Gauge tests handling gauge events
func (s *ObserverTestSuite) TestObserver_HandleEvent_Gauge() {
	gauge := &MockGauge{}
	gauge.On("With", []string{"label1", "value1"}).Return(gauge)
	gauge.On("Set", 42.5).Return()

	observer := NewObserver("test_gauge", GAUGE, Metric{gauge: gauge})

	event := Event{
		Name:   "test_gauge",
		Labels: []string{"label1", "value1"},
		Value:  42.5,
	}

	observer.HandleEvent(event)

	gauge.AssertExpectations(s.T())
}

// TestObserver_HandleEvent_Histogram tests handling histogram events
func (s *ObserverTestSuite) TestObserver_HandleEvent_Histogram() {
	histogram := &MockHistogram{}
	histogram.On("With", []string{"label1", "value1"}).Return(histogram)
	histogram.On("Observe", 0.125).Return()

	observer := NewObserver("test_histogram", HISTOGRAM, Metric{histogram: histogram})

	event := Event{
		Name:   "test_histogram",
		Labels: []string{"label1", "value1"},
		Value:  0.125,
	}

	observer.HandleEvent(event)

	histogram.AssertExpectations(s.T())
}

// TestObserver_HandleEvent_WrongName tests that events with wrong names are ignored
func (s *ObserverTestSuite) TestObserver_HandleEvent_WrongName() {
	counter := &MockCounter{}
	// No expectations set - method should not be called

	observer := NewObserver("test_counter", COUNTER, Metric{counter: counter})

	event := Event{
		Name:   "different_counter",
		Labels: []string{"label1", "value1"},
		Value:  5.0,
	}

	observer.HandleEvent(event)

	// Should not be called because name doesn't match
	counter.AssertNotCalled(s.T(), "With")
	counter.AssertNotCalled(s.T(), "Add")
}

// TestObserver_HandleEvent_NilCounter tests handling events when counter is nil
func (s *ObserverTestSuite) TestObserver_HandleEvent_NilCounter() {
	observer := NewObserver("test_counter", COUNTER, Metric{})

	event := Event{
		Name:   "test_counter",
		Labels: []string{},
		Value:  1.0,
	}

	// Should not panic, just print error
	s.NotPanics(func() {
		observer.HandleEvent(event)
	})
}

// TestObserver_HandleEvent_NilGauge tests handling events when gauge is nil
func (s *ObserverTestSuite) TestObserver_HandleEvent_NilGauge() {
	observer := NewObserver("test_gauge", GAUGE, Metric{})

	event := Event{
		Name:   "test_gauge",
		Labels: []string{},
		Value:  1.0,
	}

	// Should not panic, just print error
	s.NotPanics(func() {
		observer.HandleEvent(event)
	})
}

// TestObserver_HandleEvent_NilHistogram tests handling events when histogram is nil
func (s *ObserverTestSuite) TestObserver_HandleEvent_NilHistogram() {
	observer := NewObserver("test_histogram", HISTOGRAM, Metric{})

	event := Event{
		Name:   "test_histogram",
		Labels: []string{},
		Value:  1.0,
	}

	// Should not panic, just print error
	s.NotPanics(func() {
		observer.HandleEvent(event)
	})
}

// TestObserver_HandleEvent_EmptyLabels tests handling events with empty labels
func (s *ObserverTestSuite) TestObserver_HandleEvent_EmptyLabels() {
	counter := &MockCounter{}
	counter.On("With", []string{}).Return(counter)
	counter.On("Add", 10.0).Return()

	observer := NewObserver("test_counter", COUNTER, Metric{counter: counter})

	event := Event{
		Name:   "test_counter",
		Labels: []string{},
		Value:  10.0,
	}

	observer.HandleEvent(event)

	counter.AssertExpectations(s.T())
}

// TestObserver_HandleEvent_MultipleLabels tests handling events with multiple label pairs
func (s *ObserverTestSuite) TestObserver_HandleEvent_MultipleLabels() {
	counter := &MockCounter{}
	expectedLabels := []string{PartitionLabel, "0", TopicLabel, "test-topic", ErrorTypeLabel, "decode_error"}
	counter.On("With", expectedLabels).Return(counter)
	counter.On("Add", 1.0).Return()

	observer := NewObserver(ConsumerErrors, COUNTER, Metric{counter: counter})

	event := Event{
		Name:   ConsumerErrors,
		Labels: expectedLabels,
		Value:  1.0,
	}

	observer.HandleEvent(event)

	counter.AssertExpectations(s.T())
}
