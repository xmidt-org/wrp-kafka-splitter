// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type EventTestSuite struct {
	suite.Suite
}

func TestEventTestSuite(t *testing.T) {
	suite.Run(t, new(EventTestSuite))
}

// TestEvent_Creation tests creating Event instances with various fields
func (s *EventTestSuite) TestEvent_Creation() {
	tests := []struct {
		name           string
		event          Event
		expectedName   string
		expectedLabels []string
		expectedValue  float64
	}{
		{
			name: "event with all fields",
			event: Event{
				Name:   "test_metric",
				Labels: []string{"label1", "value1", "label2", "value2"},
				Value:  42.5,
			},
			expectedName:   "test_metric",
			expectedLabels: []string{"label1", "value1", "label2", "value2"},
			expectedValue:  42.5,
		},
		{
			name: "event with empty labels",
			event: Event{
				Name:   "simple_metric",
				Labels: []string{},
				Value:  1.0,
			},
			expectedName:   "simple_metric",
			expectedLabels: []string{},
			expectedValue:  1.0,
		},
		{
			name: "event with nil labels",
			event: Event{
				Name:  "minimal_metric",
				Value: 0.0,
			},
			expectedName:   "minimal_metric",
			expectedLabels: nil,
			expectedValue:  0.0,
		},
		{
			name: "event with negative value",
			event: Event{
				Name:   "negative_metric",
				Labels: []string{"error", "type"},
				Value:  -10.5,
			},
			expectedName:   "negative_metric",
			expectedLabels: []string{"error", "type"},
			expectedValue:  -10.5,
		},
		{
			name: "event with zero value",
			event: Event{
				Name:   "zero_metric",
				Labels: []string{},
				Value:  0,
			},
			expectedName:   "zero_metric",
			expectedLabels: []string{},
			expectedValue:  0,
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			s.Equal(tt.expectedName, tt.event.Name)
			s.Equal(tt.expectedLabels, tt.event.Labels)
			s.Equal(tt.expectedValue, tt.event.Value)
		})
	}
}

// TestEvent_ConsumerErrors tests creating consumer error events
func (s *EventTestSuite) TestEvent_ConsumerErrors() {
	event := Event{
		Name:   ConsumerFetchErrors,
		Labels: []string{PartitionLabel, "0", TopicLabel, "test-topic", ErrorTypeLabel, "decode_error"},
		Value:  1.0,
	}

	s.Equal(ConsumerFetchErrors, event.Name)
	s.Equal("fetch_errors", event.Name)
	s.Contains(event.Labels, PartitionLabel)
	s.Contains(event.Labels, TopicLabel)
	s.Contains(event.Labels, ErrorTypeLabel)
	s.Equal(1.0, event.Value)
}
