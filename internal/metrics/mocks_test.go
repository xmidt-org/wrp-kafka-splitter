// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	kit "github.com/go-kit/kit/metrics"
	"github.com/stretchr/testify/mock"
)

// MockCounter is a mock implementation of kit.Counter using testify/mock
type MockCounter struct {
	mock.Mock
}

func (m *MockCounter) With(labelValues ...string) kit.Counter {
	args := m.Called(labelValues)
	if counter := args.Get(0); counter != nil {
		return counter.(kit.Counter)
	}
	return m
}

func (m *MockCounter) Add(delta float64) {
	m.Called(delta)
}

// MockGauge is a mock implementation of kit.Gauge using testify/mock
type MockGauge struct {
	mock.Mock
}

func (m *MockGauge) With(labelValues ...string) kit.Gauge {
	args := m.Called(labelValues)
	if gauge := args.Get(0); gauge != nil {
		return gauge.(kit.Gauge)
	}
	return m
}

func (m *MockGauge) Set(value float64) {
	m.Called(value)
}

func (m *MockGauge) Add(delta float64) {
	m.Called(delta)
}

// MockHistogram is a mock implementation of kit.Histogram using testify/mock
type MockHistogram struct {
	mock.Mock
}

func (m *MockHistogram) With(labelValues ...string) kit.Histogram {
	args := m.Called(labelValues)
	if histogram := args.Get(0); histogram != nil {
		return histogram.(kit.Histogram)
	}
	return m
}

func (m *MockHistogram) Observe(value float64) {
	m.Called(value)
}
