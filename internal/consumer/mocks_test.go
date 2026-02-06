// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package consumer

import (
	"context"

	"github.com/stretchr/testify/mock"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/xmidt-org/wrpkafka"
)

// MockClient is a mock implementation of the Client interface
type MockClient struct {
	mock.Mock
}

func (m *MockClient) Ping(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockClient) PollFetches(ctx context.Context) kgo.Fetches {
	args := m.Called(ctx)
	return args.Get(0).(kgo.Fetches)
}

func (m *MockClient) MarkCommitRecords(records ...*kgo.Record) {
	m.Called(records)
}

func (m *MockClient) CommitUncommittedOffsets(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockClient) PauseFetchTopics(topics ...string) []string {
	args := m.Called(topics)
	if len(args) == 0 {
		return nil
	}
	if result := args.Get(0); result != nil {
		return result.([]string)
	}
	return nil
}

func (m *MockClient) ResumeFetchTopics(topics ...string) {
	m.Called(topics)
}

func (m *MockClient) Close() {
	m.Called()
}

func (m *MockClient) CommitMarkedOffsets(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

// MockHandler is a mock implementation of the MessageHandler interface
type MockHandler struct {
	mock.Mock
}

func (m *MockHandler) HandleMessage(ctx context.Context, record *kgo.Record) (wrpkafka.Outcome, error) {
	args := m.Called(ctx, record)
	return args.Get(0).(wrpkafka.Outcome), args.Error(1)
}
