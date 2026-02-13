// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package consumer

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Client defines the interface for a Kafka client.
// This interface allows for mocking in tests while using the real kgo.Client in production.
// Fetches abstracts the kgo.Fetches struct for testability.
type Fetches interface {
	Errors() []*kgo.FetchError
	EachRecord(func(*kgo.Record))
}

type Client interface {
	// Ping pings the broker to verify connectivity.
	Ping(ctx context.Context) error

	// PollFetches polls for new records from Kafka.
	PollFetches(ctx context.Context) Fetches

	// CommitUncommittedOffsets commits any uncommitted offsets.
	CommitUncommittedOffsets(ctx context.Context) error

	// Close closes the client and cleans up resources.
	Close()

	// ResumeFetchTopics resumes fetching for the configured topics.
	ResumeFetchTopics(topics ...string)

	// PauseFetchTopics pauses fetching for the configured topics.
	PauseFetchTopics(topics ...string) []string

	// MarkCommitRecords marks the provided records for offset commit.
	MarkCommitRecords(records ...*kgo.Record)

	// CommitMarkedOffsets commits the offsets that have been marked.
	CommitMarkedOffsets(ctx context.Context) error
}

// kgoFetchesAdapter adapts kgo.Fetches to the Fetches interface.
type kgoFetchesAdapter struct {
	f kgo.Fetches
}

func (a *kgoFetchesAdapter) Errors() []*kgo.FetchError {
	errs := a.f.Errors()
	out := make([]*kgo.FetchError, len(errs))
	for i := range errs {
		out[i] = &errs[i]
	}
	return out
}
func (a *kgoFetchesAdapter) EachRecord(fn func(*kgo.Record)) { a.f.EachRecord(fn) }

// kgoClientAdapter adapts *kgo.Client to the Client interface.
type kgoClientAdapter struct {
	*kgo.Client
}

func (c *kgoClientAdapter) PollFetches(ctx context.Context) Fetches {
	return &kgoFetchesAdapter{f: c.Client.PollFetches(ctx)}
}

// Ensure kgoClientAdapter implements our Client interface at compile time.
var _ Client = (*kgoClientAdapter)(nil)
