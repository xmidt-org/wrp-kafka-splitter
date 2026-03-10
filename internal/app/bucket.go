// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package app

import (
	"fmt"

	"xmidt-org/splitter/internal/bucket"

	"go.uber.org/fx"
)

type BucketsIn struct {
	fx.In
	Config bucket.Config
}

type BucketsOut struct {
	fx.Out
	Buckets bucket.Bucket
}

// provideBuckets creates a new Buckets instance with options from the config file
func provideBuckets(in BucketsIn) (BucketsOut, error) {
	cfg := in.Config

	b, err := bucket.NewBuckets(cfg)
	if err != nil {
		return BucketsOut{}, fmt.Errorf("failed to create buckets: %w", err)
	}

	return BucketsOut{Buckets: b}, nil
}
