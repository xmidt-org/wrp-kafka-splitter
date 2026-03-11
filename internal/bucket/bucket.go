// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package bucket

import (
	"fmt"
	"sort"

	"github.com/xmidt-org/wrp-go/v5"
)

// TODO - make the hash algorithm configurable, but not yet a requirement

// this file determines whether or not a wrp.Message should be published to a target bucket.
// A splitter will only be configured to write to one "bucket".  Messages destined for
// other buckets (in this case, regions) will be dropped.

type MissingPartitionKeyAction int

const (
	Drop MissingPartitionKeyAction = iota
	IncludeInBucket
)

const (
	DefaultMissingPartitionKeyAction             = IncludeInBucket
	DropMissingPartitionKeyActionName            = "drop"
	IncludeInBucketMissingPartitionKeyActionName = "include"
)

const (
	DeviceIdMetadataKeyName = "hw-deviceid"
)

var (
	ErrNoPartitionKey = fmt.Errorf("no partition key found")
)

type Bucket interface {
	IsInTargetBucket(msg *wrp.Message) (bool, error)
}

type Config struct {
	TargetBucket    string
	PossibleBuckets []BucketSettings

	// partition key type determines which field of the message is used for hashing to a bucket
	PartitionKeyType string

	// what to do when the partition key is missing from the message - either drop the message or include it in the target bucket
	MissingPartitionKeyAction string
}

type BucketSettings struct {
	Name      string
	Threshold float32
}

type Buckets struct {
	targetBucketIndex         int
	partitioner               Partitioner
	missingPartitionKeyAction MissingPartitionKeyAction
	hashKey                   HashKey
	metatadataKeyField        string
	buckets                   []BucketSettings
	thresholds                []float32
}

func NewBuckets(config Config) (Bucket, error) {
	if len(config.PossibleBuckets) == 0 {
		// if there are no buckets, then all messages are in the target bucket
		return &Buckets{}, nil
	}

	// sort buckets slice in order of threshold, ascending
	sort.Slice(config.PossibleBuckets, func(i, j int) bool {
		return config.PossibleBuckets[i].Threshold < config.PossibleBuckets[j].Threshold
	})

	// for convenience, extract ordered thresholds for the partitioner
	thresholds := make([]float32, len(config.PossibleBuckets))
	for i, bucket := range config.PossibleBuckets {
		thresholds[i] = bucket.Threshold
	}

	// create the partitioner
	partition := NewPartitioner()

	// set the index for the target bucket
	targetBucketIndex, err := getTargetIndex(config.TargetBucket, config.PossibleBuckets)
	if err != nil {
		return &Buckets{}, err
	}

	// set the bucket key type
	hashKey, err := ParseHashKey(config.PartitionKeyType)
	if err != nil {
		return &Buckets{}, err
	}

	// set the missing partition key action
	missingPartitionKeyAction := DefaultMissingPartitionKeyAction
	if config.MissingPartitionKeyAction != "" {
		missingPartitionKeyAction, err = getMissingPartitionKeyAction(config.MissingPartitionKeyAction)
		if err != nil {
			return &Buckets{}, err
		}
	}

	return &Buckets{
		partitioner:               partition,
		targetBucketIndex:         targetBucketIndex,
		buckets:                   config.PossibleBuckets,
		thresholds:                thresholds,
		hashKey:                   hashKey,
		missingPartitionKeyAction: missingPartitionKeyAction}, nil
}

func getTargetIndex(targetBucket string, buckets []BucketSettings) (int, error) {
	for i, bucket := range buckets {
		if bucket.Name == targetBucket {
			return i, nil
		}
	}
	return -1, fmt.Errorf("target bucket %s not found", targetBucket)
}

// determine if message hashes to the target bucket
func (r *Buckets) IsInTargetBucket(msg *wrp.Message) (bool, error) {
	// if there are no buckets or only one bucket, then all messages are in the target bucket
	if len(r.buckets) <= 1 {
		return true, nil
	}

	partitionKey, err := r.getPartitionKey(msg)
	if err != nil {
		if r.missingPartitionKeyAction == Drop {
			return false, err
		}
		return true, err
	}

	bucket, err := r.partitioner.Partition(partitionKey, r.thresholds)
	if err != nil {
		return false, err
	}

	return bucket == r.targetBucketIndex, nil
}

func (r *Buckets) getPartitionKey(msg *wrp.Message) (string, error) {
	return r.hashKey.GetHashKey(msg)
}

func getMissingPartitionKeyAction(action string) (MissingPartitionKeyAction, error) {
	if action == "" {
		return DefaultMissingPartitionKeyAction, nil
	}

	switch action {
	case DropMissingPartitionKeyActionName:
		return Drop, nil
	case IncludeInBucketMissingPartitionKeyActionName:
		return IncludeInBucket, nil
	default:
		return -1, fmt.Errorf("invalid missing partition key action %s", action)
	}
}
