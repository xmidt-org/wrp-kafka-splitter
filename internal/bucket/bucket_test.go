// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package bucket

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/xmidt-org/wrp-go/v5"
)

var TestBuckets = []BucketSettings{
	{"bucketA", 0.33},
	{"bucketB", 0.66},
	{"bucketC", 1.0},
}

type BucketsSuite struct {
	suite.Suite
	buckets    *Buckets
	bucketDefs []BucketSettings
}

func (s *BucketsSuite) SetupTest() {
	s.bucketDefs = TestBuckets

	cfg := Config{
		TargetBucket:    "bucketB",
		PossibleBuckets: s.bucketDefs,
	}
	var err error
	buckets, err := NewBuckets(cfg)
	s.NoError(err)
	s.buckets = buckets.(*Buckets)
	s.Require().NoError(err)
}

func (s *BucketsSuite) TestNewBuckets_TargetIndex() {
	assert.Equal(s.T(), 1, s.buckets.targetBucketIndex)
	assert.Equal(s.T(), 3, len(s.buckets.buckets))
	assert.Equal(s.T(), 3, len(s.buckets.thresholds))
}

func (s *BucketsSuite) TestNewBuckets_InvalidTarget() {
	cfg := Config{
		TargetBucket:    "notfound",
		PossibleBuckets: s.bucketDefs,
	}
	_, err := NewBuckets(cfg)
	assert.Error(s.T(), err)
}

func (s *BucketsSuite) TestNewBuckets_InvalidKeyType() {
	cfg := Config{
		TargetBucket:     "bucketA",
		PossibleBuckets:  s.bucketDefs,
		PartitionKeyType: "unknown_key",
	}
	_, err := NewBuckets(cfg)
	assert.Error(s.T(), err)
}

func (s *BucketsSuite) TestNewBuckets_ValidKeyType() {
	cfg := Config{
		TargetBucket:     "bucketA",
		PossibleBuckets:  s.bucketDefs,
		PartitionKeyType: "metadata/hw-deviceid",
	}
	b, err := NewBuckets(cfg)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), HashKeyMetadata, b.(*Buckets).hashKey.Name)
	assert.Equal(s.T(), "hw-deviceid", b.(*Buckets).hashKey.MetadataField)
}

func (s *BucketsSuite) TestNewBuckets_NoBuckets() {
	cfg := Config{}
	_, err := NewBuckets(cfg)
	assert.NoError(s.T(), err)
}

func (s *BucketsSuite) TestNoBuckets() {
	buckets, err := NewBuckets(Config{})
	assert.NoError(s.T(), err)
	msg := &wrp.Message{Metadata: map[string]string{DeviceIdMetadataKeyName: "mac:112233445566"}}
	inBucket, err := buckets.IsInTargetBucket(msg)
	assert.NoError(s.T(), err)
	assert.True(s.T(), inBucket)
}

func (s *BucketsSuite) TestIsInBucket_True() {
	msg := &wrp.Message{Metadata: map[string]string{DeviceIdMetadataKeyName: "mac:112233445566"}}
	partitionKey, _ := s.buckets.getPartitionKey(msg)
	partitioner := NewPartitioner()
	bucket, _ := partitioner.Partition(partitionKey, s.buckets.thresholds)
	s.buckets.targetBucketIndex = bucket
	inBucket, err := s.buckets.IsInTargetBucket(msg)
	assert.NoError(s.T(), err)
	assert.True(s.T(), inBucket)
}

// this specific mac is in bucket B
func (s *BucketsSuite) TestIsInBucketHardcodedValue_True() {
	msg := &wrp.Message{Metadata: map[string]string{DeviceIdMetadataKeyName: "mac:999999999999"}}
	inBucket, err := s.buckets.IsInTargetBucket(msg)
	assert.NoError(s.T(), err)
	assert.True(s.T(), inBucket)
}

func (s *BucketsSuite) TestIsInBucket_False() {
	msg := &wrp.Message{Metadata: map[string]string{DeviceIdMetadataKeyName: "mac:112233445566"}}
	partitionKey, _ := s.buckets.getPartitionKey(msg)
	partitioner := NewPartitioner()
	bucket, _ := partitioner.Partition(partitionKey, s.buckets.thresholds)
	s.buckets.targetBucketIndex = (bucket + 1) % len(s.buckets.thresholds)
	inBucket, err := s.buckets.IsInTargetBucket(msg)
	assert.NoError(s.T(), err)
	assert.False(s.T(), inBucket)
}

// this specific mac is not in bucket A
func (s *BucketsSuite) TestIsInBucketHardcodedValue_False() {
	s.bucketDefs = TestBuckets

	cfg := Config{
		TargetBucket:    "bucketA",
		PossibleBuckets: s.bucketDefs,
	}
	var err error
	buckets, err := NewBuckets(cfg)

	s.NoError(err)
	msg := &wrp.Message{Metadata: map[string]string{DeviceIdMetadataKeyName: "mac:999999999999"}}
	inBucket, err := buckets.IsInTargetBucket(msg)
	assert.NoError(s.T(), err)
	assert.False(s.T(), inBucket)
}

func (s *BucketsSuite) TestIsInBucket_NoPartitionKey_Include() {
	msg := &wrp.Message{Metadata: nil}
	inBucket, err := s.buckets.IsInTargetBucket(msg)
	assert.Error(s.T(), err)
	assert.True(s.T(), inBucket)
}

func (s *BucketsSuite) TestIsInBucket_NoPartitionKey_Drop() {
	s.bucketDefs = TestBuckets

	cfg := Config{
		TargetBucket:              "bucketA",
		PossibleBuckets:           s.bucketDefs,
		MissingPartitionKeyAction: "drop",
	}

	var err error
	buckets, err := NewBuckets(cfg)
	s.NoError(err)

	msg := &wrp.Message{Metadata: map[string]string{DeviceIdMetadataKeyName: ""}}
	inBucket, err := buckets.IsInTargetBucket(msg)
	assert.Error(s.T(), err)
	assert.False(s.T(), inBucket)
}

func (s *BucketsSuite) TestGetMissingPartitionKeyAction_Drop() {
	action, err := getMissingPartitionKeyAction(DropMissingPartitionKeyActionName)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), Drop, action)
}

func (s *BucketsSuite) TestGetMissingPartitionKeyAction_Include() {
	action, err := getMissingPartitionKeyAction(IncludeInBucketMissingPartitionKeyActionName)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), IncludeInBucket, action)
}

func (s *BucketsSuite) TestGetMissingPartitionKeyAction_Invalid() {
	_, err := getMissingPartitionKeyAction("invalid_action")
	assert.Error(s.T(), err)
}

func (s *BucketsSuite) TestNewBuckets_InvalidMissingParitionKeyAction() {
	cfg := Config{
		TargetBucket:              "bucketA",
		PossibleBuckets:           s.bucketDefs,
		MissingPartitionKeyAction: "invalid_action",
	}
	_, err := NewBuckets(cfg)
	assert.Error(s.T(), err)
}

func TestBucketsSuite(t *testing.T) {
	suite.Run(t, new(BucketsSuite))
}
