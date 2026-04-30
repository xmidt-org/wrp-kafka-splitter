// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package consumer

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"xmidt-org/splitter/internal/log"
	"xmidt-org/splitter/internal/observe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/xmidt-org/wrp-go/v5"
	"github.com/xmidt-org/wrpkafka"
)

// HandlerTestSuite tests WRPMessageHandler using testify suite and mocks.
type HandlerTestSuite struct {
	suite.Suite
	producer   *MockPublisher
	buckets    *MockBuckets
	logEmitter *observe.Subject[log.Event]
	handler    *WRPMessageHandler
}

func (suite *HandlerTestSuite) SetupTest() {
	suite.producer = &MockPublisher{}
	suite.buckets = &MockBuckets{}
	suite.logEmitter = log.NewNoop()

	h, err := NewWRPMessageHandler(
		WithHandlerProducer(suite.producer),
		WithBuckets(suite.buckets),
		WithHandlerLogEmitter(suite.logEmitter),
	)
	suite.Require().NoError(err)
	suite.handler = h
}

func (suite *HandlerTestSuite) TestHandleMessage_MalformedWRP() {
	record := &kgo.Record{Value: []byte("not-a-msgpack-wrp")}
	outcome, err := suite.handler.HandleMessage(context.Background(), record)
	assert.ErrorIs(suite.T(), err, ErrMalformedMsg)
	assert.Equal(suite.T(), Failed, outcome)
}

func (suite *HandlerTestSuite) TestHandleMessage_NotInTargetBucket() {
	msg := createValidWrpMsg()
	var buf bytes.Buffer
	enc := wrp.NewEncoder(&buf, wrp.Msgpack)
	_ = enc.Encode(msg, wrp.NoStandardValidation())
	b := buf.Bytes()
	suite.buckets.On("IsInTargetBucket", mock.Anything).Return(false, nil).Once()
	record := &kgo.Record{Value: b}
	outcome, err := suite.handler.HandleMessage(context.Background(), record)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), Skipped, outcome)
	suite.buckets.AssertExpectations(suite.T())
}

func (suite *HandlerTestSuite) TestHandleMessage_ProduceError() {
	msg := createValidWrpMsg()
	var buf bytes.Buffer
	enc := wrp.NewEncoder(&buf, wrp.Msgpack)
	_ = enc.Encode(msg, wrp.NoStandardValidation())
	b := buf.Bytes()
	suite.buckets.On("IsInTargetBucket", mock.Anything).Return(true, nil).Once()
	suite.producer.On("Produce", mock.Anything, mock.Anything).Return(wrpkafka.Failed, errors.New("produce fail")).Once()
	record := &kgo.Record{Value: b}
	outcome, err := suite.handler.HandleMessage(context.Background(), record)
	assert.ErrorContains(suite.T(), err, "produce fail")
	assert.Equal(suite.T(), Failed, outcome)
	suite.buckets.AssertExpectations(suite.T())
	suite.producer.AssertExpectations(suite.T())
}

func (suite *HandlerTestSuite) TestHandleMessage_Success() {
	msg := createValidWrpMsg()
	var buf bytes.Buffer
	enc := wrp.NewEncoder(&buf, wrp.Msgpack)
	_ = enc.Encode(msg, wrp.NoStandardValidation())
	b := buf.Bytes()
	suite.buckets.On("IsInTargetBucket", mock.Anything).Return(true, nil).Once()
	suite.producer.On("Produce", mock.Anything, mock.Anything).Return(wrpkafka.Attempted, nil).Once()
	record := &kgo.Record{Value: b}
	outcome, err := suite.handler.HandleMessage(context.Background(), record)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), Attempted, outcome)
	suite.buckets.AssertExpectations(suite.T())
	suite.producer.AssertExpectations(suite.T())
}

func (suite *HandlerTestSuite) TestHandleMessage_SuccessWithNoStandardValidation() {
	// Test that source value gets decoded successfully even if it doesn't conform to standard WRP validation rules, since we're using NoStandardValidation.
	msg := &wrp.Message{
		Type:        wrp.SimpleEventMessageType,
		Source:      "mac:FAKEMAC",
		Destination: "event:device-status/mac:d89c8e3a28e8/offline",
		Payload:     []byte(`{"test":"data"}`),
	}
	var buf bytes.Buffer
	enc := wrp.NewEncoder(&buf, wrp.Msgpack)
	_ = enc.Encode(msg, wrp.NoStandardValidation())
	b := buf.Bytes()
	suite.buckets.On("IsInTargetBucket", mock.Anything).Return(true, nil).Once()
	suite.producer.On("Produce", mock.Anything, mock.Anything).Return(wrpkafka.Attempted, nil).Once()
	record := &kgo.Record{Value: b}
	outcome, err := suite.handler.HandleMessage(context.Background(), record)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), Attempted, outcome)
	suite.buckets.AssertExpectations(suite.T())
	suite.producer.AssertExpectations(suite.T())
}

func createValidWrpMsg() *wrp.Message {
	return &wrp.Message{
		Type:        wrp.SimpleEventMessageType,
		Source:      "dns:integration-test.talaria.com",
		Destination: "event:device-status/mac:4ca161000109/online",
	}
}

func TestHandlerTestSuite(t *testing.T) {
	suite.Run(t, new(HandlerTestSuite))
}
