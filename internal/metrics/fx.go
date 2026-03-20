// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/xmidt-org/touchstone"
	"github.com/xmidt-org/touchstone/touchkit"
	"go.uber.org/fx"
)

type metricType int

const (
	COUNTER   metricType = 1
	GAUGE     metricType = 2
	HISTOGRAM metricType = 3
)

type metricDefinition struct {
	Type      metricType
	Name      string         // the metric name (prometheus.CounterOpts.Name, etc)
	Help      string         // the metric help (prometheus.CounterOpts.Help, etc)
	Labels    string         // a comma separated list of labels that are whitespace trimmed
	Buckets   string         // a comma separated list of labels that are whitespace trimmed
	GaugeFunc func() float64 // optional function for GaugeFunc metrics
}

// metrics
const (
	ConsumerFetchErrors    = "fetch_errors"
	ConsumerCommitErrors   = "commit_errors"
	ConsumerPauses         = "fetch_pauses"
	BucketKeyErrorCount    = "bucket_key_error_count"
	PublisherOutcomes      = "publish_outcomes"
	PublisherErrorsCounter = "publish_errors_total"

	// Kafka publisher metrics (wrpkafka event listeners)
	KafkaPublished         = "kafka_messages_published_total"
	KafkaPublishLatency    = "kafka_publish_latency_seconds"
	KafkaBufferUtilization = "kafka_buffer_utilization_percentage"
)

// labels
const (
	ErrorTypeLabel          = "error_type"
	TopicLabel              = "topic"
	PartitionLabel          = "partition"
	GroupLabel              = "group"
	MemberIdLabel           = "member"
	ClientIdLabel           = "client"
	TopicShardStrategyLabel = "topic_shard_strategy"
	OutcomeLabel            = "outcome"
)

// canned values
const (
	OutcomeSuccess = "success"
	OutcomeFailure = "failure"
)

var fxMetrics = []metricDefinition{
	{
		Type:   COUNTER,
		Name:   ConsumerFetchErrors,
		Help:   "Total number of fetch errors",
		Labels: fmt.Sprintf("%s,%s,%s", PartitionLabel, GroupLabel, ClientIdLabel),
	},
	{
		Type:   COUNTER,
		Name:   ConsumerCommitErrors,
		Help:   "Total number of commit errors",
		Labels: fmt.Sprintf("%s,%s,%s", GroupLabel, MemberIdLabel, ClientIdLabel),
	},
	{
		Type:   GAUGE,
		Name:   ConsumerPauses,
		Help:   "Current pause state (1=paused, 0=running)",
		Labels: fmt.Sprintf("%s,%s", GroupLabel, ClientIdLabel),
	},
	{
		Type:   COUNTER,
		Name:   BucketKeyErrorCount,
		Help:   "Total count of bucket key errors",
		Labels: ErrorTypeLabel,
	},
	{
		Type:   COUNTER,
		Name:   PublisherOutcomes,
		Help:   "Handler outcomes when publishing messages",
		Labels: OutcomeLabel,
	},
	{
		Type:   COUNTER,
		Name:   PublisherErrorsCounter,
		Help:   "Total number of publish errors",
		Labels: fmt.Sprintf("%s,%s,%s", ErrorTypeLabel, TopicLabel, TopicShardStrategyLabel),
	},
	{
		Type:   COUNTER,
		Name:   KafkaPublished,
		Help:   "Total number of messages published to Kafka (including failures)",
		Labels: fmt.Sprintf("%s,%s,%s", TopicLabel, TopicShardStrategyLabel, ErrorTypeLabel),
	},
	{
		Type:    HISTOGRAM,
		Name:    KafkaPublishLatency,
		Help:    "Latency of publishing messages to Kafka",
		Labels:  fmt.Sprintf("%s,%s", TopicLabel, ErrorTypeLabel),
		Buckets: "0.005,0.01,0.025,0.05,0.1,0.25,0.5,1,2.5,5,10",
	},
	{
		Type:      GAUGE,
		Name:      KafkaBufferUtilization,
		Help:      "Percentage of Kafka producer buffer utilization",
		Labels:    TopicLabel,
		GaugeFunc: getKafkaBufferUtilization,
	},
}

// BufferUtilizationFunc is a function type that returns the current and max buffer utilization values
// This matches the signature of wrpkafka.Publisher.BufferedRecords()
type BufferUtilizationFunc func() (currentRecords, maxRecords int, currentBytes, maxBytes int64)

// BufferUtilizationFunc is a global function that can be set by the publisher during start
var BufferUtilization BufferUtilizationFunc

// getKafkaBufferUtilization calculates the buffer utilization ratio (0.0-1.0)
// Called automatically by Prometheus on each scrape
func getKafkaBufferUtilization() float64 {
	if BufferUtilization == nil {
		return 0.0
	}
	current, max, _, _ := BufferUtilization()
	if max == 0 {
		return 0.0
	}
	return float64(current) / float64(max)
}

func Provide() fx.Option {
	opts := make([]fx.Option, 0, len(fxMetrics))

	for _, m := range fxMetrics {
		labels := strings.Split(m.Labels, ",")
		for i := range labels {
			labels[i] = strings.TrimSpace(labels[i])
		}

		var opt fx.Option

		switch m.Type {
		case COUNTER:
			opt = touchkit.Counter(
				prometheus.CounterOpts{
					Name: m.Name,
					Help: m.Help,
				},
				labels...)

		case GAUGE:
			opt = touchkit.Gauge(
				prometheus.GaugeOpts{
					Name: m.Name,
					Help: m.Help,
				},
				labels...)

		case HISTOGRAM:
			buckets := strings.Split(m.Buckets, ",")
			bucketLimits := make([]float64, len(buckets))
			for i := range buckets {
				bucketLimit, err := strconv.ParseFloat(strings.TrimSpace(buckets[i]), 64)
				if err != nil {
					panic(fmt.Sprintf("bucket has non-numeric value '%s'", buckets[i]))
				}
				bucketLimits[i] = bucketLimit
			}
			opt = touchkit.Histogram(
				prometheus.HistogramOpts{
					Name:    m.Name,
					Help:    m.Help,
					Buckets: bucketLimits,
				},
				labels...)
		default:
			panic(fmt.Sprintf("unknown metric type %d for '%s'", m.Type, m.Name))
		}

		if opt == nil {
			panic(fmt.Sprintf("failed to create metric '%s'", m.Name))
		}

		if m.GaugeFunc != nil {
			// Create a GaugeFunc that calls the provided function
			opt = fx.Provide(fx.Annotated{
				Name: m.Name,
				Target: func(f *touchstone.Factory) (prometheus.GaugeFunc, error) {
					return f.NewGaugeFunc(
						prometheus.GaugeOpts{
							Name: m.Name,
							Help: m.Help,
						},
						m.GaugeFunc,
					)
				},
			})
		}

		opts = append(opts, opt)
	}

	return fx.Options(opts...)
}
