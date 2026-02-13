// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package consumer

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"
	"xmidt-org/splitter/internal/log"
	"xmidt-org/splitter/internal/metrics"
	"xmidt-org/splitter/internal/observe"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/twmb/franz-go/plugin/kprom"
)

var (
	ErrNilMessageHandler = errors.New("message handler cannot be nil")
)

// Option is a functional option for configuring a Consumer.
type Option interface {
	apply(*Consumer) error
}

type optionFunc func(*Consumer) error

var _ Option = optionFunc(nil)

func (f optionFunc) apply(c *Consumer) error {
	return f(c)
}

// consumerConfig holds the configuration for a Consumer.
type consumerConfig struct {
	// Required options
	brokers []string
	topics  []string
	groupID string
	handler MessageHandler

	// franz-go client options
	kgoOpts []kgo.Opt
}

// validate ensures all required options are set.
func validate(config *consumerConfig) error {
	if len(config.brokers) == 0 {
		return errors.New("brokers are required")
	}
	if len(config.topics) == 0 {
		return errors.New("topics are required")
	}
	if config.groupID == "" {
		return errors.New("group ID is required")
	}
	if config.handler == nil {
		return errors.New("message handler is required")
	}

	return nil
}

// Required Options

// WithBrokers sets the Kafka broker addresses.
func WithBrokers(brokers ...string) Option {
	return optionFunc(func(c *Consumer) error {
		c.config.brokers = brokers
		return nil
	})
}

// WithTopics sets the topics to consume from.
// This is a required option.
func WithTopics(topics ...string) Option {
	return optionFunc(func(c *Consumer) error {
		c.config.topics = topics
		return nil
	})
}

// WithGroupID sets the consumer group ID.
// This is a required option for consumer group functionality.
func WithGroupID(groupID string) Option {
	return optionFunc(func(c *Consumer) error {
		c.config.groupID = groupID
		return nil
	})
}

// WithMessageHandler sets the handler for consumed messages.
// This is a required option.
func WithMessageHandler(handler MessageHandler) Option {
	return optionFunc(func(c *Consumer) error {
		c.config.handler = handler
		return nil
	})
}

// Session and Heartbeat Options

// WithSessionTimeout sets the consumer group session timeout.
// If not specified, franz-go's default (45s) is used.
func WithSessionTimeout(timeout time.Duration) Option {
	return optionFunc(func(c *Consumer) error {
		if timeout != 0 {
			c.config.kgoOpts = append(c.config.kgoOpts, kgo.SessionTimeout(timeout))
		}
		return nil
	})
}

// WithHeartbeatInterval sets the consumer group heartbeat interval.
// If not specified, franz-go's default (3s) is used.
func WithHeartbeatInterval(interval time.Duration) Option {
	return optionFunc(func(c *Consumer) error {
		if interval != 0 {
			c.config.kgoOpts = append(c.config.kgoOpts, kgo.HeartbeatInterval(interval))
		}
		return nil
	})
}

// WithRebalanceTimeout sets the maximum time a rebalance can take.
// If not specified, franz-go's default is used.
func WithRebalanceTimeout(timeout time.Duration) Option {
	return optionFunc(func(c *Consumer) error {
		if timeout != 0 {
			c.config.kgoOpts = append(c.config.kgoOpts, kgo.RebalanceTimeout(timeout))
		}
		return nil
	})
}

// Fetch Options

// WithFetchMinBytes sets the minimum bytes to fetch in a request.
// If not specified, franz-go's default (1 byte) is used.
func WithFetchMinBytes(bytes int32) Option {
	return optionFunc(func(c *Consumer) error {
		if bytes > 0 {
			c.config.kgoOpts = append(c.config.kgoOpts, kgo.FetchMinBytes(bytes))
		}
		return nil
	})
}

// WithFetchMaxBytes sets the maximum bytes to fetch in a request.
// If not specified, franz-go's default (50MB) is used.
func WithFetchMaxBytes(bytes int32) Option {
	return optionFunc(func(c *Consumer) error {
		if bytes > 0 {
			c.config.kgoOpts = append(c.config.kgoOpts, kgo.FetchMaxBytes(bytes))
		}
		return nil
	})
}

// WithFetchMaxWait sets the maximum time to wait for fetch min bytes.
// If not specified, franz-go's default (5s) is used.
func WithFetchMaxWait(wait time.Duration) Option {
	return optionFunc(func(c *Consumer) error {
		if wait != 0 {
			c.config.kgoOpts = append(c.config.kgoOpts, kgo.FetchMaxWait(wait))
		}
		return nil
	})
}

// WithFetchMaxPartitionBytes sets the maximum bytes to fetch from a single partition.
// If not specified, franz-go's default (1MB) is used.
func WithFetchMaxPartitionBytes(bytes int32) Option {
	return optionFunc(func(c *Consumer) error {
		if bytes > 0 {
			c.config.kgoOpts = append(c.config.kgoOpts, kgo.FetchMaxPartitionBytes(bytes))
		}
		return nil
	})
}

// WithMaxConcurrentFetches sets the maximum number of concurrent fetch requests.
// If not specified, franz-go's default is used.
func WithMaxConcurrentFetches(max int) Option {
	return optionFunc(func(c *Consumer) error {
		if max > 0 {
			c.config.kgoOpts = append(c.config.kgoOpts, kgo.MaxConcurrentFetches(max))
		}
		return nil
	})
}

// Auto-Commit Options

// WithAutoCommitInterval sets the interval for automatic offset commits.
// If not specified, franz-go's default (5s) is used.
func WithAutoCommitInterval(interval time.Duration) Option {
	return optionFunc(func(c *Consumer) error {
		if interval > 0 {
			c.config.kgoOpts = append(c.config.kgoOpts, kgo.AutoCommitInterval(interval))
		}
		return nil
	})
}

// SASL Authentication Options

// WithSASLPlain configures PLAIN SASL authentication.
func WithSASLPlain(user, pass string) Option {
	return optionFunc(func(c *Consumer) error {
		mechanism := plain.Auth{
			User: user,
			Pass: pass,
		}.AsMechanism()
		c.config.kgoOpts = append(c.config.kgoOpts, kgo.SASL(mechanism))
		return nil
	})
}

// WithSASLScram256 configures SCRAM-SHA-256 SASL authentication.
func WithSASLScram256(user, pass string) Option {
	return optionFunc(func(c *Consumer) error {
		mechanism := scram.Auth{
			User: user,
			Pass: pass,
		}.AsSha256Mechanism()
		c.config.kgoOpts = append(c.config.kgoOpts, kgo.SASL(mechanism))
		return nil
	})
}

// WithSASLScram512 configures SCRAM-SHA-512 SASL authentication.
func WithSASLScram512(user, pass string) Option {
	return optionFunc(func(c *Consumer) error {
		mechanism := scram.Auth{
			User: user,
			Pass: pass,
		}.AsSha512Mechanism()
		c.config.kgoOpts = append(c.config.kgoOpts, kgo.SASL(mechanism))
		return nil
	})
}

// TLS Options

// WithTLS configures TLS encryption.
// If tlsConfig is nil, the system's default TLS configuration is used.
func WithTLS(tlsConfig *tls.Config) Option {
	return optionFunc(func(c *Consumer) error {
		c.config.kgoOpts = append(c.config.kgoOpts, kgo.DialTLSConfig(tlsConfig))
		return nil
	})
}

func WithTLSConfig(t *TLSConfig) Option {
	return optionFunc(func(c *Consumer) error {
		if t == nil || !t.Enabled {
			return nil
		}

		tlsConfig := &tls.Config{
			InsecureSkipVerify: t.InsecureSkipVerify, // #nosec G402 - user configurable for testing/development
		}

		// Load CA certificate if provided
		if t.CAFile != "" {
			caCert, err := os.ReadFile(t.CAFile)
			if err != nil {
				return fmt.Errorf("failed to read ca file: %w", err)
			}

			caCertPool := x509.NewCertPool()
			if !caCertPool.AppendCertsFromPEM(caCert) {
				return errors.New("failed to parse consumer tls certificate")
			}
			tlsConfig.RootCAs = caCertPool
		}

		// Load client certificate if provided
		if t.CertFile != "" && t.KeyFile != "" {
			cert, err := tls.LoadX509KeyPair(t.CertFile, t.KeyFile)
			if err != nil {
				return fmt.Errorf("failed to load client certificate: %w", err)
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		}

		c.config.kgoOpts = append(c.config.kgoOpts, kgo.DialTLSConfig(tlsConfig))

		return nil
	})
}

func WithSASLConfig(s *SASLConfig) Option {
	return optionFunc(func(c *Consumer) error {
		if s == nil {
			return nil
		}

		if s.Username == "" {
			return errors.New("sasl username is required")
		}
		if s.Password == "" {
			return errors.New("sasl password is required")
		}

		switch s.Mechanism {
		case "PLAIN":
			WithSASLPlain(s.Username, s.Password)
		case "SCRAM-SHA-256": //"SCRAM-SHA-256", "SCRAM-SHA-512":
			WithSASLScram256(s.Username, s.Password)
		case "SCRAM-SHA-512":
			WithSASLScram512(s.Username, s.Password)
		default:
			return fmt.Errorf("unsupported sasl mechanism: %s", s.Mechanism)
		}

		return nil
	})
}

// Retry and Backoff Options

// WithRequestRetries sets the number of retries for requests.
// If not specified, franz-go's default is used.
func WithRequestRetries(retries int) Option {
	return optionFunc(func(c *Consumer) error {
		c.config.kgoOpts = append(c.config.kgoOpts, kgo.RequestRetries(retries))
		return nil
	})
}

// WithRetryBackoff sets the retry backoff function.
// If not specified, franz-go's default exponential backoff is used.
func WithRetryBackoff(fn func(int) time.Duration) Option {
	return optionFunc(func(c *Consumer) error {
		if fn != nil {
			c.config.kgoOpts = append(c.config.kgoOpts, kgo.RetryBackoffFn(fn))
		}
		return nil
	})
}

// Connection Options

// WithConnIdleTimeout sets the connection idle timeout.
// If not specified, franz-go's default is used.
func WithConnIdleTimeout(timeout time.Duration) Option {
	return optionFunc(func(c *Consumer) error {
		if timeout != 0 {
			c.config.kgoOpts = append(c.config.kgoOpts, kgo.ConnIdleTimeout(timeout))
		}
		return nil
	})
}

// WithRequestTimeoutOverhead sets additional timeout overhead for requests.
// If not specified, franz-go's default is used.
func WithRequestTimeoutOverhead(overhead time.Duration) Option {
	return optionFunc(func(c *Consumer) error {
		if overhead != 0 {
			c.config.kgoOpts = append(c.config.kgoOpts, kgo.RequestTimeoutOverhead(overhead))
		}
		return nil
	})
}

// Logger Options

// WithKafkaLogger sets a custom logger for franz-go.
// The logger will receive franz-go's internal log messages.
func WithKafkaLogger(logger *slog.Logger) Option {
	return optionFunc(func(c *Consumer) error {
		if logger != nil {
			kgoLogger := &slogAdapter{logger: logger}
			c.config.kgoOpts = append(c.config.kgoOpts, kgo.WithLogger(kgoLogger))
		}
		return nil
	})
}

// WithLogEmitter sets the log event emitter for the consumer.
// The emitter will receive errors and consumer lifecycle events.
func WithLogEmitter(emitter *observe.Subject[log.Event]) Option {
	return optionFunc(func(c *Consumer) error {
		if emitter != nil {
			c.logEmitter = emitter
		}
		return nil
	})
}

// slogAdapter adapts slog.Logger to kgo.Logger interface.
type slogAdapter struct {
	logger *slog.Logger
}

// Level implements kgo.Logger.
func (s *slogAdapter) Level() kgo.LogLevel {
	return kgo.LogLevelInfo
}

// Log implements kgo.Logger.
func (s *slogAdapter) Log(level kgo.LogLevel, msg string, keyvals ...interface{}) {
	attrs := make([]interface{}, 0, len(keyvals))
	attrs = append(attrs, keyvals...)

	switch level {
	case kgo.LogLevelDebug:
		s.logger.Debug(msg, attrs...)
	case kgo.LogLevelInfo:
		s.logger.Info(msg, attrs...)
	case kgo.LogLevelWarn:
		s.logger.Warn(msg, attrs...)
	case kgo.LogLevelError:
		s.logger.Error(msg, attrs...)
	default:
		s.logger.Info(msg, attrs...)
	}
}

// Metrics and Hooks Options

// WithPrometheusMetrics configures franz-go to emit Prometheus metrics.
// This uses the kprom plugin to register metrics with the provided namespace and subsystem.
// Metrics will be automatically registered with the default Prometheus registry.
//
// Example metrics exposed:
//   - {namespace}_{subsystem}_records_consumed_total
//   - {namespace}_{subsystem}_records_lag
//   - {namespace}_{subsystem}_fetch_bytes_total
//   - {namespace}_{subsystem}_buffered_fetch_records
//   - And many more...
//
// For full list of metrics, see: https://pkg.go.dev/github.com/twmb/franz-go/plugin/kprom
func WithPrometheusMetrics(namespace, subsystem string) Option {
	return optionFunc(func(c *Consumer) error {
		if namespace == "" {
			return fmt.Errorf("metrics namespace cannot be empty")
		}

		if subsystem == "" {
			return fmt.Errorf("metrics subsystem cannot be empty")
		}

		// Create kprom metrics with the specified namespace and subsystem
		metrics := kprom.NewMetrics(
			namespace,
			kprom.Subsystem(subsystem),
		)

		// Add the metrics hook to the consumer
		c.config.kgoOpts = append(c.config.kgoOpts, kgo.WithHooks(metrics))
		return nil
	})
}

func WithMetricsEmitter(emitter *observe.Subject[metrics.Event]) Option {
	return optionFunc(func(c *Consumer) error {
		if emitter != nil {
			c.metricEmitter = emitter
		}
		return nil
	})
}

// WithHooks adds custom hooks for monitoring consumer events.
// Hooks can be used for metrics, logging, or other observability needs.
func WithHooks(hooks ...kgo.Hook) Option {
	return optionFunc(func(c *Consumer) error {
		c.config.kgoOpts = append(c.config.kgoOpts, kgo.WithHooks(hooks...))
		return nil
	})
}

// Offset Management Options

func WithConsumeFromTheBeginning(beginning bool) Option {
	return optionFunc(func(c *Consumer) error {
		if beginning {
			offset := kgo.NewOffset().AtStart()
			c.config.kgoOpts = append(c.config.kgoOpts, kgo.ConsumeResetOffset(offset))
		}
		return nil
	})
}

// WithConsumeResetOffset sets where to start consuming if no offset exists.
// Common values: kgo.NewOffset().AtStart(), kgo.NewOffset().AtEnd()
func WithConsumeResetOffset(offset *kgo.Offset) Option {
	return optionFunc(func(c *Consumer) error {
		if offset != nil {
			c.config.kgoOpts = append(c.config.kgoOpts, kgo.ConsumeResetOffset(*offset))
		}
		return nil
	})
}

// Partition Assignment Options

// WithOnPartitionsAssigned sets a callback for when partitions are assigned.
// This can be used for custom initialization when partitions are assigned.
func WithOnPartitionsAssigned(fn func(context.Context, *kgo.Client, map[string][]int32)) Option {
	return optionFunc(func(c *Consumer) error {
		if fn != nil {
			c.config.kgoOpts = append(c.config.kgoOpts, kgo.OnPartitionsAssigned(fn))
		}
		return nil
	})
}

// WithOnPartitionsRevoked sets a callback for when partitions are revoked.
// This can be used for custom cleanup when partitions are revoked.
func WithOnPartitionsRevoked(fn func(context.Context, *kgo.Client, map[string][]int32)) Option {
	return optionFunc(func(c *Consumer) error {
		if fn != nil {
			c.config.kgoOpts = append(c.config.kgoOpts, kgo.OnPartitionsRevoked(fn))
		}
		return nil
	})
}

// WithOnPartitionsLost sets a callback for when partitions are lost.
// This occurs during unclean rebalances.
func WithOnPartitionsLost(fn func(context.Context, *kgo.Client, map[string][]int32)) Option {
	return optionFunc(func(c *Consumer) error {
		if fn != nil {
			c.config.kgoOpts = append(c.config.kgoOpts, kgo.OnPartitionsLost(fn))
		}
		return nil
	})
}

// Client ID Option

// WithClientID sets the client ID for the Kafka connection.
// If not specified, franz-go generates a default ID.
func WithClientID(clientID string) Option {
	return optionFunc(func(c *Consumer) error {
		if clientID != "" {
			c.clientId = clientID
			c.config.kgoOpts = append(c.config.kgoOpts, kgo.ClientID(clientID))
		}
		return nil
	})
}

// Rack Awareness Option

// WithRack sets the rack ID for rack-aware partition assignment.
// This can improve data locality and reduce cross-rack traffic.
func WithRack(rack string) Option {
	return optionFunc(func(c *Consumer) error {
		if rack != "" {
			c.config.kgoOpts = append(c.config.kgoOpts, kgo.Rack(rack))
		}
		return nil
	})
}

// Instance ID Option

// WithInstanceID sets a static consumer group member ID.
// This enables static membership, avoiding unnecessary rebalances.
func WithInstanceID(instanceID string) Option {
	return optionFunc(func(c *Consumer) error {
		if instanceID != "" {
			c.config.kgoOpts = append(c.config.kgoOpts, kgo.InstanceID(instanceID))
		}
		return nil
	})
}
