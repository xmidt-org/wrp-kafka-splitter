// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package publisher

import (
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
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/xmidt-org/wrpkafka"
)

var (
	ErrMissingBrokers     = errors.New("brokers cannot be empty")
	ErrMissingTopicRoutes = errors.New("topic routes cannot be empty")
)

// Option is a functional option for configuring a Publisher.
type Option interface {
	apply(*Publisher) error
}

type optionFunc func(*Publisher) error

var _ Option = optionFunc(nil)

func (f optionFunc) apply(p *Publisher) error {
	return f(p)
}

// publisherConfig holds the configuration for a Publisher.
type publisherConfig struct {
	// Required options
	brokers     []string
	topicRoutes []wrpkafka.TopicRoute

	// Optional publisher config fields (applied directly to wrpkafka.Publisher)
	sasl                   sasl.Mechanism
	tls                    *tls.Config
	maxBufferedRecords     int
	maxBufferedBytes       int
	requestTimeout         time.Duration
	cleanupTimeout         time.Duration
	maxRetries             int
	allowAutoTopicCreation bool
	logger                 kgo.Logger
	publishEventListeners  []func(*wrpkafka.PublishEvent)
}

// validate ensures all required configuration is present.
func (c *publisherConfig) validate() error {
	if len(c.brokers) == 0 {
		return ErrMissingBrokers
	}
	if len(c.topicRoutes) == 0 {
		return ErrMissingTopicRoutes
	}
	return nil
}

// WithBrokers sets the Kafka broker addresses.
func WithBrokers(brokers ...string) Option {
	return optionFunc(func(p *Publisher) error {
		p.config.brokers = brokers
		return nil
	})
}

// WithTopicRoutes sets the WRP topic routing rules.
func WithTopicRoutes(routes ...wrpkafka.TopicRoute) Option {
	return optionFunc(func(p *Publisher) error {
		p.config.topicRoutes = routes
		return nil
	})
}

// WithLogger sets the logger for the publisher.
func WithLogger(logger *slog.Logger) Option {
	return optionFunc(func(p *Publisher) error {
		if logger != nil {
			p.logger = logger
		}
		return nil
	})
}

// WithLogEmitter sets the log event emitter.
func WithLogEmitter(emitter *observe.Subject[log.Event]) Option {
	return optionFunc(func(p *Publisher) error {
		if emitter != nil {
			p.logEmitter = emitter
		}
		return nil
	})
}

// WithMetricsEmitter sets the metrics event emitter.
func WithMetricsEmitter(emitter *observe.Subject[metrics.Event]) Option {
	return optionFunc(func(p *Publisher) error {
		if emitter != nil {
			p.metricEmitter = emitter
		}
		return nil
	})
}

// WithKafkaLogger sets the kafka client logger.
func WithKafkaLogger(logger kgo.Logger) Option {
	return optionFunc(func(p *Publisher) error {
		if logger != nil {
			p.config.logger = logger
		}
		return nil
	})
}

// WithMaxBufferedRecords sets the maximum number of records to buffer.
func WithMaxBufferedRecords(maxRecords int) Option {
	return optionFunc(func(p *Publisher) error {
		p.config.maxBufferedRecords = maxRecords
		return nil
	})
}

// WithMaxBufferedBytes sets the maximum bytes of records to buffer.
func WithMaxBufferedBytes(maxBytes int) Option {
	return optionFunc(func(p *Publisher) error {
		p.config.maxBufferedBytes = maxBytes
		return nil
	})
}

// WithRequestTimeout sets the maximum time to wait for broker responses.
func WithRequestTimeout(timeout time.Duration) Option {
	return optionFunc(func(p *Publisher) error {
		p.config.requestTimeout = timeout
		return nil
	})
}

// WithCleanupTimeout sets the maximum time to wait for buffered messages to flush on shutdown.
func WithCleanupTimeout(timeout time.Duration) Option {
	return optionFunc(func(p *Publisher) error {
		p.config.cleanupTimeout = timeout
		return nil
	})
}

// WithMaxRetries sets the number of retries for failed requests.
func WithMaxRetries(retries int) Option {
	return optionFunc(func(p *Publisher) error {
		p.config.maxRetries = retries
		return nil
	})
}

// WithAllowAutoTopicCreation enables automatic topic creation.
func WithAllowAutoTopicCreation(allow bool) Option {
	return optionFunc(func(p *Publisher) error {
		p.config.allowAutoTopicCreation = allow
		return nil
	})
}

// WithSASLConfig configures SASL authentication.
func WithSASLConfig(saslConfig *SASLConfig) Option {
	return optionFunc(func(p *Publisher) error {
		if saslConfig == nil {
			return nil
		}

		if saslConfig.Username == "" || saslConfig.Password == "" {
			return errors.New("SASL username and password are required")
		}

		var mechanism sasl.Mechanism
		switch saslConfig.Mechanism {
		case "PLAIN":
			mechanism = plain.Auth{
				User: saslConfig.Username,
				Pass: saslConfig.Password,
			}.AsMechanism()
		case "SCRAM-SHA-256":
			mechanism = scram.Auth{
				User: saslConfig.Username,
				Pass: saslConfig.Password,
			}.AsSha256Mechanism()
		case "SCRAM-SHA-512":
			mechanism = scram.Auth{
				User: saslConfig.Username,
				Pass: saslConfig.Password,
			}.AsSha512Mechanism()
		default:
			return fmt.Errorf("unsupported SASL mechanism: %s", saslConfig.Mechanism)
		}

		p.config.sasl = mechanism
		return nil
	})
}

// WithTLSConfig configures TLS encryption.
func WithTLSConfig(tlsConfig *TLSConfig) Option {
	return optionFunc(func(p *Publisher) error {
		if tlsConfig == nil || !tlsConfig.Enabled {
			return nil
		}

		config := &tls.Config{
			InsecureSkipVerify: tlsConfig.InsecureSkipVerify,
		}

		// Load CA certificate if provided
		if tlsConfig.CAFile != "" {
			caCert, err := os.ReadFile(tlsConfig.CAFile)
			if err != nil {
				return fmt.Errorf("failed to read CA file: %w", err)
			}

			caCertPool := x509.NewCertPool()
			if !caCertPool.AppendCertsFromPEM(caCert) {
				return errors.New("failed to parse CA certificate")
			}
			config.RootCAs = caCertPool
		}

		// Load client certificate and key if provided
		if tlsConfig.CertFile != "" && tlsConfig.KeyFile != "" {
			cert, err := tls.LoadX509KeyPair(tlsConfig.CertFile, tlsConfig.KeyFile)
			if err != nil {
				return fmt.Errorf("failed to load client certificate: %w", err)
			}
			config.Certificates = []tls.Certificate{cert}
		}

		p.config.tls = config
		return nil
	})
}

// WithPublishEventListener adds a publish event listener.
func WithPublishEventListener(listener func(*wrpkafka.PublishEvent)) Option {
	return optionFunc(func(p *Publisher) error {
		if listener != nil {
			p.config.publishEventListeners = append(p.config.publishEventListeners, listener)
		}
		return nil
	})
}

// WithSASLPlain configures SASL PLAIN authentication (convenience method).
func WithSASLPlain(username, password string) Option {
	return WithSASLConfig(&SASLConfig{
		Mechanism: "PLAIN",
		Username:  username,
		Password:  password,
	})
}

// WithSASLScram256 configures SASL SCRAM-SHA-256 authentication (convenience method).
func WithSASLScram256(username, password string) Option {
	return WithSASLConfig(&SASLConfig{
		Mechanism: "SCRAM-SHA-256",
		Username:  username,
		Password:  password,
	})
}

// WithSASLScram512 configures SASL SCRAM-SHA-512 authentication (convenience method).
func WithSASLScram512(username, password string) Option {
	return WithSASLConfig(&SASLConfig{
		Mechanism: "SCRAM-SHA-512",
		Username:  username,
		Password:  password,
	})
}

// WithTLS enables TLS with default settings (convenience method).
func WithTLS() Option {
	return WithTLSConfig(&TLSConfig{Enabled: true})
}
