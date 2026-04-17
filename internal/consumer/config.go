// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package consumer

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Config represents the YAML configuration for the Kafka consumer.
// It can be unmarshaled via goschtalt and converted to functional options.
type Config struct {
	// Required fields
	Brokers []string
	Topics  []string
	GroupID string

	// Session and heartbeat
	SessionTimeout    time.Duration
	HeartbeatInterval time.Duration
	RebalanceTimeout  time.Duration

	// Fetch configuration
	FetchMinBytes          int32
	FetchMaxBytes          int32
	FetchMaxWait           time.Duration
	FetchMaxPartitionBytes int32
	MaxConcurrentFetches   int
	// Auto-commit
	AutoCommitInterval time.Duration
	//DisableAutoCommit  bool          `yaml:"disable_auto_commit,omitempty"`

	// SASL authentication
	SASL *SASLConfig

	// TLS configuration
	TLS *TLSConfig

	// Retry and backoff
	RequestRetries int

	// Connection
	ConnIdleTimeout        time.Duration
	RequestTimeoutOverhead time.Duration
	// Client identification
	ClientID   string
	Rack       string
	InstanceID string
	// Offset management
	ConsumeFromBeginning bool

	// Fetch State Management
	ResumeDelaySeconds          int
	ConsecutiveFailureThreshold int

	// Prometheus metrics configuration
	Prometheus *PrometheusConfig
}

// SASLConfig contains SASL authentication configuration.
type SASLConfig struct {
	Mechanism string
	Username  string
	// #nosec G117 -- field required for configuration, not logged or exposed
	Password string
}

// TLSConfig contains TLS encryption configuration.
type TLSConfig struct {
	Enabled            bool
	CAFile             string
	CertFile           string
	KeyFile            string
	InsecureSkipVerify bool
}

// PrometheusConfig represents Prometheus metrics configuration for the consumer.
// This consolidates all prometheus/kprom settings for franz-go into a single struct.
type PrometheusConfig struct {
	// Namespace is the prometheus namespace for metrics (e.g., "xmidt")
	Namespace string `yaml:"namespace,omitempty"`

	// Subsystem is the prometheus subsystem name (e.g., "splitter_consumer")
	Subsystem string `yaml:"subsystem,omitempty"`

	// Registerer is the prometheus registerer to use for metrics.
	// If nil, metrics will be registered with the default prometheus registry.
	// This field is typically set programmatically, not via YAML.
	Registerer prometheus.Registerer `yaml:"-"`

	// Optional franz-go kprom metrics (disabled by default)
	// Core metrics (uncompressed bytes, records, batches, by_node, by_topic) are always enabled

	// EnableCompressedBytes tracks compressed bytes (fetch_compressed_bytes_total).
	// Default: false (adds overhead)
	EnableCompressedBytes bool `yaml:"enable_compressed_bytes,omitempty"`

	// EnableGoCollectors adds Go runtime metrics (goroutines, memory, etc.).
	// Default: false
	EnableGoCollectors bool `yaml:"enable_go_collectors,omitempty"`

	// WithClientLabel adds a "client_id" label to all metrics.
	// Default: false
	WithClientLabel bool `yaml:"with_client_label,omitempty"`
}

// IsCompressedBytesEnabled returns true if compressed bytes metric should be enabled.
func (p *PrometheusConfig) IsCompressedBytesEnabled() bool {
	if p == nil {
		return false
	}
	return p.EnableCompressedBytes
}

// IsGoCollectorsEnabled returns true if Go runtime metrics should be enabled.
func (p *PrometheusConfig) IsGoCollectorsEnabled() bool {
	if p == nil {
		return false
	}
	return p.EnableGoCollectors
}

// IsClientLabelEnabled returns true if client_id label should be added.
func (p *PrometheusConfig) IsClientLabelEnabled() bool {
	if p == nil {
		return false
	}
	return p.WithClientLabel
}

// Duration is a wrapper around time.Duration that supports YAML unmarshaling.
// type Duration time.Duration

// // UnmarshalText implements encoding.TextUnmarshaler for Duration.
// func (d *Duration) UnmarshalText(text []byte) error {
// 	duration, err := time.ParseDuration(string(text))
// 	if err != nil {
// 		return err
// 	}
// 	*d = Duration(duration)
// 	return nil
// }

// // MarshalText implements encoding.TextMarshaler for Duration.
// func (d Duration) MarshalText() ([]byte, error) {
// 	return []byte(time.Duration(d).String()), nil
// }

// // Validate checks that required fields are set and values are valid.
// func (c *Config) Validate() error {
// 	if len(c.Brokers) == 0 {
// 		return errors.New("brokers are required")
// 	}
// 	if len(c.Topics) == 0 {
// 		return errors.New("topics are required")
// 	}
// 	if c.GroupID == "" {
// 		return errors.New("group_id is required")
// 	}

// 	// Validate SASL if provided
// 	if c.SASL != nil {
// 		if err := c.SASL.Validate(); err != nil {
// 			return fmt.Errorf("invalid sasl config: %w", err)
// 		}
// 	}

// 	// Validate TLS if provided
// 	if c.TLS != nil && c.TLS.Enabled {
// 		if err := c.TLS.Validate(); err != nil {
// 			return fmt.Errorf("invalid tls config: %w", err)
// 		}
// 	}

// 	return nil
// }

// // Validate checks SASL configuration.
// func (s *SASLConfig) Validate() error {
// 	if s == nil {
// 		return nil
// 	}

// 	switch s.Mechanism {
// 	case "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512":
// 		// Valid mechanisms
// 	default:
// 		return fmt.Errorf("unsupported sasl mechanism: %s", s.Mechanism)
// 	}

// 	if s.Username == "" {
// 		return errors.New("sasl username is required")
// 	}
// 	if s.Password == "" {
// 		return errors.New("sasl password is required")
// 	}

// 	return nil
// }

// // Validate checks TLS configuration.
// func (t *TLSConfig) Validate() error {
// 	if t == nil || !t.Enabled {
// 		return nil
// 	}

// 	// If CA file is specified, it must exist
// 	if t.CAFile != "" {
// 		if _, err := os.Stat(t.CAFile); err != nil {
// 			return fmt.Errorf("ca_file not found: %w", err)
// 		}
// 	}

// 	// If cert and key are specified, both must exist
// 	if t.CertFile != "" || t.KeyFile != "" {
// 		if t.CertFile == "" || t.KeyFile == "" {
// 			return errors.New("both cert_file and key_file must be specified together")
// 		}
// 		if _, err := os.Stat(t.CertFile); err != nil {
// 			return fmt.Errorf("cert_file not found: %w", err)
// 		}
// 		if _, err := os.Stat(t.KeyFile); err != nil {
// 			return fmt.Errorf("key_file not found: %w", err)
// 		}
// 	}

// 	return nil
// }

// // ToTLSConfig converts TLSConfig to *tls.Config.
// func (t *TLSConfig) ToTLSConfig() (*tls.Config, error) {
// 	if !t.Enabled {
// 		return nil, nil
// 	}

// 	tlsConfig := &tls.Config{
// 		InsecureSkipVerify: t.InsecureSkipVerify,
// 	}

// 	// Load CA certificate if provided
// 	if t.CAFile != "" {
// 		caCert, err := os.ReadFile(t.CAFile)
// 		if err != nil {
// 			return nil, fmt.Errorf("failed to read ca file: %w", err)
// 		}

// 		caCertPool := x509.NewCertPool()
// 		if !caCertPool.AppendCertsFromPEM(caCert) {
// 			return nil, errors.New("failed to parse ca certificate")
// 		}
// 		tlsConfig.RootCAs = caCertPool
// 	}

// 	// Load client certificate if provided
// 	if t.CertFile != "" && t.KeyFile != "" {
// 		cert, err := tls.LoadX509KeyPair(t.CertFile, t.KeyFile)
// 		if err != nil {
// 			return nil, fmt.Errorf("failed to load client certificate: %w", err)
// 		}
// 		tlsConfig.Certificates = []tls.Certificate{cert}
// 	}

// 	return tlsConfig, nil
// }
