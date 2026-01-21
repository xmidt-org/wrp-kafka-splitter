// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package app

import (
	"fmt"
	"log/slog"
	"xmidt-org/splitter/internal/metrics"

	"go.uber.org/fx"

	"github.com/alecthomas/kong"
	"github.com/goschtalt/goschtalt"
	"github.com/xmidt-org/arrange/arrangehttp"
	"github.com/xmidt-org/candlelight"
	"github.com/xmidt-org/touchstone"
	"github.com/xmidt-org/touchstone/touchhttp"
)

// ============================================================================
// OBSERVABILITY MODULE - Separates tracing, metrics, health, and pprof setup
// ============================================================================
// This module groups all infrastructure and observability concerns together,
// making it easy to:
// - Enable/disable observability features
// - Mock observability in tests
// - Replace observability implementations
// - Share observability infrastructure across features

// ObservabilityModule provides all observability-related DI components:
// - Structured logging (slog)
// - Distributed tracing (candlelight)
// - Prometheus metrics (touchstone)
// - Health check HTTP server
// - Metrics HTTP server
// - Pprof HTTP server
func ObservabilityModule() fx.Option {
	return fx.Module("observability",
		// Logging setup - must unmarshal LogConfig first
		fx.Provide(
			goschtalt.UnmarshalFunc[LogConfig]("logger", goschtalt.Optional()),
		),
		fx.Provide(
			provideLogger,
			provideLogObserver,
		),

		// Tracing configuration
		fx.Provide(
			goschtalt.UnmarshalFunc[candlelight.Config]("tracing"),
		),

		// Prometheus metrics setup
		fx.Provide(
			goschtalt.UnmarshalFunc[touchstone.Config]("prometheus"),
			goschtalt.UnmarshalFunc[touchhttp.Config]("prometheus_handler"),
			goschtalt.UnmarshalFunc[MetricsPath]("servers.metrics.path"),
			fx.Annotated{
				Name:   "servers.metrics.config",
				Target: goschtalt.UnmarshalFunc[arrangehttp.ServerConfig]("servers.metrics.http"),
			},
		),
		touchstone.Provide(),
		touchhttp.Provide(),
		metrics.Provide(), // Consumer errors counter and other metrics
		provideMetricEndpoint(),
		MetricObserversModule,

		// Health check server
		fx.Provide(
			goschtalt.UnmarshalFunc[HealthPath]("servers.health.path"),
			fx.Annotated{
				Name:   "servers.health.config",
				Target: goschtalt.UnmarshalFunc[arrangehttp.ServerConfig]("servers.health.http"),
			},
		),
		provideHealthCheck(),
		arrangehttp.ProvideServer("servers.health"),

		// Metrics server
		provideMetricEndpoint(),
		arrangehttp.ProvideServer("servers.metrics"),

		// Pprof server
		fx.Provide(
			goschtalt.UnmarshalFunc[PprofPathPrefix]("servers.pprof.path"),
			fx.Annotated{
				Name:   "servers.pprof.config",
				Target: goschtalt.UnmarshalFunc[arrangehttp.ServerConfig]("servers.pprof.http"),
			},
		),
		providePprofEndpoint(),
		arrangehttp.ProvideServer("servers.pprof"),
	)
}

// ============================================================================
// CORE BUSINESS LOGIC MODULE - Separates consumer and message handling
// ============================================================================
// This module groups the core business logic:
// - Kafka consumer configuration and setup
// - Message handling
// - Consumer lifecycle
//
// This module depends on observability (logs/metrics) but doesn't provide them.

// Provides a named type so it's a bit easier to flow through & use in fx.
type cliArgs []string

// Handle the CLI processing and return the processed input.
func provideCLI(args cliArgs) (*CLI, error) {
	return provideCLIWithOpts(args, false)
}

func provideCLIWithOpts(args cliArgs, testOpts bool) (*CLI, error) {
	var cli CLI

	// Create a no-op option to satisfy the kong.New() call.
	var opt kong.Option = kong.OptionFunc(
		func(*kong.Kong) error {
			return nil
		},
	)

	if testOpts {
		opt = kong.Writers(nil, nil)
	}

	parser, err := kong.New(&cli,
		kong.Name(applicationName),
		kong.Description("The wrp-kafka-splitter service.\n"+
			fmt.Sprintf("\tVersion:  %s\n", version)+
			fmt.Sprintf("\tDate:     %s\n", date)+
			fmt.Sprintf("\tCommit:   %s\n", commit)+
			fmt.Sprintf("\tBuilt By: %s\n", builtBy)+
			"\n"+
			"Configuration files are read in the following order unless the "+
			"-f option is presented:\n"+
			"----------------------------------------------------\n"+
			"\t1.  $(pwd)/xmidt-agent.{properties|yaml|yml}\n"+
			"\t2.  $(pwd)/conf.d/*.{properties|yaml|yml}\n"+
			"\t3.  /etc/xmidt-agent/xmidt-agent.{properties|yaml|yml}\n"+
			"\t4.  /etc/xmidt-agent/xonfig.d/*.{properties|yaml|yml}\n"+
			"\nIf an exact file (1 or 3) is found, the search stops.  If "+
			"a conf.d directory is found, all files in that directory are "+
			"read in lexical order."+
			"\n\nWhen the -f is used, it adds to the list of files or directories "+
			"to read.  The -f option can be used multiple times."+
			"\n\nEnvironment variables are may be used in the configuration "+
			"files.  The environment variables are expanded in the "+
			"configuration files using the standard ${ VAR } syntax."+
			"\n\nIt is suggested to explore the configuration using the -s/--show "+
			"option to help ensure you understand how the client is configured."+
			"",
		),
		kong.UsageOnError(),
		opt,
	)
	if err != nil {
		return nil, err
	}

	if testOpts {
		parser.Exit = func(_ int) { panic("exit") }
	}

	_, err = parser.Parse(args)
	if err != nil {
		parser.FatalIfErrorf(err)
	}

	return &cli, nil
}

type LoggerIn struct {
	fx.In
	CLI *CLI
	Cfg LogConfig
}

// Create the logger and configure it based on if the program is in
// debug mode or normal mode.
func provideLogger(in LoggerIn) (*slog.Logger, error) {
	if in.CLI.Dev {
		in.Cfg.EncodeLevel = "capitalColor"
		in.Cfg.EncodeTime = "RFC3339"
		in.Cfg.Level = "DEBUG"
		in.Cfg.Development = true
		in.Cfg.Encoding = "console"
		in.Cfg.OutputPaths = append(in.Cfg.OutputPaths, "stderr")
		in.Cfg.ErrorOutputPaths = append(in.Cfg.ErrorOutputPaths, "stderr")
	}

	logger, err := newLogger(in.Cfg)

	return logger, err
}
