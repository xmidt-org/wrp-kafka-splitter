// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package app

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"xmidt-org/wrp-kafka-splitter/internal/consumer"
	"xmidt-org/wrp-kafka-splitter/internal/metrics"

	"github.com/alecthomas/kong"
	"github.com/goschtalt/goschtalt"
	"github.com/xmidt-org/arrange/arrangehttp"
	"github.com/xmidt-org/candlelight"
	"github.com/xmidt-org/touchstone"
	"github.com/xmidt-org/touchstone/touchhttp"

	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"
)

const (
	applicationName = "wrp-kafka-splitter"
)

// These match what goreleaser provides.
var (
	commit  = "undefined"
	version = "undefined"
	date    = "undefined"
	builtBy = "undefined"
)

// CLI is the structure that is used to capture the command line arguments.
type CLI struct {
	Dev     bool     `optional:"" short:"d" help:"Run in development mode."`
	Show    bool     `optional:"" short:"s" help:"Show the configuration and exit."`
	Default string   `optional:""           help:"Output the default configuration file as the specified file."`
	Graph   string   `optional:"" short:"g" help:"Output the dependency graph to the specified file."`
	Files   []string `optional:"" short:"f" help:"Specific configuration files or directories."`
}

type LifeCycleIn struct {
	fx.In
	Logger     *slog.Logger
	LC         fx.Lifecycle
	Shutdowner fx.Shutdowner
	Consumer   *consumer.Consumer
}

// WrpKafkaRouter is the main entry point for the program.  It is responsible for
// setting up the dependency injection framework and returning the app object.
func WrpKafkaRouter(args []string) (*fx.App, error) {
	app := fx.New(provideAppOptions(args))
	if err := app.Err(); err != nil {
		return nil, err
	}

	return app, nil
}

// provideAppOptions returns all fx options required to start the xmidt agent fx app.
func provideAppOptions(args []string) fx.Option {
	var (
		gscfg *goschtalt.Config

		// Capture the dependency tree in case we need to debug something.
		g fx.DotGraph

		// Capture the command line arguments.
		cli *CLI
	)

	opts := fx.Options(
		fx.Supply(cliArgs(args)),
		fx.Populate(&g),
		fx.Populate(&gscfg),
		fx.Populate(&cli),

		fx.WithLogger(func(log *slog.Logger) fxevent.Logger {
			return newFxLogger(log)
		}),

		fx.Provide(
			provideCLI,
			provideLogger,
			provideConfig,
			provideLogObserver,
			provideConsumer,

			goschtalt.UnmarshalFunc[LogConfig]("logger", goschtalt.Optional()),
			goschtalt.UnmarshalFunc[candlelight.Config]("tracing"),
			goschtalt.UnmarshalFunc[touchstone.Config]("prometheus"),
			goschtalt.UnmarshalFunc[touchhttp.Config]("prometheus_handler"),
			goschtalt.UnmarshalFunc[HealthPath]("servers.health.path"),
			goschtalt.UnmarshalFunc[MetricsPath]("servers.metrics.path"),
			goschtalt.UnmarshalFunc[PprofPathPrefix]("servers.pprof.path"),
			goschtalt.UnmarshalFunc[consumer.Config]("consumer"),
			fx.Annotated{
				Name:   "servers.health.config",
				Target: goschtalt.UnmarshalFunc[arrangehttp.ServerConfig]("servers.health.http"),
			},
			fx.Annotated{
				Name:   "servers.metrics.config",
				Target: goschtalt.UnmarshalFunc[arrangehttp.ServerConfig]("servers.metrics.http"),
			},
			fx.Annotated{
				Name:   "servers.pprof.config",
				Target: goschtalt.UnmarshalFunc[arrangehttp.ServerConfig]("servers.pprof.http"),
			},
		),

		provideMetricEndpoint(),
		provideHealthCheck(),
		providePprofEndpoint(),

		arrangehttp.ProvideServer("servers.health"),
		arrangehttp.ProvideServer("servers.metrics"),
		arrangehttp.ProvideServer("servers.pprof"),

		touchstone.Provide(),
		touchhttp.Provide(),

		metrics.Provide(), // Provides consumer_errors counter and other metrics

		MetricObserversModule,

		fx.Invoke(
			lifeCycle,
		),
	)

	if cli != nil && cli.Graph != "" {
		_ = os.WriteFile(cli.Graph, []byte(g), 0600)
	}

	return opts
}

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

func onStart(logger *slog.Logger, consumer *consumer.Consumer) func(context.Context) error {
	return func(ctx context.Context) (err error) {
		if err = ctx.Err(); err != nil {
			return err
		}

		// Start the Kafka consumer
		if err = consumer.Start(); err != nil {
			logger.Error("failed to start consumer", "error", err)
			return err
		}

		logger.Info("consumer started successfully")
		return nil
	}
}

func onStop(shutdowner fx.Shutdowner, logger *slog.Logger, consumer *consumer.Consumer) func(context.Context) error {
	return func(ctx context.Context) error {
		logger.Info("stopping consumer")

		// Stop the consumer with the shutdown context
		if err := consumer.Stop(ctx); err != nil {
			logger.Error("error stopping consumer", "error", err)
			return err
		}

		logger.Info("consumer stopped successfully")
		return nil
	}
}

func lifeCycle(in LifeCycleIn) {
	logger := in.Logger.With("component", "fx_lifecycle")
	in.LC.Append(
		fx.Hook{
			OnStart: onStart(logger, in.Consumer),
			OnStop:  onStop(in.Shutdowner, logger, in.Consumer),
		},
	)
}
