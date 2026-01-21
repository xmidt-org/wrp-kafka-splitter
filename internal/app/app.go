// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package app

import (
	"context"
	"log/slog"
	"time"
	"xmidt-org/splitter/internal/consumer"

	"github.com/goschtalt/goschtalt"
	"go.uber.org/fx"
)

// builds the standalone service

const (
	applicationName = "splitter"
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
	Logger   *slog.Logger
	LC       fx.Lifecycle
	Consumer *consumer.Consumer
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

func provideAppOptions(args []string) fx.Option {
	return fx.Options(
		fx.Supply(cliArgs(args)),
		fx.Provide(provideCLI, provideConfig),

		// Infrastructure modules (in order of dependencies)
		ObservabilityModule(), // Provides logging, tracing, metrics, health, pprof
		CoreModule(),          // Provides consumer and message handling
	)
}

// CoreModule provides the core business logic:
// - Kafka consumer setup with message handling
// - Consumer lifecycle management (OnStart/OnStop hooks)
func CoreModule() fx.Option {
	return fx.Module("core",
		// Consumer configuration must be unmarshaled first
		fx.Provide(
			goschtalt.UnmarshalFunc[consumer.Config]("consumer"),
		),
		// Consumer is the main business logic component
		fx.Provide(
			provideConsumer,
		),

		// Register consumer lifecycle hooks (start/stop)
		fx.Invoke(
			lifeCycle,
		),
	)
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

func onStop(logger *slog.Logger, consumer *consumer.Consumer) func(context.Context) error {
	return func(ctx context.Context) error {
		logger.Info("stopping consumer")

		// Create a timeout context for the shutdown
		shutdownCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		defer cancel()

		// Stop the consumer with the shutdown context
		if err := consumer.Stop(shutdownCtx); err != nil {
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
			OnStop:  onStop(logger, in.Consumer),
		},
	)
}
