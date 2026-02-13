// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package app

import (
	_ "embed"
	"fmt"
	"os"

	"xmidt-org/splitter/internal/configuration"
	"xmidt-org/splitter/internal/consumer"

	"github.com/goschtalt/goschtalt"
	_ "github.com/goschtalt/goschtalt/pkg/typical"
	_ "github.com/goschtalt/properties-decoder"
	_ "github.com/goschtalt/yaml-decoder"
	_ "github.com/goschtalt/yaml-encoder"
	"github.com/xmidt-org/arrange/arrangehttp"
	"github.com/xmidt-org/candlelight"
	"github.com/xmidt-org/touchstone"
	"github.com/xmidt-org/touchstone/touchhttp"
	"gopkg.in/dealancer/validate.v2"
)

//go:embed default-config.yaml
var defaultConfigFile []byte

// Config is the configuration for the wrp-kafka-splitter.
type Config struct {
	Logger            LogConfig
	Tracing           candlelight.Config
	Prometheus        touchstone.Config
	PrometheusHandler touchhttp.Config
	Servers           Servers
	Consumer          consumer.Config
}

type Servers struct {
	Health    HealthServer
	Metrics   MetricsServer
	Pprof     PprofServer
	Primary   PrimaryServer
	Alternate PrimaryServer
}

type HealthServer struct {
	HTTP arrangehttp.ServerConfig
	Path HealthPath `validate:"empty=false"`
}

type HealthPath string

type MetricsServer struct {
	HTTP arrangehttp.ServerConfig
	Path MetricsPath `validate:"empty=false"`
}

type MetricsPath string

type PrimaryServer struct {
	HTTP arrangehttp.ServerConfig
}

type PprofServer struct {
	HTTP arrangehttp.ServerConfig
	Path PprofPathPrefix
}

type PprofPathPrefix string

// Collect and process the configuration files and env vars and
// produce a configuration object.
func provideConfig(cli *CLI) (*goschtalt.Config, error) {

	gs, err := goschtalt.New(
		goschtalt.StdCfgLayout(applicationName, cli.Files...),
		goschtalt.ConfigIs("two_words"),
		goschtalt.DefaultUnmarshalOptions(
			goschtalt.WithValidator(
				goschtalt.ValidatorFunc(validate.Validate),
			),
		),
		// Seed the program with the default, built-in configuration
		goschtalt.AddBuffer("!default-config.yaml", defaultConfigFile, goschtalt.AsDefault()),
	)
	if err != nil {
		return nil, err
	}

	// Externals are a list of individually processed external configuration
	// files.  Each external configuration file is processed and the resulting
	// map is used to populate the configuration.
	//
	// This is done after the initial configuration has been calculated because
	// the external configurations are listed in the configuration.
	if err = configuration.Apply(gs, "externals", false); err != nil {
		return nil, err
	}

	if cli.Default != "" {
		err := os.WriteFile("./"+cli.Default, defaultConfigFile, 0644) // nolint: gosec
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(-1)
		}
		os.Exit(0)
	}

	if cli.Show {
		// handleCLIShow handles the -s/--show option where the configuration is
		// shown, then the program is exited.
		//
		// Exit with success because if the configuration is broken it will be
		// very hard to debug where the problem originates.  This way you can
		// see the configuration and then run the service with the same
		// configuration to see the error.

		fmt.Fprintln(os.Stdout, gs.Explain().String())

		out, err := gs.Marshal()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
		} else {
			fmt.Fprintln(os.Stdout, "## Final Configuration\n---\n"+string(out))
		}

		os.Exit(0)
	}

	var tmp Config
	err = gs.Unmarshal(goschtalt.Root, &tmp)
	if err != nil {
		fmt.Fprintln(os.Stderr, "There is a critical error in the configuration.")
		fmt.Fprintln(os.Stderr, "Run with -s/--show to see the configuration.")
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)

		// Exit here to prevent a very difficult to debug error from occurring.
		os.Exit(0)
	}

	return gs, nil
}
