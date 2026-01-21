// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

// ^ OnStop Issue: Data race in go.uber.org/fx.exitCodeOption.apply() (at shutdown.go:44)
// Root Cause: Multiple goroutines from server instances calling Shutdown() concurrently, with one writing to the exit code while another is reading it.
package app

import (
	"testing"
	"time"
	"xmidt-org/splitter/internal/consumer"

	_ "github.com/goschtalt/goschtalt/pkg/typical"
	_ "github.com/goschtalt/yaml-decoder"
	_ "github.com/goschtalt/yaml-encoder"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_provideCLI(t *testing.T) {
	tests := []struct {
		description string
		args        cliArgs
		want        CLI
		exits       bool
		expectedErr error
	}{
		{
			description: "no arguments, everything works",
		}, {
			description: "dev mode",
			args:        cliArgs{"-d"},
			want:        CLI{Dev: true},
		}, {
			description: "invalid argument",
			args:        cliArgs{"-w"},
			exits:       true,
		}, {
			description: "invalid argument",
			args:        cliArgs{"-d", "-w"},
			exits:       true,
		}, {
			description: "help",
			args:        cliArgs{"-h"},
			exits:       true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			assert := assert.New(t)

			if tc.exits {
				assert.Panics(func() {
					_, _ = provideCLIWithOpts(tc.args, true)
				})
			} else {
				got, err := provideCLI(tc.args)

				assert.ErrorIs(err, tc.expectedErr)
				want := tc.want
				assert.Equal(&want, got)
			}
		})
	}
}

func TestWrpKafkaRouter(t *testing.T) {
	tests := []struct {
		description      string
		args             []string
		duration         time.Duration
		expectedErr      error
		expectedStartErr error
		panic            bool
	}{
		{
			description: "show config and exit",
			args:        []string{"-s"},
			panic:       true,
		}, {
			description: "show help and exit",
			args:        []string{"-h"},
			panic:       true,
		}, {
			description: "enable debug mode",
			args:        []string{"-d", "-f", "splitter.yaml"},
		}, {
			description: "output graph",
			args:        []string{"-g", "graph.dot", "-f", "splitter.yaml"},
		}, {
			description:      "start and stop",
			duration:         time.Millisecond,
			args:             []string{"-f", "splitter.yaml"},
			expectedStartErr: consumer.ErrPingingBroker,
		},
	}
	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			assert := assert.New(t)
			require := require.New(t)

			if tc.panic {
				assert.Panics(func() {
					_, _ = WrpKafkaRouter(tc.args)
				})
				return
			}

			app, err := WrpKafkaRouter(tc.args)

			assert.ErrorIs(err, tc.expectedErr)
			if tc.expectedErr != nil {
				assert.Nil(app)
				return
			} else {
				require.NoError(err)
			}

			if tc.duration <= 0 {
				return
			}

			// TODO below will fail the race condition in uber.fx, see top of the file

			// only run the program for	a few seconds to make sure it starts
			// startCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			// defer cancel()
			// err = app.Start(startCtx)
			// if tc.expectedStartErr != nil {
			// 	require.True(errors.Is(err, tc.expectedStartErr))
			// }

			// time.Sleep(tc.duration)

			// stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			// defer cancel()
			// err = app.Stop(stopCtx)
			// require.NoError(err)
		})
	}
}

func Test_provideLogger(t *testing.T) {
	tests := []struct {
		description string
		cli         *CLI
		cfg         LogConfig
		expectedErr error
	}{
		{
			description: "validate empty config",
			cfg:         LogConfig{},
			cli:         &CLI{},
		}, {
			description: "validate dev config",
			cfg:         LogConfig{},
			cli:         &CLI{Dev: true},
		},
	}
	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			assert := assert.New(t)

			got, err := provideLogger(LoggerIn{CLI: tc.cli, Cfg: tc.cfg})

			if tc.expectedErr == nil {
				assert.NotNil(got)
				assert.NoError(err)
				return
			}
			assert.ErrorIs(err, tc.expectedErr)
			assert.Nil(got)
		})
	}
}
