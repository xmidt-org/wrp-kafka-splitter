// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package app

import (
	"fmt"
	"io"
	"log/slog"
	"os"

	"go.uber.org/fx/fxevent"
	"gopkg.in/natefinch/lumberjack.v2"
)

// LogConfig defines the configuration for structured logging using slog.
type LogConfig struct {
	// Level is the log level (DEBUG, INFO, WARN, ERROR)
	Level string `default:"INFO"`

	// Encoding is the log encoding format (json, console)
	Encoding string `default:"json"`

	// OutputPaths is a list of URLs or file paths to write logs to
	OutputPaths []string `default:"[stdout]"`

	// ErrorOutputPaths is a list of URLs or file paths to write error logs to
	ErrorOutputPaths []string `default:"[stderr]"`

	// Development enables development mode (more verbose output, colored)
	Development bool `default:"false"`

	// EncodeLevel defines how to encode the level (lowercase, capital, capitalColor)
	EncodeLevel string `default:"lowercase"`

	// EncodeTime defines how to encode the time (iso8601, millis, nanos, rfc3339)
	EncodeTime string `default:"iso8601"`

	// Rotation configures log rotation for file outputs
	Rotation RotationConfig
}

// RotationConfig defines log rotation settings
type RotationConfig struct {
	// MaxSize is the maximum size in megabytes of the log file before it gets rotated
	MaxSize int `yaml:"maxsize" default:"50"`

	// MaxAge is the maximum number of days to retain old log files
	MaxAge int `yaml:"maxage" default:"2"`

	// MaxBackups is the maximum number of old log files to retain
	MaxBackups int `yaml:"maxbackups" default:"10"`

	// Compress determines if rotated files should be compressed using gzip
	Compress bool `yaml:"compress" default:"true"`

	// LocalTime determines if the time used for formatting the timestamps in backup files is the computer's local time
	LocalTime bool `yaml:"localtime" default:"false"`
}

// newLogger creates a new structured logger based on the configuration.
func newLogger(cfg LogConfig) (*slog.Logger, error) {
	// Determine the output writer
	var out io.Writer
	switch len(cfg.OutputPaths) {
	case 0:
		out = os.Stdout
	case 1: // performance optimization to avoid a loop
		switch cfg.OutputPaths[0] {
		case "stdout", "":
			out = os.Stdout
		case "stderr":
			out = os.Stderr
		default:
			// Use rotating logger for file outputs
			out = &lumberjack.Logger{
				Filename:   cfg.OutputPaths[0],
				MaxSize:    cfg.Rotation.MaxSize,
				MaxAge:     cfg.Rotation.MaxAge,
				MaxBackups: cfg.Rotation.MaxBackups,
				Compress:   cfg.Rotation.Compress,
				LocalTime:  cfg.Rotation.LocalTime,
			}
		}
	default:
		// For multiple output paths, write to all
		writers := make([]io.Writer, 0, len(cfg.OutputPaths))
		for _, path := range cfg.OutputPaths {
			switch path {
			case "stdout", "":
				writers = append(writers, os.Stdout)
			case "stderr":
				writers = append(writers, os.Stderr)
			default:
				// Use rotating logger for file outputs
				writers = append(writers, &lumberjack.Logger{
					Filename:   path,
					MaxSize:    cfg.Rotation.MaxSize,
					MaxAge:     cfg.Rotation.MaxAge,
					MaxBackups: cfg.Rotation.MaxBackups,
					Compress:   cfg.Rotation.Compress,
					LocalTime:  cfg.Rotation.LocalTime,
				})
			}
		}
		out = io.MultiWriter(writers...)
	}

	// Parse the log level
	level := slog.LevelInfo
	switch cfg.Level {
	case "DEBUG":
		level = slog.LevelDebug
	case "WARN":
		level = slog.LevelWarn
	case "ERROR":
		level = slog.LevelError
	}

	// Determine if we should use text (console) or JSON handler
	var handler slog.Handler
	opts := &slog.HandlerOptions{
		Level: level,
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			return a
		},
	}

	if cfg.Encoding == "console" {
		handler = slog.NewTextHandler(out, opts)
	} else {
		handler = slog.NewJSONHandler(out, opts)
	}

	logger := slog.New(handler)
	return logger, nil
}

// slogFxLogger is a wrapper that implements fxevent.Logger using slog.
type slogFxLogger struct {
	logger *slog.Logger
}

// LogEvent logs an fx event using slog.
func (l *slogFxLogger) LogEvent(event fxevent.Event) {
	switch e := event.(type) {
	case *fxevent.OnStartExecuting:
		l.logger.Debug("fx: OnStart hook executing",
			slog.String("caller", e.CallerName),
		)
	case *fxevent.OnStartExecuted:
		if e.Err != nil {
			l.logger.Error("fx: OnStart hook failed",
				slog.String("caller", e.CallerName),
				slog.String("error", e.Err.Error()),
			)
		} else {
			l.logger.Debug("fx: OnStart hook executed",
				slog.String("caller", e.CallerName),
			)
		}
	case *fxevent.OnStopExecuting:
		l.logger.Debug("fx: OnStop hook executing",
			slog.String("caller", e.CallerName),
		)
	case *fxevent.OnStopExecuted:
		if e.Err != nil {
			l.logger.Error("fx: OnStop hook failed",
				slog.String("caller", e.CallerName),
				slog.String("error", e.Err.Error()),
			)
		} else {
			l.logger.Debug("fx: OnStop hook executed",
				slog.String("caller", e.CallerName),
			)
		}
	case *fxevent.Supplied:
		l.logger.Debug("fx: supplied",
			slog.String("type", e.TypeName),
		)
	case *fxevent.Provided:
		l.logger.Debug("fx: provided",
			slog.String("module", e.ModuleName),
		)
	case *fxevent.Invoked:
		l.logger.Debug("fx: invoked",
			slog.String("function", e.FunctionName),
		)
	case *fxevent.Stopped:
		if e.Err != nil {
			l.logger.Error("fx: stopped",
				slog.String("error", e.Err.Error()),
			)
		} else {
			l.logger.Info("fx: stopped")
		}
	case *fxevent.Started:
		l.logger.Info("fx: started")
	default:
		l.logger.Debug("fx event", slog.String("event", fmt.Sprintf("%#v", event)))
	}
}

// newFxLogger creates a new fx event logger that uses slog.
func newFxLogger(logger *slog.Logger) fxevent.Logger {
	return &slogFxLogger{logger: logger}
}
